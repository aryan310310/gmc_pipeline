# Databricks notebook source
# MAGIC %pip install pymupdf openai typing_extensions --upgrade -q

# COMMAND ----------

# DBTITLE 1,Import shared utilities
# Accept parameters from orchestrator (empty = standalone mode)
dbutils.widgets.text("pdf_path", "", "PDF File Path")
dbutils.widgets.text("entity_id", "", "Entity ID")

# Run the common extractor - provides: text, client, extract_with_llm(), normalize_boolean(), etc.
import json as _json, os as _os, time as _time, base64 as _b64, requests as _req

_nb_workspace_path = "/Users/aryan.more@edmeinsurance.com/gmc_extraction_pipeline/common_pdf_extractor"
_nb_file_path = "/Workspace/Users/aryan.more@edmeinsurance.com/gmc_extraction_pipeline/common_pdf_extractor.ipynb"

_nb = None

# Method 1: Try filesystem (fastest when available)
for _attempt in range(3):
    if _os.path.exists(_nb_file_path):
        try:
            with open(_nb_file_path, "r", encoding="utf-8") as _f:
                _nb = _json.load(_f)
            break
        except Exception:
            pass
    _time.sleep(3)

# Method 2: Workspace API (reliable on fresh serverless)
if _nb is None:
    try:
        _host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
        _token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        _resp = _req.get(
            f"{_host}/api/2.0/workspace/export",
            headers={"Authorization": f"Bearer {_token}"},
            params={"path": _nb_workspace_path, "format": "JUPYTER"}
        )
        if _resp.status_code == 200:
            _content_b64 = _resp.json().get("content", "")
            _nb = _json.loads(_b64.b64decode(_content_b64).decode("utf-8"))
    except Exception as _e:
        print(f"API fallback failed: {_e}")

if _nb is None:
    raise FileNotFoundError(
        f"Cannot find {_nb_file_path}. Please run the common_pdf_extractor notebook first "
        f"or ensure it exists in your workspace."
    )

for _cell in _nb.get("cells", []):
    if _cell.get("cell_type") == "code":
        _source = "".join(_cell.get("source", []))
        if _source.strip().startswith("%"):
            continue  # Skip: packages pre-installed by %pip cell above
        exec(_source)

print("\u2705 Common utilities loaded successfully")

# COMMAND ----------

# DBTITLE 1,GMC Room Rent Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in room rent limits.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any value not explicitly written in the document
- HALLUCINATE numbers that are not clearly stated

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY what is EXPLICITLY written in the document.
- A single policy has MULTIPLE rows - one per combination of room_type × hospital_location (× grade/SI_basis if applicable).
- If room rent is NOT mentioned at all → return a single object with room_rent_basis = null.

Rules:
- Return a JSON ARRAY of objects
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_number: Extract the policy number.

3. is_graded: BOOLEAN. true if room rent varies by employee grade/designation. false otherwise.

4. grade_classification: STRING. The grade/plan name if is_graded=true.
   - Examples: "Executive", "Managerial", "Director", "Plan 1", "Plan 2", "G1 G2", "VP & Above", "AVP & Below", "Top Managment", "Package1"
   - Set to "Flat" if not graded (single tier for all employees)
   - null if is_graded=false AND is_SI_basis=true

5. is_SI_basis: BOOLEAN. true if room rent varies by Sum Insured slab. false otherwise.

6. SI_basis_description: STRING. The SI slab if is_SI_basis=true.
   - Examples: "3L", "5L", "10L", "15L", "5L & 10L", "1L-3L"
   - Use lakh notation: "3L" for 3 Lakhs, "10L" for 10 Lakhs
   - null if is_SI_basis=false

7. room_type: STRING. ENUM values:
   - "Normal" - Regular/Standard/General ward room
   - "ICU" - Intensive Care Unit / Critical Care Unit / ICCU / NICU
   - Look for: "Room Rent", "Normal Room", "ICU charges", "Single AC Room"

8. hospital_location: STRING. ENUM values:
   - "metro" - Metro city / Tier-1 city
   - "non-metro" - Non-metro / Tier-2 / Tier-3 / Other cities
   - If NO location distinction is made, return BOTH "metro" and "non-metro" with SAME values

9. room_rent_basis: STRING. How room rent is calculated. ENUM values:
   - "Percent of SI" - Room rent is a PERCENTAGE of Sum Insured (values typically 1-6%)
   - "Flat" - Room rent is a FIXED AMOUNT per day (values typically 1000-20000)
   - "Actuals" - As per actual charges, no cap (room_rent = "As Per Actuals")
   - null - Room rent info not found in document

   CRITICAL: How to determine room_rent_basis:
   - If value is between 0.5-10 (or expressed as "1% of SI", "2% SI") → "Percent of SI"
   - If value is > 100 (like 3000, 5000, 10000) → "Flat"
   - If document says "Actuals", "As per actuals", "No limit on room rent" → "Actuals"
   - If value is exactly 100 (or "100%") → "Percent of SI" (means full SI, no sublimit)

10. room_rent: STRING. The actual limit value.
    - For "Percent of SI": the percentage number (e.g., "1", "1.5", "2", "100")
    - For "Flat": the amount per day (e.g., "5000", "10000", "3500")
    - For "Actuals": "As Per Actuals"
    - null if room_rent_basis is null

11. room_restriction: STRING. Any room category restriction.
    - "Single Private AC" - Single occupancy private AC room
    - "Single Standard AC" - Single occupancy standard AC room
    - "Standard AC" - Standard AC room (may be shared)
    - "No Capping" - No restriction on room type
    - A numeric value if a specific amount cap (e.g., "5000", "10000")
    - null if no restriction mentioned

12. max_room_rent: INTEGER. Maximum room rent cap per day (if separate from room_rent).
    - This is an absolute maximum even when percentage/basis allows higher
    - Common values: 1500, 3500, 4000, 5000, 7000, 7500, 8000, 10000, 12500, 20000
    - null if no separate max cap mentioned

ROW EXPANSION RULES:
- Minimum 2 rows: Normal + ICU (if no metro/non-metro distinction)
- Typical 4 rows: Normal×metro, Normal×non-metro, ICU×metro, ICU×non-metro
- If graded: 4 rows per grade (room_type × hospital_location per grade)
- If SI_basis: 4 rows per SI slab
- If BOTH is_graded AND is_SI_basis: 4 rows per grade-SI combination

IMPORTANT EXTRACTION CLUES:
- "1% of SI" or "1% of Sum Insured" → room_rent_basis = "Percent of SI", room_rent = "1"
- "Rs. 5000 per day" or "5000/day" → room_rent_basis = "Flat", room_rent = "5000"
- "Single AC Room" or "Private Room" → room_restriction (not room_type)
- "No room rent capping" or "As per actuals" → room_rent_basis = "Actuals"
- ICU is typically 2x Normal rent (e.g., Normal=1% → ICU=2%)
- "Room Rent limit" / "Accommodation charges" = room rent section

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "is_graded": false,
    "grade_classification": "Flat",
    "is_SI_basis": false,
    "SI_basis_description": null,
    "room_type": "Normal",
    "hospital_location": "metro",
    "room_rent_basis": "Percent of SI",
    "room_rent": "1",
    "room_restriction": null,
    "max_room_rent": null
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract room rent data - returns a LIST of rows
truncated_text = text[:250000]

response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": prompt},
        {"role": "user", "content": truncated_text}
    ],
    temperature=0
)

json_output = response.choices[0].message.content
json_output = json_output.replace("```json", "").replace("```", "").strip()

# Parse as array
match = re.search(r"\[.*\]", json_output, re.DOTALL)
if match:
    room_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(room_rows)} room rent rows")
    for i, row in enumerate(room_rows):
        print(f"  Row {i+1}: {row.get('room_type')} | {row.get('hospital_location')} | basis={row.get('room_rent_basis')} | rent={row.get('room_rent')} | grade={row.get('grade_classification')} | SI={row.get('SI_basis_description')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        room_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        room_rows = []
        print("NO VALID JSON FOUND")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re
from itertools import product
from collections import defaultdict

# Set entity_id and policy_number
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

if room_rows and room_rows[0].get("policy_number"):
    policy_number_extracted = room_rows[0]["policy_number"]

print("--- Room Rent Post-processing ---")

# --- STEP 1: Normalize enum values ---
ALLOWED_ROOM_TYPE = ["Normal", "ICU"]
ALLOWED_HOSPITAL_LOCATION = ["metro", "non-metro"]

validated_rows = []
for row in room_rows:
    row["entity_id"] = entity_id
    row["policy_number"] = policy_number_extracted
    
    # Normalize room_type
    rt = str(row.get("room_type", "")).strip().lower()
    if rt in ["icu", "intensive care", "critical care", "iccu", "nicu"]:
        row["room_type"] = "ICU"
    else:
        row["room_type"] = "Normal"
    
    # Normalize hospital_location
    hl = str(row.get("hospital_location", "")).strip().lower()
    if hl in ["non-metro", "non metro", "nonmetro", "tier-2", "tier 2", "tier-3", "other"]:
        row["hospital_location"] = "non-metro"
    else:
        row["hospital_location"] = "metro"
    
    # Normalize room_rent_basis
    rrb = row.get("room_rent_basis")
    if rrb is not None:
        rrb_lower = str(rrb).strip().lower()
        if rrb_lower in ["percent of si", "percentage of si", "% of si", "percent"]:
            row["room_rent_basis"] = "Percent of SI"
        elif rrb_lower in ["flat", "fixed", "per day"]:
            row["room_rent_basis"] = "Flat"
        elif rrb_lower in ["actuals", "as per actuals", "actual", "no limit"]:
            row["room_rent_basis"] = "Actuals"
        else:
            row["room_rent_basis"] = None
    
    # room_rent_basis auto-detection based on value
    if row.get("room_rent_basis") in ["Percent of SI", "Flat"] and row.get("room_rent") is not None:
        try:
            rent_val = float(str(row["room_rent"]).replace(",", "").replace("Rs.", "").replace("INR", "").strip())
            if row["room_rent_basis"] == "Percent of SI" and rent_val > 100:
                row["room_rent_basis"] = "Flat"
            elif row["room_rent_basis"] == "Flat" and rent_val <= 10:
                row["room_rent_basis"] = "Percent of SI"
            row["room_rent"] = str(rent_val) if rent_val != int(rent_val) or rent_val > 100 else str(int(rent_val))
        except (ValueError, TypeError):
            pass
    
    # Actuals: set room_rent to "As Per Actuals"
    if row.get("room_rent_basis") == "Actuals":
        row["room_rent"] = "As Per Actuals"
    
    # Normalize room_restriction
    rr = row.get("room_restriction")
    if rr is not None:
        rr_str = str(rr).strip()
        rr_lower = rr_str.lower()
        if rr_lower in ["single private ac", "single private a/c", "private ac", "single pvt ac"]:
            row["room_restriction"] = "Single Private AC"
        elif rr_lower in ["single standard ac", "single standard a/c", "standard single ac"]:
            row["room_restriction"] = "Single Standard AC"
        elif rr_lower in ["standard ac", "standard a/c", "standard"]:
            row["room_restriction"] = "Standard AC"
        elif rr_lower in ["no capping", "no cap", "no limit", "no restriction"]:
            row["room_restriction"] = "No Capping"
        elif rr_str.replace(".", "").isdigit():
            row["room_restriction"] = str(int(float(rr_str)))  # numeric cap
        else:
            row["room_restriction"] = rr_str  # keep as-is
    else:
        row["room_restriction"] = None
    
    # max_room_rent: ensure integer or null
    mrr = row.get("max_room_rent")
    if mrr is not None:
        try:
            row["max_room_rent"] = int(float(str(mrr).replace(",", "")))
        except (ValueError, TypeError):
            row["max_room_rent"] = None
    else:
        row["max_room_rent"] = None
    
    validated_rows.append(row)

# --- STEP 2: Determine is_graded and is_SI_basis based on ACTUAL data variation ---
# is_graded = true ONLY if different grades have different room_rent values
# is_SI_basis = true ONLY if different SI slabs have different room_rent values

# Check if multiple distinct grade_classification values exist with DIFFERENT rents
grade_values = set()
si_values = set()
rent_by_grade = defaultdict(set)
rent_by_si = defaultdict(set)

for row in validated_rows:
    gc = row.get("grade_classification")
    si = row.get("SI_basis_description")
    rent = row.get("room_rent")
    rt = row.get("room_type")
    
    if gc and gc.lower() != "flat":
        grade_values.add(gc)
        rent_by_grade[(gc, rt)].add(rent)
    if si:
        si_values.add(si)
        rent_by_si[(si, rt)].add(rent)

# is_graded: true only if MULTIPLE grades with DIFFERENT room rents
has_multiple_grades = len(grade_values) > 1
if has_multiple_grades:
    # Check if different grades actually have different rent values for same room_type
    rents_per_grade_normal = {gc: rents for (gc, rt), rents in rent_by_grade.items() if rt == "Normal"}
    rents_per_grade_icu = {gc: rents for (gc, rt), rents in rent_by_grade.items() if rt == "ICU"}
    grades_differ = len(set(frozenset(v) for v in rents_per_grade_normal.values())) > 1 or \
                    len(set(frozenset(v) for v in rents_per_grade_icu.values())) > 1
else:
    grades_differ = False

# is_SI_basis: true only if MULTIPLE SI slabs with DIFFERENT room rents
has_multiple_si = len(si_values) > 1
if has_multiple_si:
    rents_per_si_normal = {si: rents for (si, rt), rents in rent_by_si.items() if rt == "Normal"}
    rents_per_si_icu = {si: rents for (si, rt), rents in rent_by_si.items() if rt == "ICU"}
    si_differ = len(set(frozenset(v) for v in rents_per_si_normal.values())) > 1 or \
                len(set(frozenset(v) for v in rents_per_si_icu.values())) > 1
else:
    si_differ = False

print(f"  Grade values found: {grade_values} -> is_graded = {grades_differ}")
print(f"  SI values found: {si_values} -> is_SI_basis = {si_differ}")

# Apply the determined flags to all rows
for row in validated_rows:
    # is_graded: null if not graded, true if graded
    if grades_differ:
        row["is_graded"] = True
    else:
        row["is_graded"] = None
        row["grade_classification"] = None  # no grading, so no classification
    
    # is_SI_basis: null if not SI-based, true if SI-based
    if si_differ:
        row["is_SI_basis"] = True
    else:
        row["is_SI_basis"] = None
        row["SI_basis_description"] = None  # no SI differentiation, so no description

# --- STEP 3: Row expansion ---
# Ensure all combinations of room_type × hospital_location exist for each grade/SI group
def get_group_key(row):
    return (row.get("grade_classification"), row.get("SI_basis_description"))

groups = defaultdict(list)
for row in validated_rows:
    groups[get_group_key(row)].append(row)

expanded_rows = []
for group_key, rows in groups.items():
    # Find which combos exist
    existing_combos = set((r["room_type"], r["hospital_location"]) for r in rows)
    all_combos = set(product(ALLOWED_ROOM_TYPE, ALLOWED_HOSPITAL_LOCATION))
    missing_combos = all_combos - existing_combos
    
    expanded_rows.extend(rows)
    
    # Fill missing combos using existing data as template
    if missing_combos and rows:
        for combo in missing_combos:
            rt, hl = combo
            # Find a template: same room_type first, then any
            template = None
            for r in rows:
                if r["room_type"] == rt:
                    template = r
                    break
            if template is None:
                template = rows[0]
            
            new_row = template.copy()
            new_row["room_type"] = rt
            new_row["hospital_location"] = hl
            expanded_rows.append(new_row)
            print(f"  \u2795 Expanded: {rt} | {hl} (from group {group_key})")

room_rows = expanded_rows

print(f"\n--- Final: {len(room_rows)} room rent rows ---")
for i, row in enumerate(room_rows):
    print(f"  \u2705 Row {i+1}: {row['room_type']} | {row['hospital_location']} | basis={row.get('room_rent_basis')} | rent={row.get('room_rent')} | is_graded={row.get('is_graded')} | grade={row.get('grade_classification')} | is_SI={row.get('is_SI_basis')} | SI={row.get('SI_basis_description')} | restriction={row.get('room_restriction')} | max={row.get('max_room_rent')}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("is_graded", BooleanType(), True),
    StructField("grade_classification", StringType(), True),
    StructField("is_SI_basis", BooleanType(), True),
    StructField("SI_basis_description", StringType(), True),
    StructField("room_type", StringType(), False),
    StructField("hospital_location", StringType(), False),
    StructField("room_rent_basis", StringType(), True),
    StructField("room_rent", StringType(), True),
    StructField("room_restriction", StringType(), True),
    StructField("max_room_rent", IntegerType(), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in room_rows:
    formatted = {}
    for col_name in schema_cols:
        val = row.get(col_name)
        field = schema[col_name]
        if isinstance(field.dataType, IntegerType) and val is not None:
            try:
                formatted[col_name] = int(float(str(val).replace(",", "")))
            except (ValueError, TypeError):
                formatted[col_name] = None
        elif isinstance(field.dataType, BooleanType):
            if val is None:
                formatted[col_name] = None
            else:
                formatted[col_name] = bool(val)
        elif isinstance(field.dataType, StringType):
            if val is None or str(val).strip().lower() in ["none", "null", ""]:
                formatted[col_name] = None
            else:
                formatted[col_name] = str(val).strip()
        else:
            formatted[col_name] = val
    formatted_rows.append(formatted)

# Create DataFrame
if formatted_rows:
    room_rent_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    room_rent_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No room rent data found - empty DataFrame created")

display(room_rent_df)
print(f"\n\u2705 GMC Room Rent extraction complete! ({room_rent_df.count()} rows)")
