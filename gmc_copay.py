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

# DBTITLE 1,GMC Copay Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in co-pay/co-payment details.

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
- A single policy has MULTIPLE rows - one per combination of coverage × relationship (× grade/SI_basis if applicable).
- If co-pay is NOT mentioned at all → return rows with copay = 0 for all relationships.

Rules:
- Return a JSON ARRAY of objects
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_number: Extract the policy number.

3. is_graded: BOOLEAN or null.
   - true ONLY if co-pay varies by employee grade/designation/age bracket.
   - null if no grading applies.
   - Examples of grading: "20% for 50-80 years", "40% for 81-100 years", different copay per designation.

4. grade_classification: STRING.
   - The grade/plan/age-bracket name if is_graded=true.
   - Examples: "50 - 80 years", "81 - 100 years", "100 & above years", "Executive", "Plan 3", "G1 G2", "Top management employees"
   - null if is_graded is null.

5. is_SI_basis: BOOLEAN or null.
   - true ONLY if co-pay varies by Sum Insured slab.
   - null if no SI-based variation.

6. SI_basis_description: STRING.
   - The SI slab if is_SI_basis=true. Use lakh notation: "2L", "5L", "10L".
   - null if is_SI_basis is null.

7. coverage: STRING. ENUM values:
   - "network" - Co-pay for network/listed/empanelled hospital
   - "non-network" - Co-pay for non-network/non-listed/non-empanelled hospital
   - If document mentions BOTH, create separate rows for each.
   - If document does NOT distinguish, create rows for BOTH "network" and "non-network" with SAME copay value.

8. relationship: STRING. ENUM values (ONLY these 4 allowed):
   - "employee" - Self/Employee
   - "spouse" - Spouse/Wife/Husband/Partner
   - "child" - Child/Children/Son/Daughter/Sibling/Brother/Sister
   - "parents" - Parents/Parent/Father/Mother/Father-in-law/Mother-in-law/Parents-in-law

   MERGE RULES:
   - self/employee/husband/wife → "employee" (if context is the insured employee)
   - spouse/wife/husband/partner → "spouse"
   - child/child1/child2/children/sibling/brother/sister → "child"
   - parents/parent/father/mother/father in law/mother in law/parents in law → "parents"

9. copay: INTEGER (0-100). The co-pay percentage.
   - 0 = No co-pay / Nil co-pay
   - 10 = 10% co-pay
   - 20 = 20% co-pay
   - If co-pay NOT mentioned for a relationship → 0
   - Common values: 0, 5, 10, 15, 18, 20, 25, 30, 40, 50

ROW EXPANSION RULES:
- Minimum rows: coverage(2) × relationships found = typically 8 rows (network + non-network for each of employee, spouse, child, parents)
- If graded: additional rows per grade for each coverage × relationship combo
- If ONLY parents have copay: still include employee/spouse/child rows with copay=0

IMPORTANT EXTRACTION CLUES:
- "Co-payment" / "Copay" / "Co-Pay" = the copay section
- "Nil" / "No copay" / "Not applicable" / "NA" / "0%" = copay is 0
- "10% copay on parents" = parents get 10%, others get 0
- "20% copay on non-network" = non-network gets 20% for all relationships
- "Copay applicable on parents aged 50+ years" = graded by age for parents only
- If parents have different copay than employee/spouse/child, that's relationship-based variation (NOT grading)
- Grading = SAME relationship but DIFFERENT copay by designation/age bracket

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "is_graded": null,
    "grade_classification": null,
    "is_SI_basis": null,
    "SI_basis_description": null,
    "coverage": "network",
    "relationship": "employee",
    "copay": 0
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract copay data - returns a LIST of rows
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
    copay_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(copay_rows)} copay rows")
    for i, row in enumerate(copay_rows):
        print(f"  Row {i+1}: {row.get('coverage')} | {row.get('relationship')} | copay={row.get('copay')} | grade={row.get('grade_classification')} | SI={row.get('SI_basis_description')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        copay_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        copay_rows = []
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

if copay_rows and copay_rows[0].get("policy_number"):
    policy_number_extracted = copay_rows[0]["policy_number"]

print("--- Copay Post-processing ---")

# --- STEP 1: Normalize enum values ---
ALLOWED_COVERAGE = ["network", "non-network"]
ALLOWED_RELATIONSHIP = ["employee", "spouse", "child", "parents"]

validated_rows = []
for row in copay_rows:
    row["entity_id"] = entity_id
    row["policy_number"] = policy_number_extracted
    
    # Normalize coverage
    cov = str(row.get("coverage", "")).strip().lower()
    if cov in ["non-network", "non network", "nonnetwork", "non-listed", "non-empanelled"]:
        row["coverage"] = "non-network"
    else:
        row["coverage"] = "network"
    
    # Normalize relationship -> only 4 allowed
    rel = str(row.get("relationship", "")).strip().lower()
    if rel in ["employee", "self"]:
        row["relationship"] = "employee"
    elif rel in ["spouse", "wife", "husband", "partner"]:
        row["relationship"] = "spouse"
    elif rel in ["child", "child1", "child2", "children", "sibling", "brother", "sister", "son", "daughter"]:
        row["relationship"] = "child"
    elif rel in ["parents", "parent", "father", "mother", "father in law", "mother in law",
                 "parents in law", "parents in laws", "parent in law", "in-law", "in-laws",
                 "father-in-law", "mother-in-law"]:
        row["relationship"] = "parents"
    else:
        row["relationship"] = "employee"  # fallback
    
    # Copay: ensure integer 0-100
    copay_val = row.get("copay")
    if copay_val is not None:
        try:
            copay_int = int(float(str(copay_val).replace("%", "").strip()))
            row["copay"] = max(0, min(copay_int, 100))  # clamp 0-100
        except (ValueError, TypeError):
            row["copay"] = 0
    else:
        row["copay"] = 0
    
    validated_rows.append(row)

# --- STEP 2: Determine is_graded and is_SI_basis ---
# is_graded = true ONLY if SAME relationship+coverage has DIFFERENT copay by grade
# is_SI_basis = true ONLY if SAME relationship+coverage has DIFFERENT copay by SI slab

grade_values = set()
si_values = set()
copay_by_grade = defaultdict(set)
copay_by_si = defaultdict(set)

for row in validated_rows:
    gc = row.get("grade_classification")
    si = row.get("SI_basis_description")
    copay_val = row.get("copay")
    key = (row["coverage"], row["relationship"])
    
    if gc:
        grade_values.add(gc)
        copay_by_grade[(key, gc)].add(copay_val)
    if si:
        si_values.add(si)
        copay_by_si[(key, si)].add(copay_val)

# Check if different grades have different copay for same coverage+relationship
has_multiple_grades = len(grade_values) > 1
if has_multiple_grades:
    # Group by coverage+relationship, check if different grades differ
    by_key = defaultdict(dict)
    for (key, gc), vals in copay_by_grade.items():
        by_key[key][gc] = vals
    grades_differ = any(len(set(frozenset(v) for v in grade_copays.values())) > 1 for grade_copays in by_key.values())
else:
    grades_differ = False

has_multiple_si = len(si_values) > 1
if has_multiple_si:
    by_key_si = defaultdict(dict)
    for (key, si), vals in copay_by_si.items():
        by_key_si[key][si] = vals
    si_differ = any(len(set(frozenset(v) for v in si_copays.values())) > 1 for si_copays in by_key_si.values())
else:
    si_differ = False

print(f"  Grade values found: {grade_values} -> is_graded = {grades_differ if grades_differ else None}")
print(f"  SI values found: {si_values} -> is_SI_basis = {si_differ if si_differ else None}")

# Apply flags
for row in validated_rows:
    if grades_differ:
        row["is_graded"] = True
    else:
        row["is_graded"] = None
        row["grade_classification"] = None
    
    if si_differ:
        row["is_SI_basis"] = True
    else:
        row["is_SI_basis"] = None
        row["SI_basis_description"] = None

# --- STEP 3: Merge duplicates (same coverage+relationship+grade+SI → take max copay) ---
merge_key = lambda r: (r["coverage"], r["relationship"], r.get("grade_classification"), r.get("SI_basis_description"))
merged = {}
for row in validated_rows:
    key = merge_key(row)
    if key not in merged:
        merged[key] = row
    else:
        # Take max copay (most restrictive)
        if row["copay"] > merged[key]["copay"]:
            merged[key]["copay"] = row["copay"]

validated_rows = list(merged.values())

# --- STEP 4: Ensure all relationships exist for each coverage+grade+SI group ---
# Get the family definition from the PDF - use all 4 relationships
# If copay not mentioned for a relationship, default to 0

def get_group_key(row):
    return (row["coverage"], row.get("grade_classification"), row.get("SI_basis_description"))

groups = defaultdict(list)
for row in validated_rows:
    groups[get_group_key(row)].append(row)

expanded_rows = []
for group_key, rows in groups.items():
    coverage, grade, si_desc = group_key
    existing_rels = set(r["relationship"] for r in rows)
    
    expanded_rows.extend(rows)
    
    # Add missing relationships with copay=0
    for rel in ALLOWED_RELATIONSHIP:
        if rel not in existing_rels:
            new_row = {
                "entity_id": entity_id,
                "policy_number": policy_number_extracted,
                "is_graded": rows[0]["is_graded"],
                "grade_classification": grade,
                "is_SI_basis": rows[0]["is_SI_basis"],
                "SI_basis_description": si_desc,
                "coverage": coverage,
                "relationship": rel,
                "copay": 0
            }
            expanded_rows.append(new_row)
            print(f"  \u2795 Added missing: {coverage} | {rel} | copay=0 (group: grade={grade}, SI={si_desc})")

# --- STEP 5: Ensure both network and non-network exist ---
# Group by relationship+grade+SI, check if both coverages exist
def get_rel_group(row):
    return (row["relationship"], row.get("grade_classification"), row.get("SI_basis_description"))

rel_groups = defaultdict(list)
for row in expanded_rows:
    rel_groups[get_rel_group(row)].append(row)

final_rows = []
for rel_key, rows in rel_groups.items():
    existing_cov = set(r["coverage"] for r in rows)
    final_rows.extend(rows)
    
    for cov in ALLOWED_COVERAGE:
        if cov not in existing_cov:
            # Copy from existing coverage with same copay (if not mentioned, same applies)
            template = rows[0].copy()
            template["coverage"] = cov
            final_rows.append(template)
            print(f"  \u2795 Added missing coverage: {cov} | {rel_key[0]} | copay={template['copay']}")

copay_rows = final_rows

print(f"\n--- Final: {len(copay_rows)} copay rows ---")
for i, row in enumerate(copay_rows):
    print(f"  \u2705 Row {i+1}: {row['coverage']} | {row['relationship']} | copay={row['copay']} | is_graded={row.get('is_graded')} | grade={row.get('grade_classification')} | is_SI={row.get('is_SI_basis')} | SI={row.get('SI_basis_description')}")

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
    StructField("coverage", StringType(), False),
    StructField("relationship", StringType(), False),
    StructField("copay", ByteType(), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in copay_rows:
    formatted = {}
    for col_name in schema_cols:
        val = row.get(col_name)
        field = schema[col_name]
        if isinstance(field.dataType, ByteType) and val is not None:
            try:
                v = int(float(str(val)))
                formatted[col_name] = max(-128, min(v, 127))  # ByteType range
            except (ValueError, TypeError):
                formatted[col_name] = None
        elif isinstance(field.dataType, IntegerType) and val is not None:
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
    copay_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    copay_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No copay data found - empty DataFrame created")

display(copay_df)
print(f"\n\u2705 GMC Copay extraction complete! ({copay_df.count()} rows)")
