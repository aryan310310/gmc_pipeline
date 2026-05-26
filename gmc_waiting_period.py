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

# DBTITLE 1,GMC Waiting Period Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in waiting period details.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any value not explicitly written in the document
- HALLUCINATE values that are not clearly stated

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY what is EXPLICITLY written in the document.
- Focus on: Which waiting periods are WAIVED (covered from day 1) vs which are NOT WAIVED (have a waiting period).
- Return a JSON ARRAY of objects.

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_number: Extract the policy number.

3. waiting_period_category: STRING. The type of waiting period.
   Allowed values (ENUM):
   - "Initial - 30 Days" : Initial waiting period (first 30 days exclusion)
   - "1st Year" : First year specific disease/illness exclusion
   - "2nd Year" : Second year specific disease/illness exclusion
   - "3rd Year - PED" : Third year Pre-Existing Disease waiting period
   - "4th Year - PED" : Fourth year Pre-Existing Disease waiting period
   - "3rd Year" : Third year general exclusion (non-PED)
   - "4th Year" : Fourth year general exclusion (non-PED)
   - "Maternity" : Maternity waiting period

4. is_waived: BOOLEAN.
   - true = Waiting period is WAIVED / Covered from Day 1 / No waiting period
   - false = Waiting period is NOT waived / Applicable / Has waiting days

5. waiting_period_days: INTEGER.
   - 0 if is_waived = true (no waiting period)
   - Number of days if is_waived = false:
     * Initial 30 Days: typically 30
     * 1st Year: typically 365
     * 2nd Year: typically 730
     * 3rd Year / 3rd Year - PED: typically 1095 (or 30 if reduced)
     * 4th Year / 4th Year - PED: typically 1460 (or 30 if reduced)
     * Maternity: typically 270 (9 months)

EXTRACTION RULES:

1. PRE-EXISTING DISEASE (PED) waived:
   - If document says PED is "covered", "waived", "from day 1", "no waiting period for PED"
   - → Create 2 rows: "3rd Year - PED" (is_waived=true, days=0) AND "4th Year - PED" (is_waived=true, days=0)

2. SPECIFIC DISEASE waiting period waived:
   - If document says "specific disease waiting period waived" or "1st/2nd year exclusion waived"
   - → Create 2 rows: "1st Year" (is_waived=true, days=0) AND "2nd Year" (is_waived=true, days=0)

3. INITIAL WAITING PERIOD waived:
   - If document says "initial waiting period waived" or "30 day waiting waived" or "no initial waiting"
   - → Create 1 row: "Initial - 30 Days" (is_waived=true, days=0)

4. ALL YEAR EXCLUSIONS waived (1st/2nd/3rd/4th year):
   - If document says "all waiting periods waived" or "1st/2nd/3rd/4th year exclusions waived"
   - → Create 4 rows: "1st Year", "2nd Year", "3rd Year", "4th Year" (all is_waived=true, days=0)

5. PED NOT waived (waiting period applicable):
   - If PED has a waiting period (e.g., "48 months", "4 years")
   - → Create rows with is_waived=false and appropriate days (1095 for 3yr, 1460 for 4yr)

6. MATERNITY waiting period:
   - If maternity waiting period is waived → "Maternity" (is_waived=true, days=0)
   - If maternity has 9 months waiting → "Maternity" (is_waived=false, days=270)

IMPORTANT CLUES:
- "Waiting Period" / "Exclusion Period" section in the PDF
- "Day 1 Coverage" / "From inception" / "No waiting period" = is_waived = true
- "Pre-existing" / "PED" / "Pre-Existing Disease" = relates to 3rd Year - PED / 4th Year - PED
- "Specific diseases" / "Named diseases" / "Listed ailments" = relates to 1st Year / 2nd Year
- "Initial waiting" / "First 30 days" / "Cooling period" = Initial - 30 Days
- Look for specific mentions like: "All waiting periods and exclusions are waived"

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "waiting_period_category": "Initial - 30 Days",
    "is_waived": true,
    "waiting_period_days": 0
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract waiting period data - returns a LIST of rows
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
    wp_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(wp_rows)} waiting period rows")
    for i, row in enumerate(wp_rows):
        print(f"  Row {i+1}: {row.get('waiting_period_category')} | is_waived={row.get('is_waived')} | days={row.get('waiting_period_days')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        wp_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        wp_rows = []
        print("NO VALID JSON FOUND")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id and policy_number
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

if wp_rows and wp_rows[0].get("policy_number"):
    policy_number_extracted = wp_rows[0]["policy_number"]

print("--- Waiting Period Post-processing ---")

# --- ALLOWED CATEGORIES ---
ALLOWED_CATEGORIES = [
    "Initial - 30 Days",
    "1st Year",
    "2nd Year",
    "3rd Year - PED",
    "4th Year - PED",
    "3rd Year",
    "4th Year",
    "Maternity"
]

# --- STEP 1: Normalize and validate each row ---
validated_rows = []
for row in wp_rows:
    row["entity_id"] = entity_id
    row["policy_number"] = policy_number_extracted
    
    # Normalize waiting_period_category
    cat = str(row.get("waiting_period_category", "")).strip()
    cat_lower = cat.lower()
    
    # Map variations to standard categories
    if cat_lower in ["initial - 30 days", "initial 30 days", "initial waiting", "30 days", "cooling period"]:
        row["waiting_period_category"] = "Initial - 30 Days"
    elif cat_lower in ["1st year", "first year", "1 year", "year 1"]:
        row["waiting_period_category"] = "1st Year"
    elif cat_lower in ["2nd year", "second year", "2 year", "year 2"]:
        row["waiting_period_category"] = "2nd Year"
    elif cat_lower in ["3rd year - ped", "3rd year ped", "ped 3rd year", "pre-existing 3rd year", "ped 3 years"]:
        row["waiting_period_category"] = "3rd Year - PED"
    elif cat_lower in ["4th year - ped", "4th year ped", "ped 4th year", "pre-existing 4th year", "ped 4 years"]:
        row["waiting_period_category"] = "4th Year - PED"
    elif cat_lower in ["3rd year", "third year", "3 year", "year 3"]:
        row["waiting_period_category"] = "3rd Year"
    elif cat_lower in ["4th year", "fourth year", "4 year", "year 4"]:
        row["waiting_period_category"] = "4th Year"
    elif cat_lower in ["maternity", "maternity waiting", "9 month", "9 months", "nine months"]:
        row["waiting_period_category"] = "Maternity"
    elif "ped" in cat_lower or "pre-existing" in cat_lower or "pre existing" in cat_lower:
        # Generic PED mention - split into 3rd and 4th year PED
        row["waiting_period_category"] = "3rd Year - PED"
        # Add 4th year PED as well
        row_4th = row.copy()
        row_4th["waiting_period_category"] = "4th Year - PED"
        validated_rows.append(row_4th)
    elif "specific" in cat_lower or "named disease" in cat_lower:
        # Specific disease - split into 1st and 2nd year
        row["waiting_period_category"] = "1st Year"
        row_2nd = row.copy()
        row_2nd["waiting_period_category"] = "2nd Year"
        validated_rows.append(row_2nd)
    else:
        # Skip unrecognized categories
        print(f"  \u26a0\ufe0f Skipping unrecognized category: '{cat}'")
        continue
    
    # Normalize is_waived
    waived = row.get("is_waived")
    if isinstance(waived, str):
        row["is_waived"] = waived.lower() in ["true", "yes", "1", "waived", "covered"]
    elif waived is None:
        row["is_waived"] = True  # default: waived if not mentioned
    else:
        row["is_waived"] = bool(waived)
    
    # waiting_period_days: 0 if waived, otherwise validate
    if row["is_waived"]:
        row["waiting_period_days"] = 0
    else:
        days = row.get("waiting_period_days")
        if days is not None:
            try:
                row["waiting_period_days"] = int(float(str(days)))
            except (ValueError, TypeError):
                defaults = {
                    "Initial - 30 Days": 30,
                    "1st Year": 365,
                    "2nd Year": 730,
                    "3rd Year": 1095,
                    "3rd Year - PED": 1095,
                    "4th Year": 1460,
                    "4th Year - PED": 1460,
                    "Maternity": 270
                }
                row["waiting_period_days"] = defaults.get(row["waiting_period_category"], 0)
        else:
            defaults = {
                "Initial - 30 Days": 30,
                "1st Year": 365,
                "2nd Year": 730,
                "3rd Year": 1095,
                "3rd Year - PED": 1095,
                "4th Year": 1460,
                "4th Year - PED": 1460,
                "Maternity": 270
            }
            row["waiting_period_days"] = defaults.get(row["waiting_period_category"], 0)
    
    validated_rows.append(row)

# --- STEP 2: Apply expansion rules based on PDF keywords ---
PED_WAIVED_PATTERN = r'(?i)(pre.?exist\w*\s*(disease|condition|ailment)?\s*(is\s*)?\s*(waiv|cover|from\s*day\s*1|no\s*waiting|nil))'
SPECIFIC_WAIVED_PATTERN = r'(?i)(specific\s*(disease|illness|ailment)\s*(waiting|exclusion)?\s*(period)?\s*(is\s*)?\s*(waiv|cover|nil|not\s*applicable))'
INITIAL_WAIVED_PATTERN = r'(?i)(initial\s*(waiting|30\s*day|cooling)\s*(period)?\s*(is\s*)?\s*(waiv|cover|nil|not\s*applicable))'
ALL_WAIVED_PATTERN = r'(?i)(all\s*(waiting|exclusion)\s*(period)?s?\s*(are\s*)?\s*(waiv|cover|nil|not\s*applicable))'
MATERNITY_WAIVED_PATTERN = r'(?i)(maternit\w*\s*(waiting|exclusion)?\s*(period)?\s*(is\s*)?\s*(waiv|cover|nil|from\s*day|9\s*month))'

existing_categories = set(r["waiting_period_category"] for r in validated_rows)

# If "all waiting periods waived" found
if re.search(ALL_WAIVED_PATTERN, text):
    print("  \U0001f4a1 Detected: ALL waiting periods waived")
    for cat in ["1st Year", "2nd Year", "3rd Year", "4th Year"]:
        if cat not in existing_categories:
            validated_rows.append({
                "entity_id": entity_id,
                "policy_number": policy_number_extracted,
                "waiting_period_category": cat,
                "is_waived": True,
                "waiting_period_days": 0
            })
            existing_categories.add(cat)
            print(f"  \u2795 Appended: {cat} (is_waived=true, days=0)")

# If PED waived and no PED rows exist
if re.search(PED_WAIVED_PATTERN, text):
    print("  \U0001f4a1 Detected: PED waiting period waived")
    for cat in ["3rd Year - PED", "4th Year - PED"]:
        if cat not in existing_categories:
            validated_rows.append({
                "entity_id": entity_id,
                "policy_number": policy_number_extracted,
                "waiting_period_category": cat,
                "is_waived": True,
                "waiting_period_days": 0
            })
            existing_categories.add(cat)
            print(f"  \u2795 Appended: {cat} (is_waived=true, days=0)")

# If Specific disease waived and no 1st/2nd year rows exist
if re.search(SPECIFIC_WAIVED_PATTERN, text):
    print("  \U0001f4a1 Detected: Specific disease waiting period waived")
    for cat in ["1st Year", "2nd Year"]:
        if cat not in existing_categories:
            validated_rows.append({
                "entity_id": entity_id,
                "policy_number": policy_number_extracted,
                "waiting_period_category": cat,
                "is_waived": True,
                "waiting_period_days": 0
            })
            existing_categories.add(cat)
            print(f"  \u2795 Appended: {cat} (is_waived=true, days=0)")

# If Initial waiting waived
if re.search(INITIAL_WAIVED_PATTERN, text):
    print("  \U0001f4a1 Detected: Initial waiting period waived")
    if "Initial - 30 Days" not in existing_categories:
        validated_rows.append({
            "entity_id": entity_id,
            "policy_number": policy_number_extracted,
            "waiting_period_category": "Initial - 30 Days",
            "is_waived": True,
            "waiting_period_days": 0
        })
        existing_categories.add("Initial - 30 Days")
        print(f"  \u2795 Appended: Initial - 30 Days (is_waived=true, days=0)")

# If Maternity waived
if re.search(MATERNITY_WAIVED_PATTERN, text):
    print("  \U0001f4a1 Detected: Maternity waiting period waived")
    if "Maternity" not in existing_categories:
        validated_rows.append({
            "entity_id": entity_id,
            "policy_number": policy_number_extracted,
            "waiting_period_category": "Maternity",
            "is_waived": True,
            "waiting_period_days": 0
        })
        existing_categories.add("Maternity")
        print(f"  \u2795 Appended: Maternity (is_waived=true, days=0)")

# --- STEP 3: CRITICAL RULE - If other major waiting periods are ALL waived,
# then Initial - 30 Days MUST also be waived (is_waived=true, days=0) ---
# Logic: if PED and specific disease exclusions are waived, initial 30 days is definitely waived too
other_waived_categories = {"1st Year", "2nd Year", "3rd Year - PED", "4th Year - PED"}
waived_cats = set(r["waiting_period_category"] for r in validated_rows if r.get("is_waived") == True)

if other_waived_categories.issubset(waived_cats):
    # Force Initial - 30 Days to waived
    for row in validated_rows:
        if row["waiting_period_category"] == "Initial - 30 Days" and row["is_waived"] == False:
            print(f"  \U0001f504 Overriding: Initial - 30 Days -> is_waived=True, days=0 (all other WPs are waived)")
            row["is_waived"] = True
            row["waiting_period_days"] = 0

# --- STEP 4: Deduplicate (keep unique category) ---
seen = set()
final_rows = []
for row in validated_rows:
    key = row["waiting_period_category"]
    if key not in seen:
        seen.add(key)
        final_rows.append(row)

wp_rows = final_rows

print(f"\n--- Final: {len(wp_rows)} waiting period rows ---")
for i, row in enumerate(wp_rows):
    print(f"  \u2705 Row {i+1}: {row['waiting_period_category']} | is_waived={row['is_waived']} | days={row['waiting_period_days']}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("waiting_period_category", StringType(), False),
    StructField("is_waived", BooleanType(), True),
    StructField("waiting_period_days", IntegerType(), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in wp_rows:
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
    waiting_period_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    waiting_period_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No waiting period data found - empty DataFrame created")

display(waiting_period_df)
print(f"\n\u2705 GMC Waiting Period extraction complete! ({waiting_period_df.count()} rows)")
