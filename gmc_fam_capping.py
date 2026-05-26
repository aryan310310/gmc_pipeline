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

# DBTITLE 1,GMC Family Capping Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in family capping (maximum lives allowed per relationship).

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
- A single policy has MULTIPLE rows - one per relationship type.
- If family capping is NOT mentioned at all → return defaults based on family structure.

Rules:
- Return a JSON ARRAY of objects (one per relationship type)
- All numeric fields should be INTEGERS
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.

2. policy_number: Extract the policy number.

3. relationship: The family member relationship. POSSIBLE VALUES:
   - "employee" → Self / Employee (always max_allowed = 1)
   - "spouse" → Spouse / Husband / Wife (always max_allowed = 1)
   - "child" → Children (typically 2, can be 3, 4, or 6)
   - "parents" → Parents (typically 2, can be 4 if cross-combination/in-laws included)
   - "parents in law" → Parents-in-law specifically (if separately mentioned)
   - "parent" → Single parent (if specifically limited)
   - "father" / "mother" / "father in law" / "mother in law" → Individual parent entries
   - "Brother" / "Sister" → Siblings if covered

4. max_allowed: Maximum number of lives allowed for this relationship.
   - employee: ALWAYS 1
   - spouse: ALWAYS 1
   - child: Default 2, but can be 1, 3, 4, or 6 if explicitly stated
   - parents: Default 2, but can be 4 if cross-combination allowed (both own parents + in-laws)
   - parents in law: typically 2
   - Individual (father/mother/etc): 1

5. max_entry_age: Maximum age at which this person can ENTER the policy.
   - null if not mentioned
   - Look for: "entry age", "joining age", "age at entry"

6. max_coverage_age: Maximum age until which this person REMAINS covered.
   - null if not mentioned
   - child: Look for "up to 25 years", "till 18", "age limit"
   - employee: Look for "retirement age", "up to 65/70/80 years"
   - parents: Look for "up to 80/85/90 years"
   - Look for: "coverage age", "age limit", "covered till age", "up to age"

IMPORTANT - HOW TO DETERMINE MAX_ALLOWED:
- If "Employee + Spouse + 2 Children + 2 Parents" → employee=1, spouse=1, child=2, parents=2
- If "4 Children allowed" or "up to 4 dependents" with children context → child=4
- If "Cross combination of parents" or "Own parents + In-laws" → parents=4
- If "2 parents OR 2 parents-in-law" (not both) → parents=2
- If "2 parents AND 2 parents-in-law" → parents=4 (or separate rows: parents=2 + parents in law=2)
- Look for: "Family Definition", "Family Structure", "Eligible Dependents", "Family Composition"

AGE CAPPING CLUES:
- Children: "up to 25 years", "till 18 years", "dependent child up to age 25"
- Employee: "65 years", "70 years", "no age bar"
- Parents: "up to 80 years", "no age bar", "85 years"
- Spouse: usually no specific age cap (80-100)

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "relationship": "employee",
    "max_allowed": 1,
    "max_entry_age": null,
    "max_coverage_age": null
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract family capping data - returns a LIST of rows
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
    fam_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(fam_rows)} family capping rows")
    for i, row in enumerate(fam_rows):
        print(f"  Row {i+1}: {row.get('relationship')} | max_allowed={row.get('max_allowed')} | entry_age={row.get('max_entry_age')} | coverage_age={row.get('max_coverage_age')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        fam_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        fam_rows = []
        print("NO VALID JSON FOUND")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id and policy_number for all rows
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

# Get policy_number from first row
if fam_rows and fam_rows[0].get("policy_number"):
    policy_number_extracted = fam_rows[0]["policy_number"]

print("--- Family Capping Post-processing ---")

# --- STEP 1: Normalize all relationships to 4 categories ---
# employee, spouse, child, parents (ONLY these 4)
for row in fam_rows:
    rel = row.get("relationship", "").strip().lower()
    if rel in ["self", "employee"]:
        row["relationship"] = "employee"
    elif rel in ["spouse", "wife", "husband"]:
        row["relationship"] = "spouse"
    elif rel in ["child", "children", "kids", "son", "daughter", "brother", "sister", "sibling"]:
        row["relationship"] = "child"
    elif rel in ["parents", "parent", "parents in law", "parent in law", "in-laws", "in-law",
                 "father", "mother", "father in law", "mother in law", "father-in-law", "mother-in-law"]:
        row["relationship"] = "parents"
    elif rel in ["dependent", "dependents"]:
        row["relationship"] = "child"  # map generic dependent to child
    else:
        row["relationship"] = "child"  # fallback

# --- STEP 2: Merge duplicates (take max of max_allowed, max of ages) ---
# Group by relationship
from collections import defaultdict
merged = defaultdict(lambda: {"max_allowed": None, "max_entry_age": None, "max_coverage_age": None})

for row in fam_rows:
    rel = row["relationship"]
    # max_allowed: take the highest value (most permissive)
    ma = row.get("max_allowed")
    if ma is not None:
        try:
            ma_int = int(ma)
            if merged[rel]["max_allowed"] is None or ma_int > merged[rel]["max_allowed"]:
                merged[rel]["max_allowed"] = ma_int
        except (ValueError, TypeError):
            pass
    # max_entry_age: take the highest
    ea = row.get("max_entry_age")
    if ea is not None:
        try:
            ea_int = int(ea)
            if 0 <= ea_int <= 150:
                if merged[rel]["max_entry_age"] is None or ea_int > merged[rel]["max_entry_age"]:
                    merged[rel]["max_entry_age"] = ea_int
        except (ValueError, TypeError):
            pass
    # max_coverage_age: take the highest
    ca = row.get("max_coverage_age")
    if ca is not None:
        try:
            ca_int = int(ca)
            if 0 <= ca_int <= 150:
                if merged[rel]["max_coverage_age"] is None or ca_int > merged[rel]["max_coverage_age"]:
                    merged[rel]["max_coverage_age"] = ca_int
        except (ValueError, TypeError):
            pass

# --- STEP 3: Apply defaults ---
# Employee: ALWAYS 1
if "employee" in merged:
    merged["employee"]["max_allowed"] = 1
else:
    merged["employee"] = {"max_allowed": 1, "max_entry_age": None, "max_coverage_age": None}

# Spouse: ALWAYS 1
if "spouse" in merged:
    merged["spouse"]["max_allowed"] = 1
else:
    merged["spouse"] = {"max_allowed": 1, "max_entry_age": None, "max_coverage_age": None}

# Child: default 2 if not explicitly stated or invalid
if "child" in merged:
    ma = merged["child"]["max_allowed"]
    if ma is None or ma not in [1, 2, 3, 4, 6, 7]:
        merged["child"]["max_allowed"] = 2
else:
    # Add child if dependents mentioned in PDF
    child_pattern = r'(?i)(child|children|son|daughter|dependent)'
    if re.search(child_pattern, text):
        merged["child"] = {"max_allowed": 2, "max_entry_age": None, "max_coverage_age": None}

# Parents: default 2, can be 4 if cross-combination
if "parents" in merged:
    ma = merged["parents"]["max_allowed"]
    if ma is None or ma not in [1, 2, 4]:
        merged["parents"]["max_allowed"] = 2
else:
    parents_pattern = r'(?i)(parent|father|mother|in.?law)'
    if re.search(parents_pattern, text):
        merged["parents"] = {"max_allowed": 2, "max_entry_age": None, "max_coverage_age": None}

# --- STEP 4: Build final rows ---
fam_rows = []
for rel in ["employee", "spouse", "child", "parents"]:
    if rel in merged:
        fam_rows.append({
            "entity_id": entity_id,
            "policy_number": policy_number_extracted,
            "relationship": rel,
            "max_allowed": merged[rel]["max_allowed"],
            "max_entry_age": merged[rel]["max_entry_age"],
            "max_coverage_age": merged[rel]["max_coverage_age"]
        })

print(f"\n--- Final: {len(fam_rows)} family capping rows ---")
for i, row in enumerate(fam_rows):
    print(f"  \u2705 Row {i+1}: {row['relationship']} | max_allowed={row['max_allowed']} | entry_age={row.get('max_entry_age')} | coverage_age={row.get('max_coverage_age')}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("relationship", StringType(), False),
    StructField("max_allowed", ByteType(), True),
    StructField("max_entry_age", ShortType(), True),
    StructField("max_coverage_age", ShortType(), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in fam_rows:
    formatted = {}
    for col_name in schema_cols:
        val = row.get(col_name)
        field = schema[col_name]
        if isinstance(field.dataType, ByteType) and val is not None:
            try:
                int_val = int(val)
                if -128 <= int_val <= 127:
                    formatted[col_name] = int_val
                else:
                    formatted[col_name] = None
            except (ValueError, TypeError):
                formatted[col_name] = None
        elif isinstance(field.dataType, ShortType) and val is not None:
            try:
                int_val = int(val)
                if -32768 <= int_val <= 32767:
                    formatted[col_name] = int_val
                else:
                    formatted[col_name] = None
            except (ValueError, TypeError):
                formatted[col_name] = None
        elif isinstance(field.dataType, IntegerType) and val is not None:
            try:
                formatted[col_name] = int(val)
            except (ValueError, TypeError):
                formatted[col_name] = None
        else:
            formatted[col_name] = val
    formatted_rows.append(formatted)

# Create DataFrame
if formatted_rows:
    fam_capping_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    fam_capping_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No family capping data found - empty DataFrame created")

display(fam_capping_df)
print(f"\n\u2705 GMC Family Capping extraction complete! ({fam_capping_df.count()} rows)")
