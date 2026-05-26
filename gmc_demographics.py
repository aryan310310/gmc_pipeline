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

# DBTITLE 1,GMC Demographics Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in demographics/lives covered.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any value not explicitly written in the document
- HALLUCINATE numbers or counts that are not clearly stated

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY what is EXPLICITLY written in the document.
- If lives/member count is NOT mentioned at all → return an EMPTY list: []
- A single policy has MULTIPLE rows - one per relationship type found.

Rules:
- Return a JSON ARRAY of objects (one per relationship type)
- count fields should contain INTEGER numbers only
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.

2. policy_number: Extract the policy number.

3. relationship: The relationship/category of people covered. STRICT ENUM:
   - "employee" → Self / Employee / Insured Employee / Member (the primary insured)
   - "spouse" → Spouse / Husband / Wife
   - "child" → Child / Children / Son / Daughter / Kids
   - "parents" → Parents / Father / Mother / In-laws / Parent-in-law
   - "dependent" → Dependents (general, when not broken down into spouse/child/parents)
   - "total" → Total lives / Total members covered (sum of all)

4. count: The NUMBER of lives/people in that relationship category.
   - Extract as INTEGER only
   - Look for: "Number of Lives", "No. of Members", "Lives Covered", "Total Lives"
   - If separate counts given for Self and Dependent, extract both as separate rows

IMPORTANT RULES:
- Create ONE row per relationship type found in the document.
- "Self" = 208, "Dependent" = 115 → 2 rows (employee=208, dependent=115)
- If TOTAL is mentioned, add a "total" row too.
- If only total is mentioned without breakdown, just create 1 row with "total".
- If "Self: 208" and "Dependent: 115" are mentioned, also calculate and add total = 323.
- entity_id and policy_number remain SAME across all rows.

HOW TO IDENTIFY:
- Look for: "Insured Person Details", "Number of Lives", "Lives Covered"
- Look for tables with "Relationship Type" and "Number of Lives" columns
- "Self" or "Employee" = employee
- "Dependent" = dependent (if not broken down further)
- If dependents ARE broken down (spouse: X, child: Y, parents: Z), use those specific types

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "relationship": "employee",
    "count": null
  }
]

If NO demographics/lives data is found, return: []
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract demographics data - returns a LIST of rows
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
    demo_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(demo_rows)} demographics rows")
    for i, row in enumerate(demo_rows):
        print(f"  Row {i+1}: {row.get('relationship')} = {row.get('count')}")
else:
    # Try as single object
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        demo_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        demo_rows = []
        print("NO VALID JSON FOUND - assuming no demographics data")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id and policy_number for all rows
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

# Get policy_number from first row
if demo_rows and demo_rows[0].get("policy_number"):
    policy_number_extracted = demo_rows[0]["policy_number"]

# --- PDF KEYWORD VERIFICATION ---
LIVES_PATTERN = r'(?i)(number\s*of\s*lives|lives\s*covered|total\s*lives|insured\s*person|members?\s*covered)'

print("--- PDF Keyword Check ---")
has_lives_data = bool(re.search(LIVES_PATTERN, text))
print(f"  Lives/demographics mentioned in PDF: {has_lives_data}")

if not has_lives_data and demo_rows:
    print(f"  \u26a0\ufe0f LLM returned {len(demo_rows)} rows but NO lives keyword in PDF \u2192 clearing all rows")
    demo_rows = []

# --- VALIDATION ---
ALLOWED_RELATIONSHIPS = ["employee", "spouse", "child", "parents", "dependent", "total"]

print("\n--- Row Validation ---")
validated_rows = []
for i, row in enumerate(demo_rows):
    # Set entity_id and policy_number
    row["entity_id"] = entity_id
    if policy_number_extracted:
        row["policy_number"] = policy_number_extracted
    
    # Normalize relationship
    rel = row.get("relationship", "").strip().lower()
    if rel in ["self", "employee", "insured", "member"]:
        row["relationship"] = "employee"
    elif rel in ["spouse", "husband", "wife"]:
        row["relationship"] = "spouse"
    elif rel in ["child", "children", "son", "daughter", "kids"]:
        row["relationship"] = "child"
    elif rel in ["parents", "parent", "father", "mother", "in-law", "in-laws", "parent-in-law"]:
        row["relationship"] = "parents"
    elif rel in ["dependent", "dependents", "dependant", "dependants"]:
        row["relationship"] = "dependent"
    elif rel in ["total", "all", "total lives"]:
        row["relationship"] = "total"
    elif rel not in ALLOWED_RELATIONSHIPS:
        print(f"  \u26a0\ufe0f Row {i+1}: Unknown relationship '{rel}' \u2192 skipping")
        continue
    
    # Validate count
    count_val = row.get("count")
    if count_val is not None:
        try:
            row["count"] = int(count_val)
            if row["count"] <= 0:
                print(f"  \u26a0\ufe0f Row {i+1}: Count <= 0 \u2192 skipping")
                continue
        except (ValueError, TypeError):
            print(f"  \u26a0\ufe0f Row {i+1}: Invalid count '{count_val}' \u2192 skipping")
            continue
    else:
        print(f"  \u26a0\ufe0f Row {i+1}: No count value \u2192 skipping")
        continue
    
    validated_rows.append(row)
    print(f"  \u2705 Row {i+1}: {row['relationship']} = {row['count']}")

demo_rows = validated_rows

# --- AUTO-CALCULATE TOTAL ---
# If we have employee + dependent (or breakdown) but no total, calculate it
relationships_found = [r["relationship"] for r in demo_rows]
if "total" not in relationships_found and demo_rows:
    total_count = sum(r["count"] for r in demo_rows)
    demo_rows.append({
        "entity_id": entity_id,
        "policy_number": policy_number_extracted,
        "relationship": "total",
        "count": total_count
    })
    print(f"  \u2795 Auto-calculated total: {total_count}")

print(f"\n--- Final: {len(demo_rows)} demographics rows ---")
for i, row in enumerate(demo_rows):
    print(f"  Row {i+1}: {row['relationship']} = {row['count']}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("relationship", StringType(), False),
    StructField("count", IntegerType(), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in demo_rows:
    formatted = {}
    for col_name in schema_cols:
        val = row.get(col_name)
        if col_name in ["entity_id", "count"] and val is not None:
            try:
                formatted[col_name] = int(val)
            except (ValueError, TypeError):
                formatted[col_name] = None
        else:
            formatted[col_name] = val
    formatted_rows.append(formatted)

# Create DataFrame
if formatted_rows:
    demographics_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    demographics_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No demographics data found - empty DataFrame created")

display(demographics_df)
print(f"\n\u2705 GMC Demographics extraction complete! ({demographics_df.count()} rows)")
