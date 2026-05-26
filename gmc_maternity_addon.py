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

# DBTITLE 1,GMC Maternity Addon Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in Maternity addon benefits (twin/second birth limits).

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any value not explicitly written in the document
- HALLUCINATE amounts, limits, or coverage that is not clearly stated

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY what is EXPLICITLY written in the document.
- If maternity twin/second birth information is NOT mentioned → return all fields as null (except entity_id and policy_number).

Rules:
- Return a SINGLE JSON object (NOT an array - this table has one row per entity+policy)
- amount fields should contain numbers only (no commas, no Rs., no INR, NO DECIMALS)
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.

2. policy_number: Extract the policy number.

3. gmc_second_birth_limit: Whether maternity covers the SECOND delivery (especially for twins).
   STRICT ENUM:
   - "both" → Both first AND second delivery covered under maternity (both children covered)
   - "single" → Only FIRST delivery covered (second child/twin NOT covered or shared limit)
   - null → If not mentioned
   
   HOW TO IDENTIFY:
   - Look for: "twin", "second delivery", "second child", "multiple birth", "2 living children", "first 2 children"
   - If policy says "applicable for first 2 living children" or "both deliveries" → "both"
   - If policy says "only first child" or "single delivery only" → "single"
   - If policy mentions maternity for 2 children → "both"

4. maternity_limit_twin_enhancement: What happens to the maternity limit in case of twins/second delivery.
   STRICT ENUM:
   - "Enhanced" → The maternity limit is INCREASED/ENHANCED for twins (extra amount given)
   - "Shared" → The SAME maternity limit is SHARED between both deliveries (no extra)
   - null → If not mentioned
   
   HOW TO IDENTIFY:
   - "Enhanced": "enhanced limit", "additional maternity", "twin enhancement", "extra limit for twins", "double the limit"
   - "Shared": "within maternity limit", "shared limit", "same limit", "no additional"

5. maternity_enhancement_type: If enhanced, HOW is it enhanced.
   STRICT ENUM:
   - "Percent of Maternity" → Enhancement is a percentage of the maternity limit (e.g., 100% = double)
   - "Flat" → Enhancement is a fixed flat rupee amount (e.g., Rs. 50000 extra)
   - null → If not enhanced or not mentioned

6. maternity_enhancement_value: The value of the enhancement.
   - If type is "Percent of Maternity" → the percentage (e.g., 100 means 100% of maternity limit)
   - If type is "Flat" → the flat rupee amount (e.g., 50000)
   - null → If not enhanced or not mentioned
   Extract as INTEGER only.

EXAMPLES:
- "Maternity benefit applicable for first 2 living children" + no enhancement mentioned:
  → gmc_second_birth_limit="both", maternity_limit_twin_enhancement="Shared", type=null, value=null

- "Twin delivery: maternity limit doubled":
  → gmc_second_birth_limit="both", maternity_limit_twin_enhancement="Enhanced", type="Percent of Maternity", value=100

- "Twin delivery: additional Rs.50000":
  → gmc_second_birth_limit="both", maternity_limit_twin_enhancement="Enhanced", type="Flat", value=50000

- "Maternity only for first child":
  → gmc_second_birth_limit="single", maternity_limit_twin_enhancement="Shared", type=null, value=null

RETURN FORMAT:
{
  "entity_id": null,
  "policy_number": null,
  "gmc_second_birth_limit": null,
  "maternity_limit_twin_enhancement": null,
  "maternity_enhancement_type": null,
  "maternity_enhancement_value": null
}
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
# Extract maternity addon data using the shared LLM function
data = extract_with_llm(prompt, text)

if data:
    print(f"Extracted {len(data)} fields")
    for k, v in data.items():
        print(f"  {k}: {v}")
else:
    raise ValueError("LLM extraction failed - no valid JSON returned")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id for this policy (change per run)
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
data["entity_id"] = entity_id

# --- PDF KEYWORD VERIFICATION ---
MATERNITY_PATTERN = r'(?i)(maternit|normal\s*deliver|caesarean|c.?section)'
TWIN_PATTERN = r'(?i)(twin|second\s*deliver|second\s*child|multiple\s*birth|2\s*living\s*children|first\s*2|both\s*deliver)'

print("--- PDF Keyword Check ---")
has_maternity = bool(re.search(MATERNITY_PATTERN, text))
has_twin_info = bool(re.search(TWIN_PATTERN, text))
print(f"  Maternity mentioned in PDF: {has_maternity}")
print(f"  Twin/second birth mentioned in PDF: {has_twin_info}")

# --- VALIDATION ---
ALLOWED_SECOND_BIRTH_LIMIT = ["both", "single"]
ALLOWED_TWIN_ENHANCEMENT = ["Enhanced", "Shared"]
ALLOWED_ENHANCEMENT_TYPE = ["Percent of Maternity", "Flat"]

# Validate gmc_second_birth_limit
val = data.get("gmc_second_birth_limit")
if val:
    val_clean = val.strip().lower()
    if val_clean == "both":
        data["gmc_second_birth_limit"] = "both"
    elif val_clean == "single":
        data["gmc_second_birth_limit"] = "single"
    else:
        data["gmc_second_birth_limit"] = None

# If LLM says second_birth_limit but no twin keyword in PDF, verify
if data.get("gmc_second_birth_limit") and not has_twin_info:
    child_count_pattern = r'(?i)(first\s*2|2\s*living|two\s*(living\s*)?child|both\s*child)'
    if not re.search(child_count_pattern, text):
        if has_maternity:
            pass  # Most policies cover 2 children - keep as-is
        else:
            data["gmc_second_birth_limit"] = None

# Validate maternity_limit_twin_enhancement
val = data.get("maternity_limit_twin_enhancement")
if val:
    val_clean = val.strip()
    if val_clean in ALLOWED_TWIN_ENHANCEMENT:
        data["maternity_limit_twin_enhancement"] = val_clean
    elif "enhance" in val_clean.lower() or "additional" in val_clean.lower() or "extra" in val_clean.lower() or "double" in val_clean.lower():
        data["maternity_limit_twin_enhancement"] = "Enhanced"
    elif "share" in val_clean.lower() or "same" in val_clean.lower() or "within" in val_clean.lower():
        data["maternity_limit_twin_enhancement"] = "Shared"
    else:
        data["maternity_limit_twin_enhancement"] = None

# Validate maternity_enhancement_type
val = data.get("maternity_enhancement_type")
if val:
    val_clean = val.strip()
    if val_clean in ALLOWED_ENHANCEMENT_TYPE:
        data["maternity_enhancement_type"] = val_clean
    elif "percent" in val_clean.lower():
        data["maternity_enhancement_type"] = "Percent of Maternity"
    elif "flat" in val_clean.lower():
        data["maternity_enhancement_type"] = "Flat"
    else:
        data["maternity_enhancement_type"] = None

# --- LOGIC RULES ---

# RULE 1: If twin/both is mentioned and it's "Shared" → 
#   Shared means both children share the same maternity limit (100% of maternity)
#   So: enhancement_type = "Percent of Maternity", enhancement_value = 100
if data.get("maternity_limit_twin_enhancement") == "Shared":
    data["maternity_enhancement_type"] = "Percent of Maternity"
    data["maternity_enhancement_value"] = 100

# RULE 2: If "Enhanced" → use LLM-extracted type and value
#   If type/value missing, default to Percent of Maternity / 100
if data.get("maternity_limit_twin_enhancement") == "Enhanced":
    if not data.get("maternity_enhancement_type"):
        data["maternity_enhancement_type"] = "Percent of Maternity"
    if not data.get("maternity_enhancement_value"):
        data["maternity_enhancement_value"] = 100

# RULE 3: If no second birth limit info at all, ensure everything is null
if not data.get("gmc_second_birth_limit"):
    data["maternity_limit_twin_enhancement"] = None
    data["maternity_enhancement_type"] = None
    data["maternity_enhancement_value"] = None

# RULE 4: If no twin enhancement info but second_birth_limit exists, default to Shared
if data.get("gmc_second_birth_limit") and not data.get("maternity_limit_twin_enhancement"):
    data["maternity_limit_twin_enhancement"] = "Shared"
    data["maternity_enhancement_type"] = "Percent of Maternity"
    data["maternity_enhancement_value"] = 100

# Payment type correction: if value > 100 and type is Percent, switch to Flat
if data.get("maternity_enhancement_value"):
    try:
        val_int = int(data["maternity_enhancement_value"])
        if val_int > 100 and data.get("maternity_enhancement_type") == "Percent of Maternity":
            data["maternity_enhancement_type"] = "Flat"
    except (ValueError, TypeError):
        pass

print("\n--- Final Values ---")
for k, v in data.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("gmc_second_birth_limit", StringType(), True),
    StructField("maternity_limit_twin_enhancement", StringType(), True),
    StructField("maternity_enhancement_type", StringType(), True),
    StructField("maternity_enhancement_value", DecimalType(8, 0), True)
])

# Convert data to proper types
schema_cols = [f.name for f in schema.fields]
filtered_data = {}
for col_name in schema_cols:
    val = data.get(col_name)
    field = schema[col_name]
    if isinstance(field.dataType, DecimalType) and val is not None:
        try:
            filtered_data[col_name] = Decimal(str(int(val)))
        except (ValueError, TypeError):
            filtered_data[col_name] = None
    elif isinstance(field.dataType, IntegerType) and val is not None:
        try:
            filtered_data[col_name] = int(val)
        except (ValueError, TypeError):
            filtered_data[col_name] = None
    else:
        filtered_data[col_name] = val

# Create DataFrame
maternity_addon_df = spark.createDataFrame([filtered_data], schema=schema)
display(maternity_addon_df)

print(f"\n\u2705 GMC Maternity Addon extraction complete!")
