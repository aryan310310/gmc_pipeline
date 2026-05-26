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

# DBTITLE 1,GMC SI Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in Sum Insured (SI) structures.

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
- A single policy can have MULTIPLE Sum Insured values (graded, modular, voluntary top-ups).
- Return ONE JSON object per distinct SI slab/grade/option found.
- If only ONE flat SI is mentioned, return a single row.

Rules:
- Return a JSON ARRAY of objects (even if only 1 SI found)
- amount/value fields should contain numbers only (no commas, no Rs., no INR, NO DECIMALS)
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.

2. policy_number: Extract the policy number.

3. is_multiple_SI: Boolean.
   - true if there are MULTIPLE SI values/slabs/grades/options in the policy
   - false if there is only ONE flat SI for all employees

4. SI_subcategory: The TYPE of SI structure. MUST be one of:
   - "Base" → Single flat SI for all (is_multiple_SI = false)
   - "Graded" → Different SI based on employee grade/level/designation/management level
   - "Modular" → Multiple options employees can choose from (Option 1, Option 2, etc.)
   - "Voluntary" → Optional voluntary top-up options employees can opt into
   - "Top-up" → Top-up policy with its own SI

5. SI_slab_classification: The NAME of the grade/option/slab.
   - For Base (single SI): "Flat"
   - For Graded: Use the grade name as found in PDF. Examples:
     * Named grades: "Gold", "Silver", "Bronze", "Platinum", "Diamond"
     * Designation: "Directors", "VP & Above", "AVP & Below", "Manager/Sr. Manager"
     * Numbered: "Grade1", "Grade2", "Grade3", etc. (use GradeN format if just numbered)
     * Generic: "G1", "G2", "G3" if abbreviated
   - For Modular: "Option 1", "Option 2", etc.
   - For Voluntary: "Option1", "Option2", etc.
   - For Top-up: "Top-up1", "Top-up2", etc.

6. sum_insured: The actual Sum Insured VALUE in rupees.
   - Extract as INTEGER (no decimals, no commas)
   - Convert lakhs: 3L / 3 Lakhs = 300000, 5L = 500000, 10L = 1000000

7. parental_SI_sublimit_type: If parents have a DIFFERENT (lower) SI limit.
   STRICT ENUM:
   - "Flat" → Parents have a fixed amount SI (e.g., Rs. 200000)
   - "Percent of SI" → Parents get a percentage of the main SI (e.g., 20%)
   - null if no parental sublimit / parents get same SI

8. parental_sublimit_value: The value of the parental sublimit.
   - If type is "Flat" → the rupee amount (e.g., 200000)
   - If type is "Percent of SI" → the percentage (e.g., 20)
   - null if no parental sublimit

IMPORTANT RULES FOR MULTIPLE ROWS:
- ONE row per SI slab/grade/option.
- If a policy has Grade1=3L, Grade2=5L, Grade3=10L → 3 separate rows.
- If a policy has Option1=1L, Option2=3L, Option3=5L → 3 separate rows.
- If a policy has just one flat SI for everyone → 1 row with SI_subcategory="Base", SI_slab_classification="Flat".
- entity_id and policy_number remain SAME across all rows.

HOW TO IDENTIFY SI STRUCTURE:
- Look for: "Sum Insured", "SI", "Coverage Amount", "Plan Options"
- Graded indicators: "Grade", "Level", "Band", "Category", different designations with different SIs
- Modular indicators: "Option", "Plan", "Choice", employees choose
- Voluntary indicators: "Voluntary", "Optional", "Top-up", "Buy-up"
- Parental sublimit indicators: "Parents", "In-laws", "parental", "sublimit for parents"

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "is_multiple_SI": false,
    "SI_subcategory": "Base",
    "SI_slab_classification": "Flat",
    "sum_insured": null,
    "parental_SI_sublimit_type": null,
    "parental_sublimit_value": null
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract SI data - returns a LIST of rows
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
    si_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(si_rows)} SI rows")
    for i, row in enumerate(si_rows):
        print(f"\n  Row {i+1}:")
        for k, v in row.items():
            print(f"    {k}: {v}")
else:
    # Try as single object
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        si_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        si_rows = []
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
if si_rows and si_rows[0].get("policy_number"):
    policy_number_extracted = si_rows[0]["policy_number"]

# --- VALIDATION ---
ALLOWED_SI_SUBCATEGORY = ["Base", "Graded", "Modular", "Voluntary", "Top-up"]
ALLOWED_PARENTAL_SUBLIMIT_TYPE = ["Flat", "Percent of SI"]

print("--- SI Post-processing ---")

# Determine if multiple SI
is_multiple = len(si_rows) > 1

validated_rows = []
for i, row in enumerate(si_rows):
    # Set entity_id and policy_number
    row["entity_id"] = entity_id
    if policy_number_extracted:
        row["policy_number"] = policy_number_extracted
    
    # Set is_multiple_SI
    row["is_multiple_SI"] = is_multiple
    
    # Validate SI_subcategory
    subcat = row.get("SI_subcategory")
    if subcat:
        # Normalize common variations
        subcat_clean = subcat.strip()
        if subcat_clean.lower() in ["base", "flat"]:
            row["SI_subcategory"] = "Base"
        elif subcat_clean.lower() in ["graded", "grade"]:
            row["SI_subcategory"] = "Graded"
        elif subcat_clean.lower() in ["modular", "module"]:
            row["SI_subcategory"] = "Modular"
        elif subcat_clean.lower() in ["voluntary", "voluntary top-up", "voluntary topup"]:
            row["SI_subcategory"] = "Voluntary"
        elif subcat_clean.lower() in ["top-up", "topup", "top up"]:
            row["SI_subcategory"] = "Top-up"
        elif subcat_clean not in ALLOWED_SI_SUBCATEGORY:
            row["SI_subcategory"] = "Base"  # default
    else:
        row["SI_subcategory"] = "Graded" if is_multiple else "Base"
    
    # For single SI (Base), ensure SI_slab_classification = "Flat"
    if not is_multiple and row["SI_subcategory"] == "Base":
        row["SI_slab_classification"] = "Flat"
    
    # If SI_slab_classification is missing for graded/modular, auto-number
    if not row.get("SI_slab_classification") and is_multiple:
        if row["SI_subcategory"] == "Graded":
            row["SI_slab_classification"] = f"Graded{i+1}"
        elif row["SI_subcategory"] == "Modular":
            row["SI_slab_classification"] = f"Option {i+1}"
        elif row["SI_subcategory"] == "Voluntary":
            row["SI_slab_classification"] = f"Option{i+1}"
        elif row["SI_subcategory"] == "Top-up":
            row["SI_slab_classification"] = f"Top-up{i+1}"
    
    # Validate parental_SI_sublimit_type
    pst = row.get("parental_SI_sublimit_type")
    if pst and pst not in ALLOWED_PARENTAL_SUBLIMIT_TYPE:
        # Try to normalize
        if "flat" in str(pst).lower():
            row["parental_SI_sublimit_type"] = "Flat"
        elif "percent" in str(pst).lower():
            row["parental_SI_sublimit_type"] = "Percent of SI"
        else:
            row["parental_SI_sublimit_type"] = None
            row["parental_sublimit_value"] = None
    
    # If no parental sublimit type, ensure value is also null
    if not row.get("parental_SI_sublimit_type"):
        row["parental_sublimit_value"] = None
    
    # Convert sum_insured: handle lakh notation
    si_val = row.get("sum_insured")
    if si_val is not None:
        si_str = str(si_val).replace(",", "").strip()
        # Handle lakh notation: "3L", "3 Lakh", "3 Lakhs"
        lakh_match = re.match(r'^([\d.]+)\s*(L|lakh|lakhs?)$', si_str, re.IGNORECASE)
        if lakh_match:
            row["sum_insured"] = int(float(lakh_match.group(1)) * 100000)
        else:
            try:
                row["sum_insured"] = int(float(si_str))
            except (ValueError, TypeError):
                row["sum_insured"] = None
    
    validated_rows.append(row)
    print(f"  \u2705 Row {i+1}: {row['SI_subcategory']} / {row.get('SI_slab_classification')} / SI={row.get('sum_insured')}")

si_rows = validated_rows
print(f"\n--- Final: {len(si_rows)} SI rows ---")
for i, row in enumerate(si_rows):
    print(f"  Row {i+1}: {row}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("is_multiple_SI", BooleanType(), True),
    StructField("SI_subcategory", StringType(), True),
    StructField("SI_slab_classification", StringType(), True),
    StructField("sum_insured", DecimalType(8, 0), True),
    StructField("parental_SI_sublimit_type", StringType(), True),
    StructField("parental_sublimit_value", DecimalType(8, 0), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in si_rows:
    formatted = {}
    for col_name in schema_cols:
        val = row.get(col_name)
        field = schema[col_name]
        if isinstance(field.dataType, DecimalType) and val is not None:
            try:
                formatted[col_name] = Decimal(str(int(val)))
            except (ValueError, TypeError):
                formatted[col_name] = None
        elif isinstance(field.dataType, IntegerType) and val is not None:
            try:
                formatted[col_name] = int(val)
            except (ValueError, TypeError):
                formatted[col_name] = None
        elif isinstance(field.dataType, BooleanType) and val is not None:
            formatted[col_name] = bool(val)
        else:
            formatted[col_name] = val
    formatted_rows.append(formatted)

# Create DataFrame
if formatted_rows:
    si_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    si_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No SI data found - empty DataFrame created")

display(si_df)
print(f"\n\u2705 GMC SI extraction complete! ({si_df.count()} rows)")
