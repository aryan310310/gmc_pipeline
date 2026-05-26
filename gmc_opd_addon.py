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

# DBTITLE 1,GMC OPD Addon Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in OPD (Out-Patient Department) benefits.

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
- If OPD is NOT mentioned at all in the document → return an EMPTY list: []
- A single policy can have MULTIPLE OPD benefits (e.g., General OPD, Dental OPD, Vision OPD, etc.)
- Return ONE JSON object per distinct OPD benefit found.

Rules:
- Return a JSON ARRAY of objects (even if only 1 OPD benefit found)
- amount/value fields should contain numbers only (no commas, no Rs., no INR, NO DECIMALS)
- if a sub-field is missing for a specific OPD type, return null for that field
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. opd_description: The TYPE of OPD benefit. MUST be one of these EXACT values:
   - "General OPD" (general consultation, doctor visits, medicine, diagnostics)
   - "Dental OPD" (dental treatment, dental consultation)
   - "Dental RCT Only" (only root canal treatment)
   - "Vision OPD" (eye/vision, spectacles, lenses)
   - "Accidental OPD" (OPD due to accident)
   - "Illness OPD" (OPD due to illness)
   - "Psychiatric OPD" (mental health, psychiatrist, counselling OPD)
   - "Cancer OPD" (cancer/oncology OPD)
   - "Covid OPD" (COVID-19 OPD)
   - "Autism OPD" (autism related OPD)
   - "IVF OPD" (infertility/IVF OPD)
   - "Pre & Post-natal OPD" (maternity related OPD, prenatal, postnatal)
   - "Well baby OPD" (well baby, vaccination, immunization OPD)
   - "Animal or Snake Bite OPD" (animal bite, snake bite OPD)
   - "Sleep apnea OPD" (sleep apnea related OPD)

2. opd_limit_type: STRICT ENUM:
   - "Within SI" (within sum insured)
   - "Above SI" (additional, over and above SI)
   - "Within Maternity" (within maternity limit)

3. opd_payment_type: STRICT ENUM:
   - "Flat" (fixed amount in rupees)
   - "Percent of SI" (percentage of sum insured)
   - "Percent of Maternity" (percentage of maternity limit)

4. opd_SI_basis: The sum insured slab/basis for this OPD benefit.
   Values like: "6L", "8L", "10L", "Self", "Floater"
   - Use "6L" for 6 lakh SI, "8L" for 8 lakh, "10L" for 10 lakh etc.
   - Use "Self" if OPD is on individual basis
   - Use "Floater" if OPD is on floater basis
   - null if not mentioned

5. opd_value: The amount or percentage value.
   - If payment_type is "Flat" → the rupee amount (e.g., 5000, 10000)
   - If payment_type is "Percent of SI" or "Percent of Maternity" → the percentage (e.g., 100)
   Extract as INTEGER only.

6. opd_dep_sublimit: Dependent sub-limit if mentioned separately.
   Extract as INTEGER only. null if not mentioned.

IMPORTANT RULES FOR MULTIPLE ROWS:
- If the policy has DIFFERENT SI slabs with different OPD amounts (e.g., 6L SI gets Rs.5000, 8L gets Rs.7000), create SEPARATE rows for each.
- If the policy has multiple OPD types (General + Dental + Vision), create SEPARATE rows for each.
- entity_id and policy_number remain the SAME across all rows.

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "opd_description": null,
    "opd_limit_type": null,
    "opd_payment_type": null,
    "opd_SI_basis": null,
    "opd_value": null,
    "opd_dep_sublimit": null
  }
]

If NO OPD benefits are found in the document, return: []
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract OPD addon data - returns a LIST of rows
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
    opd_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(opd_rows)} OPD benefit rows")
    for i, row in enumerate(opd_rows):
        print(f"\n  Row {i+1}:")
        for k, v in row.items():
            print(f"    {k}: {v}")
else:
    # Try as single object
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        opd_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        opd_rows = []
        print("NO VALID JSON FOUND - assuming no OPD benefits")

# COMMAND ----------

# DBTITLE 1,Post-processing and PDF verification
import re

# Set entity_id and policy_number for all rows
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

# Get policy_number from first row or from PDF
if opd_rows and opd_rows[0].get("policy_number"):
    policy_number_extracted = opd_rows[0]["policy_number"]

# --- PDF KEYWORD VERIFICATION ---
# Check if OPD-related keywords exist in PDF at all
OPD_GENERAL_PATTERN = r'(?i)(OPD|out.?patient|out.?patient\s*department)'

# Specific OPD type verification patterns
OPD_TYPE_PATTERNS = {
    "General OPD": r'(?i)(general\s*OPD|OPD\s*benefit|OPD\s*cover|out.?patient\s*(benefit|cover|expense))',
    "Dental OPD": r'(?i)(dental\s*OPD|dental\s*(cover|benefit|treatment))',
    "Dental RCT Only": r'(?i)(dental\s*RCT|root\s*canal)',
    "Vision OPD": r'(?i)(vision\s*OPD|eye\s*OPD|spectacle|optical|lenses?\s*(cover|benefit)|vision\s*(cover|benefit))',
    "Accidental OPD": r'(?i)(accidental\s*OPD|accident.{0,10}OPD|OPD.{0,10}accident)',
    "Illness OPD": r'(?i)(illness\s*OPD|OPD.{0,10}illness)',
    "Psychiatric OPD": r'(?i)(psychiatr.{0,10}OPD|mental\s*health\s*OPD|counsell.{0,10}OPD|OPD.{0,10}psychiatr)',
    "Cancer OPD": r'(?i)(cancer\s*OPD|oncolog.{0,10}OPD)',
    "Covid OPD": r'(?i)(covid.{0,10}OPD|corona.{0,10}OPD)',
    "Autism OPD": r'(?i)(autism.{0,10}OPD|OPD.{0,10}autism)',
    "IVF OPD": r'(?i)(IVF.{0,10}OPD|infertil.{0,10}OPD|OPD.{0,10}IVF)',
    "Pre & Post-natal OPD": r'(?i)(pre.{0,5}post.{0,5}natal.{0,10}OPD|natal.{0,10}OPD|maternity.{0,10}OPD|OPD.{0,10}natal|OPD.{0,10}maternity|pre.{0,5}natal.{0,10}OPD)',
    "Well baby OPD": r'(?i)(well\s*baby.{0,10}OPD|vaccination.{0,10}OPD|immuniz.{0,10}OPD)',
    "Animal or Snake Bite OPD": r'(?i)(animal.{0,10}bite|snake\s*bite)',
    "Sleep apnea OPD": r'(?i)(sleep\s*apnea)',
}

# Allowed enums
ALLOWED_DESCRIPTIONS = list(OPD_TYPE_PATTERNS.keys())
ALLOWED_LIMIT_TYPES = ["Within SI", "Above SI", "Within Maternity"]
ALLOWED_PAYMENT_TYPES = ["Flat", "Percent of SI", "Percent of Maternity"]
ALLOWED_SI_BASIS = ["6L", "8L", "10L", "Self", "Floater"]

print("--- PDF OPD Keyword Check ---")
has_opd_in_pdf = bool(re.search(OPD_GENERAL_PATTERN, text))
print(f"  OPD mentioned in PDF: {has_opd_in_pdf}")

if not has_opd_in_pdf and opd_rows:
    print(f"  \u26a0\ufe0f LLM returned {len(opd_rows)} rows but NO OPD keyword in PDF \u2192 clearing all rows")
    opd_rows = []

# Validate and clean each row
validated_rows = []
print("\n--- Row Validation ---")
for i, row in enumerate(opd_rows):
    desc = row.get("opd_description")
    
    # Validate opd_description against allowed list
    if desc not in ALLOWED_DESCRIPTIONS:
        # Try fuzzy match
        matched = False
        for allowed in ALLOWED_DESCRIPTIONS:
            if desc and allowed.lower() in desc.lower():
                row["opd_description"] = allowed
                matched = True
                break
        if not matched:
            print(f"  Row {i+1}: Invalid opd_description '{desc}' \u2192 skipping")
            continue
    
    # Verify this specific OPD type exists in PDF
    desc = row["opd_description"]
    pattern = OPD_TYPE_PATTERNS.get(desc, "")
    if pattern and not re.search(pattern, text):
        # For General OPD, also accept if just "OPD" is mentioned with amounts
        if desc == "General OPD" and has_opd_in_pdf:
            pass  # Allow General OPD if OPD is mentioned at all
        else:
            print(f"  Row {i+1}: '{desc}' keyword NOT found in PDF \u2192 skipping")
            continue
    
    # Validate enums
    if row.get("opd_limit_type") and row["opd_limit_type"] not in ALLOWED_LIMIT_TYPES:
        row["opd_limit_type"] = None
    if row.get("opd_payment_type") and row["opd_payment_type"] not in ALLOWED_PAYMENT_TYPES:
        row["opd_payment_type"] = None
    if row.get("opd_SI_basis") and row["opd_SI_basis"] not in ALLOWED_SI_BASIS:
        row["opd_SI_basis"] = None
    
    # Set entity_id and policy_number
    row["entity_id"] = entity_id
    if policy_number_extracted:
        row["policy_number"] = policy_number_extracted
    
    # Payment type correction: if value > 100, it's Flat
    val = row.get("opd_value")
    if val is not None:
        try:
            if int(val) > 100:
                row["opd_payment_type"] = "Flat"
        except (ValueError, TypeError):
            pass
    
    validated_rows.append(row)
    print(f"  \u2705 Row {i+1}: {desc} - verified")

opd_rows = validated_rows
print(f"\n--- Final: {len(opd_rows)} validated OPD rows ---")
for i, row in enumerate(opd_rows):
    print(f"  Row {i+1}: {row}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("entity_id", IntegerType(), True),
    StructField("policy_number", StringType(), True),
    StructField("opd_description", StringType(), True),
    StructField("opd_limit_type", StringType(), True),
    StructField("opd_payment_type", StringType(), True),
    StructField("opd_SI_basis", StringType(), True),
    StructField("opd_value", DecimalType(8, 0), True),
    StructField("opd_dep_sublimit", DecimalType(8, 0), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in opd_rows:
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
        else:
            formatted[col_name] = val
    formatted_rows.append(formatted)

# Create DataFrame (may be empty if no OPD benefits found)
if formatted_rows:
    opd_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    opd_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No OPD benefits found in this PDF - empty DataFrame created")

display(opd_df)
print(f"\n\u2705 GMC OPD Addon extraction complete! ({opd_df.count()} rows)")
