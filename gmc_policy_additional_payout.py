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

# DBTITLE 1,GMC Policy Additional Payout Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in additional payout benefits.

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
- If a field is NOT mentioned at all in the document → set to null.
- For boolean fields: true ONLY if explicitly mentioned as covered/included, false ONLY if explicitly excluded, null if not mentioned.

Rules:
- amount/value fields should contain numbers only (no commas, no Rs., no INR, NO DECIMALS)
- days fields should contain integer numbers only
- limit_type STRICT ENUM: ["Within SI", "Additional", "Within Maternity"] - ONLY these values allowed
- payment_type STRICT ENUM: ["Percent of SI", "Flat"] - ONLY these values allowed
- if field missing return null
- return booleans as true/false only

IMPORTANT - FIELD EXTRACTION RULES:

1. entity_id: Always set to null (will be assigned in post-processing)

2. policy_number: Extract the policy number from the document.

3. hospital_cash_covered: Is hospital daily cash benefit covered?
   Look for: "hospital cash", "daily cash", "daily allowance", "hospital daily benefit"

4. hospital_cash_amount: The daily cash amount (per day).
   Look for: "Rs. X per day", "daily allowance of Rs."
   Extract as INTEGER only.

5. hospital_cash_days: Maximum number of days for hospital cash.
   Look for: "maximum X days", "upto X days", "per hospitalization"

6. nursing_allowance_covered: Is nursing/convalescence allowance covered?
   Look for: "nursing allowance", "convalescence", "recovery benefit"

7. nursing_allowance_amount: The daily nursing allowance amount.
   Extract as INTEGER only.

8. nursing_allowance_days: Maximum number of days for nursing allowance.

9. nursing_allowance_deductible_days: Number of deductible/waiting days before nursing kicks in.
   Look for: "after X days", "deductible of X days", "waiting period of X days"

10. family_transportation_benefit_covered: Is family transportation benefit covered?
    Look for: "transportation benefit", "travel allowance", "family transportation"

11. family_transportation_benefit_amount: The transportation benefit amount.
    Extract as INTEGER only.

12. attendant_charges_covered: Are attendant/companion charges covered?
    Look for: "attendant charges", "companion charges", "attendant allowance"

13. attendant_charges_limit_type: The limit type for attendant charges.
    STRICT ENUM: ["Within SI", "Additional"]

14. attendat_charges_payment_type: The payment type for attendant charges.
    STRICT ENUM: ["Percent of SI", "Flat"]

15. attendant_charges_amount: The amount for attendant charges.
    Extract as INTEGER only.

Extract ALL fields exactly as below:

{
  "entity_id": null,
  "policy_number": null,
  "hospital_cash_covered": null,
  "hospital_cash_amount": null,
  "hospital_cash_days": null,
  "nursing_allowance_covered": null,
  "nursing_allowance_amount": null,
  "nursing_allowance_days": null,
  "nursing_allowance_deductible_days": null,
  "family_transportation_benefit_covered": null,
  "family_transportation_benefit_amount": null,
  "attendant_charges_covered": null,
  "attendant_charges_limit_type": null,
  "attendat_charges_payment_type": null,
  "attendant_charges_amount": null
}
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
# Extract GMC policy additional payout data using the shared LLM function
data = extract_with_llm(prompt, text)

if data:
    print(f"Extracted {len(data)} fields")
    for k, v in data.items():
        print(f"  {k}: {v}")
else:
    raise ValueError("LLM extraction failed - no valid JSON returned")

# COMMAND ----------

# DBTITLE 1,Set entity ID and post-processing
import re

# Set entity_id for this policy (change per run)
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
data["entity_id"] = entity_id

# --- Post-processing rules ---

# Boolean columns list
boolean_columns = [
    "hospital_cash_covered", "nursing_allowance_covered",
    "family_transportation_benefit_covered", "attendant_charges_covered"
]

# Apply boolean normalization
data = apply_boolean_normalization(data, boolean_columns)

# --- PDF KEYWORD VERIFICATION ---
# If keyword not found in PDF text, set boolean to FALSE

KEYWORD_VERIFICATION = {
    "hospital_cash_covered": r'(?i)(hospital\s*cash|daily\s*cash|daily\s*allowance|hospital\s*daily\s*benefit|daily\s*benefit)',
    "nursing_allowance_covered": r'(?i)(nursing\s*allowance|convalescence|recovery\s*benefit|nursing\s*charge)',
    "family_transportation_benefit_covered": r'(?i)(transportation\s*benefit|travel\s*allowance|family\s*transportation|conveyance)',
    "attendant_charges_covered": r'(?i)(attendant\s*charge|companion\s*charge|attendant\s*allowance|attendant\s*fee)',
}

print("\n--- PDF Keyword Verification ---")
for field, pattern in KEYWORD_VERIFICATION.items():
    if data.get(field) is True:
        if not re.search(pattern, text):
            print(f"  \u26a0\ufe0f {field}: LLM said True but keyword NOT found in PDF \u2192 setting to False")
            data[field] = False
        else:
            print(f"  \u2705 {field}: Verified - keyword found in PDF")
    elif data.get(field) is None or data.get(field) is False:
        # Check if keyword IS in the PDF but LLM missed it -> set to True
        if re.search(pattern, text):
            print(f"  \u2705 {field}: LLM missed it but keyword FOUND in PDF \u2192 setting to True")
            data[field] = True
        else:
            data[field] = False

# --- BENEFIT GROUP NULLIFICATION ---
# If covered=false, nullify sub-fields

# hospital_cash: if not covered, null amount and days
if data.get("hospital_cash_covered") is not True:
    data["hospital_cash_amount"] = None
    data["hospital_cash_days"] = None

# nursing_allowance: if not covered, null amount, days, deductible
if data.get("nursing_allowance_covered") is not True:
    data["nursing_allowance_amount"] = None
    data["nursing_allowance_days"] = None
    data["nursing_allowance_deductible_days"] = None

# family_transportation: if not covered, null amount
if data.get("family_transportation_benefit_covered") is not True:
    data["family_transportation_benefit_amount"] = None

# attendant_charges: if not covered, null limit_type, payment_type, amount
if data.get("attendant_charges_covered") is not True:
    data["attendant_charges_limit_type"] = None
    data["attendat_charges_payment_type"] = None
    data["attendant_charges_amount"] = None
else:
    # Validate enums
    ALLOWED_LIMIT_TYPES = ["Within SI", "Additional", "Within Maternity"]
    ALLOWED_PAYMENT_TYPES = ["Percent of SI", "Flat"]
    if data.get("attendant_charges_limit_type") and data["attendant_charges_limit_type"] not in ALLOWED_LIMIT_TYPES:
        data["attendant_charges_limit_type"] = None
    if data.get("attendat_charges_payment_type") and data["attendat_charges_payment_type"] not in ALLOWED_PAYMENT_TYPES:
        data["attendat_charges_payment_type"] = None
    # Defaults when covered=true but details missing
    if not data.get("attendant_charges_limit_type"):
        data["attendant_charges_limit_type"] = "Within SI"
    if not data.get("attendat_charges_payment_type"):
        data["attendat_charges_payment_type"] = "Percent of SI"
    if not data.get("attendant_charges_amount"):
        data["attendant_charges_amount"] = 100
    # Payment type correction: if amount > 100, it's Flat
    try:
        if int(data.get("attendant_charges_amount", 0)) > 100:
            data["attendat_charges_payment_type"] = "Flat"
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
    StructField("hospital_cash_covered", BooleanType(), True),
    StructField("hospital_cash_amount", DecimalType(8, 0), True),
    StructField("hospital_cash_days", IntegerType(), True),
    StructField("nursing_allowance_covered", BooleanType(), True),
    StructField("nursing_allowance_amount", DecimalType(8, 0), True),
    StructField("nursing_allowance_days", IntegerType(), True),
    StructField("nursing_allowance_deductible_days", IntegerType(), True),
    StructField("family_transportation_benefit_covered", BooleanType(), True),
    StructField("family_transportation_benefit_amount", DecimalType(8, 0), True),
    StructField("attendant_charges_covered", BooleanType(), True),
    StructField("attendant_charges_limit_type", StringType(), True),
    StructField("attendat_charges_payment_type", StringType(), True),
    StructField("attendant_charges_amount", DecimalType(8, 0), True)
])

# Filter data to only schema columns and convert types
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
    elif isinstance(field.dataType, IntegerType) and col_name != "entity_id" and val is not None:
        try:
            filtered_data[col_name] = int(val)
        except (ValueError, TypeError):
            filtered_data[col_name] = None
    else:
        filtered_data[col_name] = val

# Create DataFrame
additional_payout_df = spark.createDataFrame([filtered_data], schema=schema)
display(additional_payout_df)

print("\n\u2705 GMC Policy Additional Payout extraction complete!")
