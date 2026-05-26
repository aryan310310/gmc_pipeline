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

# DBTITLE 1,GMC Master Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor.

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

Rules:
- amount fields should contain numbers only (no commas, no Rs., no INR, NO DECIMALS - round to nearest integer)
- date fields should be in YYYY-MM-DD format
- if field missing return null
- return booleans as true/false only

IMPORTANT - FIELD EXTRACTION RULES:

1. entity_id: Always set to null (will be assigned in post-processing)

2. policy_number: Extract the policy number from the document.
   Look for: "Policy No.", "Policy Number", "Certificate No."

3. insurer_name: Extract the insurance company name and MAP it to the EXACT standardized name from this list:
   - "Acko General Insurance Limited"
   - "Aditya Birla Health Insurance Co. Ltd."
   - "Bajaj Allianz General Insurance Co. Ltd"
   - "Care Health Insurance Ltd"
   - "Cholamandalam MS General Insurance Co. Ltd"
   - "Future Generali India Insurance Co. Ltd."
   - "Go Digit General Insurance Ltd"
   - "HDFC ERGO General Insurance Co.Ltd."
   - "ICICI LOMBARD General Insurance Co. Ltd."
   - "IFFCO TOKIO General Insurance Co. Ltd."
   - "Liberty General Insurance Ltd."
   - "Magma HDI General Insurance Co. Ltd."
   - "Manipal Cigna Health Insurance Company Limited"
   - "National Insurance Co. Ltd."
   - "Niva Bupa Health Insurance Co Ltd."
   - "Raheja QBE General Insurance Co. Ltd."
   - "Reliance General Insurance Co.Ltd"
   - "Royal Sundaram General Insurance Co. Ltd."
   - "SBI General Insurance Co. Ltd."
   - "Star Health & Allied Insurance Co.Ltd."
   - "Tata AIG General Insurance Co. Ltd."
   - "The New India Assurance Co. Ltd"
   - "The Oriental Insurance Co. Ltd."
   - "United India Insurance Co. Ltd."
   - "Universal Sompo General Insurance Co. Ltd."
   - "Zuno General Insurance Limited"
   ALWAYS use the EXACT name from this list. Match based on the company mentioned in the PDF.

4. policy_type: Extract whether this is a "BASE" policy or "TOP-UP" policy.
   - If the document mentions "Top-Up", "Super Top-Up", "top up" → set to "TOP-UP"
   - Otherwise → set to "BASE"

5. cadre: Extract the cadre/grade/category if mentioned.
   Look for: "Grade", "Cadre", "Category", "Band", "Level"
   If not mentioned → null

6. business_unit: Extract the business unit if mentioned.
   If not mentioned → null

7. sub_division: Extract the sub-division if mentioned.
   If not mentioned → null

8. is_included_generic_benchmark: Always set to true.

9. policy_description: Extract the product/plan name.
   Look for: "Product Name", "Plan Name", "Scheme Name"
   Examples: "Group Activ Health", "Group Health Insurance", "Group Mediclaim"

10. policy_start_date: Extract the policy start date in YYYY-MM-DD format.
    Look for: "Start Date", "Commencement Date", "Effective From", "From"
    Convert DD/MM/YYYY or other formats to YYYY-MM-DD.

11. policy_end_date: Extract the policy end date in YYYY-MM-DD format.
    Look for: "End Date", "Expiry Date", "Valid Till", "To"
    Convert DD/MM/YYYY or other formats to YYYY-MM-DD.

12. TPA_name: Extract the TPA (Third Party Administrator) name and MAP it to the EXACT standardized name from this list:
   - "Aditya Birla Health Insurance Co. Ltd."
   - "Care Health Insurance Ltd"
   - "Cholamandalam MS General Insurance Co Ltd"
   - "Ericson Insurance TPA Pvt Ltd"
   - "Family Health Plan Insurance TPA Ltd."
   - "Go Digit General Insurance Ltd"
   - "Hdfc Ergo General Insurance TPA"
   - "Health India TPA Ltd."
   - "Heritage Health Services TPA Pvt. Ltd."
   - "ICICI Lombard Healthcare"
   - "In-house"
   - "MDIndia Health Insurance TPA Pvt. Ltd."
   - "Medi Assist Insurance TPA Pvt. Ltd."
   - "Niva Bupa Insurance Ltd"
   - "Paramount Health Insurance TPA"
   - "Park Mediclaim TPA"
   - "Reliance General Insurance Ltd"
   - "SBI General Insurance Co Ltd"
   - "Safeway TPA Ltd."
   - "Star Health & Allied Insurance CoLtd"
   - "TATA AIG General Insurance Company Ltd"
   - "Universal Sompo Health Serve"
   - "Vidal Health Insurance TPA Pvt Ltd."
   - "Volo Health TPA"
   ALWAYS use the EXACT name from this list. Match based on the TPA mentioned in the PDF.
   If not mentioned → null
   If the insurer handles claims in-house (no TPA) → "In-house"

13. premium_without_gst: Extract the net premium (without GST/tax).
    Look for: "Net Premium", "Base Premium", "Premium (excl. GST)"
    Extract as INTEGER only (round to nearest whole number, no decimals).

14. premium_with_gst: Extract the gross/total premium (with GST/tax included).
    Look for: "Gross Premium", "Total Premium", "Premium (incl. GST)"
    Extract as INTEGER only (round to nearest whole number, no decimals).

15. is_floter_policy: Determine if this is a floater policy.
    - If "Floater" or "Family Floater" is mentioned → true
    - If "Individual" basis is mentioned → false
    - Default → true (most GMC policies are floater)

Extract ALL fields exactly as below:

{
  "entity_id": null,
  "policy_number": null,
  "insurer_name": null,
  "policy_type": null,
  "cadre": null,
  "business_unit": null,
  "sub_division": null,
  "is_included_generic_benchmark": true,
  "policy_description": null,
  "policy_start_date": null,
  "policy_end_date": null,
  "TPA_name": null,
  "premium_without_gst": null,
  "premium_with_gst": null,
  "is_floter_policy": null
}
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
# Extract GMC master data using the shared LLM function
data = extract_with_llm(prompt, text)

if data:
    print(f"Extracted {len(data)} fields")
    for k, v in data.items():
        print(f"  {k}: {v}")
else:
    raise ValueError("LLM extraction failed - no valid JSON returned")

# COMMAND ----------

# DBTITLE 1,Set entity ID and post-processing
import decimal
from datetime import datetime, date

# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
data["entity_id"] = entity_id

# --- Post-processing rules ---

# 1. is_included_generic_benchmark: always true
data["is_included_generic_benchmark"] = True

# 2. policy_type: default to BASE if not extracted
if not data.get("policy_type"):
    data["policy_type"] = "BASE"
else:
    pt = data["policy_type"].upper().strip()
    if "TOP" in pt:
        data["policy_type"] = "TOP-UP"
    else:
        data["policy_type"] = "BASE"

# 3. is_floter_policy: default to true for GMC
if data.get("is_floter_policy") is None:
    data["is_floter_policy"] = True

# 4. Parse dates from various formats to date objects
def parse_date(val):
    if val is None:
        return None
    if isinstance(val, date):
        return val
    val = str(val).strip()
    formats = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%m/%d/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    # Try to extract date from text like "From 00:00 Hrs of 17/10/2025"
    import re
    m = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', val)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if month > 12:  # swap if month > 12 (likely DD/MM/YYYY)
            day, month = month, day
        try:
            return date(year, month, day)
        except ValueError:
            pass
    return None

data["policy_start_date"] = parse_date(data.get("policy_start_date"))
data["policy_end_date"] = parse_date(data.get("policy_end_date"))

# 5. Premium: ensure integer (no decimals)
def parse_premium(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(round(val))
    val = str(val).replace(",", "").replace("Rs.", "").replace("INR", "").replace("/-", "").strip()
    try:
        return int(round(float(val)))
    except:
        return None

data["premium_without_gst"] = parse_premium(data.get("premium_without_gst"))
data["premium_with_gst"] = parse_premium(data.get("premium_with_gst"))

# 6. Validate insurer_name against known list
KNOWN_INSURERS = [
    "Acko General Insurance Limited",
    "Aditya Birla Health Insurance Co. Ltd.",
    "Bajaj Allianz General Insurance Co. Ltd",
    "Care Health Insurance Ltd",
    "Cholamandalam MS General Insurance Co. Ltd",
    "Future Generali India Insurance Co. Ltd.",
    "Go Digit General Insurance Ltd",
    "HDFC ERGO General Insurance Co.Ltd.",
    "ICICI LOMBARD General Insurance Co. Ltd.",
    "IFFCO TOKIO General Insurance Co. Ltd.",
    "Liberty General Insurance Ltd.",
    "Magma HDI General Insurance Co. Ltd.",
    "Manipal Cigna Health Insurance Company Limited",
    "National Insurance Co. Ltd.",
    "Niva Bupa Health Insurance Co Ltd.",
    "Raheja QBE General Insurance Co. Ltd.",
    "Reliance General Insurance Co.Ltd",
    "Royal Sundaram General Insurance Co. Ltd.",
    "SBI General Insurance Co. Ltd.",
    "Star Health & Allied Insurance Co.Ltd.",
    "Tata AIG General Insurance Co. Ltd.",
    "The New India Assurance Co. Ltd",
    "The Oriental Insurance Co. Ltd.",
    "United India Insurance Co. Ltd.",
    "Universal Sompo General Insurance Co. Ltd.",
    "Zuno General Insurance Limited"
]

KNOWN_TPAS = [
    "Aditya Birla Health Insurance Co. Ltd.",
    "Care Health Insurance Ltd",
    "Cholamandalam MS General Insurance Co Ltd",
    "Ericson Insurance TPA Pvt Ltd",
    "Family Health Plan Insurance TPA Ltd.",
    "Go Digit General Insurance Ltd",
    "Hdfc Ergo General Insurance TPA",
    "Health India TPA Ltd.",
    "Heritage Health Services TPA Pvt. Ltd.",
    "ICICI Lombard Healthcare",
    "In-house",
    "MDIndia Health Insurance TPA Pvt. Ltd.",
    "Medi Assist Insurance TPA Pvt. Ltd.",
    "Niva Bupa Insurance Ltd",
    "Paramount Health Insurance TPA",
    "Park Mediclaim TPA",
    "Reliance General Insurance Ltd",
    "SBI General Insurance Co Ltd",
    "Safeway TPA Ltd.",
    "Star Health & Allied Insurance CoLtd",
    "TATA AIG General Insurance Company Ltd",
    "Universal Sompo Health Serve",
    "Vidal Health Insurance TPA Pvt Ltd.",
    "Volo Health TPA"
]

def match_name(extracted, known_list):
    """Fuzzy match extracted name to known standardized name."""
    if not extracted:
        return None
    extracted_lower = extracted.lower().strip()
    # Exact match first
    for name in known_list:
        if name.lower() == extracted_lower:
            return name
    # Partial match
    for name in known_list:
        if name.lower() in extracted_lower or extracted_lower in name.lower():
            return name
    # Keyword match
    for name in known_list:
        keywords = [w for w in name.lower().split() if len(w) > 3]
        matches = sum(1 for kw in keywords if kw in extracted_lower)
        if matches >= 2:
            return name
    # Return as-is if no match (LLM should have used exact name)
    return extracted

data["insurer_name"] = match_name(data.get("insurer_name"), KNOWN_INSURERS)
data["TPA_name"] = match_name(data.get("TPA_name"), KNOWN_TPAS)

print("Post-processing complete:")
for k, v in data.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
import json as _json_exit

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("insurer_name", StringType(), False),
    StructField("policy_type", StringType(), True),
    StructField("cadre", StringType(), True),
    StructField("business_unit", StringType(), True),
    StructField("sub_division", StringType(), True),
    StructField("is_included_generic_benchmark", BooleanType(), True),
    StructField("policy_description", StringType(), False),
    StructField("policy_start_date", DateType(), False),
    StructField("policy_end_date", DateType(), True),
    StructField("TPA_name", StringType(), False),
    StructField("premium_without_gst", IntegerType(), False),
    StructField("premium_with_gst", IntegerType(), False),
    StructField("is_floter_policy", BooleanType(), False)
])

# Filter data to only schema columns
schema_cols = [f.name for f in schema.fields]
filtered_data = {k: data.get(k) for k in schema_cols}

# Create DataFrame
master_df = spark.createDataFrame([filtered_data], schema=schema)
display(master_df)

_row_count = master_df.count()
print(f"\n\u2705 GMC Master extraction complete! ({_row_count} rows)")
