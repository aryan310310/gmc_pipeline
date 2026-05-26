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

# DBTITLE 1,GMC Modern Treatment Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in modern treatment coverage.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any value not explicitly written in the document
- HALLUCINATE values that are not clearly stated
- SET max_limit unless EXPLICITLY stated in the document as a cap amount

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY what is EXPLICITLY written in the document.
- A single policy has MULTIPLE rows - one per modern treatment covered.
- Do NOT assume max_limit for any treatment unless the document EXPLICITLY states a cap.

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_number: Extract the policy number.

3. treatment: STRING. The modern treatment name.
   STANDARD 9 DEFAULT TREATMENTS (use EXACT names):
   - "Oral chemotherapy"
   - "Uterine Artery Embolization & High-Intensity Focused Ultrasound (HIFU)"
   - "Balloon Sinuplasty"
   - "Deep Brain Stimulation"
   - "Immunotherapy"
   - "Intravitreous (Intra-vitreal) Injections"
   - "Bronchial Thermoplasty"
   - "Cochlear implant"
   - "Vaporisation of the Prostrate"

   ADDITIONAL treatments (include ONLY if EXPLICITLY mentioned by name in document WITH their values):
   - "Robotic Surgeries"
   - "Stereotactic Radiosurgeries"
   - "Stem Cell Transplantation"
   - "Intra-Operative Neuro Monitoring (IONM)"
   - "Cyber knife treatment"
   - "Gamma Knife Treatment"
   - "Lucentis Injection"
   - "Avastin injection"
   - "Bariatric surgery"
   - "Peritoneal dialysis treatment"

   If any of these ADDITIONAL treatments are explicitly called out in the document with their
   coverage details (percentage, flat amount, copay), include them as separate rows.

4. is_graded: BOOLEAN or null.
   - true ONLY if treatment limits vary by employee grade.
   - null if not graded.

5. grade_classification: STRING.
   - Grade name if is_graded=true.
   - null if is_graded is null.

6. is_SI_basis: BOOLEAN or null.
   - true ONLY if treatment limits vary by Sum Insured slab.
   - null if not SI-based.

7. SI_basis_description: STRING.
   - SI slab in lakh notation ("3L", "5L") if is_SI_basis=true.
   - null if is_SI_basis is null.

8. payment_type: STRING. ENUM values:
   - "Percent of SI" - Treatment covered as percentage of Sum Insured
   - "Co-Pay" - Treatment has a co-pay (patient pays a percentage)
   - "Flat" - Treatment has a fixed amount limit

9. treatment_value: INTEGER or null.
   - For "Percent of SI": the percentage (e.g., 50 means 50% of SI)
   - For "Flat": the flat amount (e.g., 50000, 100000)
   - For "Co-Pay": null (the copay percentage goes in copay field)
   - Common values for Percent of SI: 25, 30, 50, 75, 100

10. max_limit: INTEGER or null.
    - Maximum cap amount ONLY if EXPLICITLY mentioned in the document.
    - DO NOT assume or infer any max_limit.
    - null if no cap explicitly mentioned.

11. copay: INTEGER or null.
    - Co-pay percentage for the treatment ONLY if explicitly mentioned.
    - null if copay is NOT mentioned at all.
    - Do NOT default to 0. Leave null if not stated.

CRITICAL RULES:

1. If document says "Modern treatment covered" generically (without listing individual treatments):
   - Return ALL 9 standard default treatments.
   - Default: payment_type="Percent of SI", treatment_value=50, max_limit=null, copay=null

2. If treatments are INDIVIDUALLY called out with specific limits:
   - Return ONLY those specifically mentioned treatments.
   - Use their explicit payment_type/treatment_value.
   - Include ADDITIONAL treatments (Cyber knife, Gamma Knife, Lucentis, Avastin, Bariatric)
     if they are also explicitly called out with values.

3. If a specific percentage is mentioned (e.g., "50% of SI", "covered up to 50%"):
   - Apply that percentage to treatment_value.

4. If Co-Pay is mentioned for modern treatments:
   - payment_type = "Co-Pay", treatment_value = null, copay = the copay percentage

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "treatment": "Oral chemotherapy",
    "is_graded": null,
    "grade_classification": null,
    "is_SI_basis": null,
    "SI_basis_description": null,
    "payment_type": "Percent of SI",
    "treatment_value": 50,
    "max_limit": null,
    "copay": null
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract modern treatment data - returns a LIST of rows
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
    mt_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(mt_rows)} modern treatment rows")
    for i, row in enumerate(mt_rows):
        print(f"  Row {i+1}: {row.get('treatment')} | type={row.get('payment_type')} | value={row.get('treatment_value')} | max={row.get('max_limit')} | copay={row.get('copay')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        mt_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        mt_rows = []
        print("NO VALID JSON FOUND")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re
from collections import defaultdict, Counter

# Set entity_id and policy_number
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

if mt_rows and mt_rows[0].get("policy_number"):
    policy_number_extracted = mt_rows[0]["policy_number"]

print("--- Modern Treatment Post-processing ---")

# --- STANDARD 9 DEFAULT TREATMENTS ---
STANDARD_9_TREATMENTS = [
    "Oral chemotherapy",
    "Uterine Artery Embolization & High-Intensity Focused Ultrasound (HIFU)",
    "Balloon Sinuplasty",
    "Deep Brain Stimulation",
    "Immunotherapy",
    "Intravitreous (Intra-vitreal) Injections",
    "Bronchial Thermoplasty",
    "Cochlear implant",
    "Vaporisation of the Prostrate"
]

# ALL recognized treatments (standard 9 + additional)
ALL_RECOGNIZED_TREATMENTS = STANDARD_9_TREATMENTS + [
    "Robotic Surgeries",
    "Stereotactic Radiosurgeries",
    "Stem Cell Transplantation",
    "Intra-Operative Neuro Monitoring (IONM)",
    "Cyber knife treatment",
    "Gamma Knife Treatment",
    "Lucentis Injection",
    "Avastin injection",
    "Bariatric surgery",
    "Peritoneal dialysis treatment"
]

# Treatment name normalization map
TREATMENT_NORMALIZE = {
    "uterine artery embolization": "Uterine Artery Embolization & High-Intensity Focused Ultrasound (HIFU)",
    "hifu": "Uterine Artery Embolization & High-Intensity Focused Ultrasound (HIFU)",
    "high intensity focused ultrasound": "Uterine Artery Embolization & High-Intensity Focused Ultrasound (HIFU)",
    "balloon sinuplasty": "Balloon Sinuplasty",
    "deep brain stimulation": "Deep Brain Stimulation",
    "oral chemotherapy": "Oral chemotherapy",
    "immunotherapy": "Immunotherapy",
    "monoclonal antibod": "Immunotherapy",
    "intravitreous": "Intravitreous (Intra-vitreal) Injections",
    "intra-vitreal": "Intravitreous (Intra-vitreal) Injections",
    "intravitreal": "Intravitreous (Intra-vitreal) Injections",
    "robotic": "Robotic Surgeries",
    "robotic surgeries": "Robotic Surgeries",
    "robotic surgery": "Robotic Surgeries",
    "stereotactic": "Stereotactic Radiosurgeries",
    "stereotactic radiosurgeries": "Stereotactic Radiosurgeries",
    "stereotactic radio surgeries": "Stereotactic Radiosurgeries",
    "bronchial thermoplasty": "Bronchial Thermoplasty",
    "vaporisation of the prostrate": "Vaporisation of the Prostrate",
    "vaporisation of prostate": "Vaporisation of the Prostrate",
    "vapourisation": "Vaporisation of the Prostrate",
    "green laser": "Vaporisation of the Prostrate",
    "holmium laser": "Vaporisation of the Prostrate",
    "stem cell transplantation": "Stem Cell Transplantation",
    "stem cell therapy": "Stem Cell Transplantation",
    "stem cell transplant": "Stem Cell Transplantation",
    "stem cell": "Stem Cell Transplantation",
    "intra-operative neuro monitoring": "Intra-Operative Neuro Monitoring (IONM)",
    "ionm": "Intra-Operative Neuro Monitoring (IONM)",
    "neuro monitoring": "Intra-Operative Neuro Monitoring (IONM)",
    "cochlear implant": "Cochlear implant",
    "cochlear": "Cochlear implant",
    "cyber knife": "Cyber knife treatment",
    "cyberknife": "Cyber knife treatment",
    "gamma knife": "Gamma Knife Treatment",
    "lucentis": "Lucentis Injection",
    "avastin": "Avastin injection",
    "bariatric": "Bariatric surgery",
    "peritoneal dialysis": "Peritoneal dialysis treatment",
}

# --- STEP 1: Normalize treatment names and validate ---
validated_rows = []
for row in mt_rows:
    row["entity_id"] = entity_id
    row["policy_number"] = policy_number_extracted
    
    # Normalize treatment name
    treatment_raw = str(row.get("treatment", "")).strip()
    treatment_lower = treatment_raw.lower()
    
    # Try exact match first
    matched = None
    if treatment_raw in ALL_RECOGNIZED_TREATMENTS:
        matched = treatment_raw
    else:
        # Try normalization map
        for key, val in TREATMENT_NORMALIZE.items():
            if key in treatment_lower:
                matched = val
                break
    
    if matched:
        row["treatment"] = matched
    else:
        row["treatment"] = treatment_raw  # keep as-is for any other treatments
    
    # Normalize payment_type
    pt = row.get("payment_type")
    if pt:
        pt_lower = str(pt).strip().lower()
        if pt_lower in ["percent of si", "percentage of si", "% of si", "percent"]:
            row["payment_type"] = "Percent of SI"
        elif pt_lower in ["co-pay", "copay", "co pay"]:
            row["payment_type"] = "Co-Pay"
        elif pt_lower in ["flat", "fixed"]:
            row["payment_type"] = "Flat"
        else:
            row["payment_type"] = "Percent of SI"  # default
    else:
        row["payment_type"] = "Percent of SI"
    
    # treatment_value: validate and apply payment type correction
    tv = row.get("treatment_value")
    if tv is not None:
        try:
            tv_num = int(float(str(tv).replace(",", "").replace("Rs.", "").replace("INR", "").strip()))
            # If Percent of SI but value > 100, switch to Flat
            if row["payment_type"] == "Percent of SI" and tv_num > 100:
                row["payment_type"] = "Flat"
            row["treatment_value"] = tv_num
        except (ValueError, TypeError):
            row["treatment_value"] = None
    else:
        row["treatment_value"] = None
    
    # For Co-Pay type: treatment_value should be null, copay has the percentage
    if row["payment_type"] == "Co-Pay":
        row["treatment_value"] = None
    
    # max_limit: validate - ONLY keep if explicitly a number, never assume
    ml = row.get("max_limit")
    if ml is not None:
        try:
            ml_val = int(float(str(ml).replace(",", "")))
            row["max_limit"] = ml_val
        except (ValueError, TypeError):
            row["max_limit"] = None
    else:
        row["max_limit"] = None
    
    # copay: LEAVE NULL if not mentioned. Do NOT default to 0.
    cp = row.get("copay")
    if cp is not None:
        try:
            cp_val = int(float(str(cp).replace("%", "").strip()))
            if cp_val == 0:
                row["copay"] = None  # 0 means not mentioned, leave null
            else:
                row["copay"] = cp_val
        except (ValueError, TypeError):
            row["copay"] = None
    else:
        row["copay"] = None
    
    # is_graded / is_SI_basis: null if not applicable
    if not row.get("is_graded"):
        row["is_graded"] = None
        row["grade_classification"] = None
    if not row.get("is_SI_basis"):
        row["is_SI_basis"] = None
        row["SI_basis_description"] = None
    
    validated_rows.append(row)

# --- STEP 2: Decide whether to append default 9 or keep only individually called-out ---
# RULE:
#   - If LLM extracted INDIVIDUAL treatments (called out by name in PDF) -> keep ONLY those
#     (includes additional treatments like Cyber knife, Gamma Knife, Lucentis, Avastin, Bariatric)
#   - If LLM extracted nothing BUT PDF generically says "modern treatment covered" -> append all 9 defaults

existing_treatments = set(r["treatment"] for r in validated_rows)

if len(validated_rows) == 0:
    # LLM found nothing - check if PDF mentions modern treatment generically
    MODERN_GENERIC_PATTERN = r'(?i)(modern\s*treat\w*\s*(is\s*)?\s*(cover|includ|applic)|advance\s*treat\w*\s*(cover|includ))'
    if re.search(MODERN_GENERIC_PATTERN, text):
        print("  \U0001f4a1 Generic 'modern treatment covered' detected - appending all 9 defaults")
        for treatment in STANDARD_9_TREATMENTS:
            validated_rows.append({
                "entity_id": entity_id,
                "policy_number": policy_number_extracted,
                "treatment": treatment,
                "is_graded": None,
                "grade_classification": None,
                "is_SI_basis": None,
                "SI_basis_description": None,
                "payment_type": "Percent of SI",
                "treatment_value": 50,
                "max_limit": None,
                "copay": None
            })
            print(f"  \u2795 Appended: {treatment} | Percent of SI | value=50")
    else:
        print("  \u26a0\ufe0f No modern treatment found in PDF at all")

else:
    # LLM called out specific treatments individually -> keep ONLY what LLM extracted
    # This includes any additional treatments (Cyber knife, Gamma Knife, Lucentis, Avastin, Bariatric)
    # that were explicitly mentioned with their values
    print(f"  \U0001f4a1 {len(validated_rows)} treatments individually called out in PDF - keeping as-is")
    print(f"     (includes any additional treatments explicitly mentioned with values)")

# --- STEP 3: Deduplicate by treatment (keep first occurrence per treatment+grade+SI) ---
seen = set()
final_rows = []
for row in validated_rows:
    key = (row["treatment"], row.get("grade_classification"), row.get("SI_basis_description"))
    if key not in seen:
        seen.add(key)
        final_rows.append(row)

mt_rows = final_rows

print(f"\n--- Final: {len(mt_rows)} modern treatment rows ---")
for i, row in enumerate(mt_rows):
    print(f"  \u2705 Row {i+1}: {row['treatment']} | {row['payment_type']} | value={row.get('treatment_value')} | max={row.get('max_limit')} | copay={row.get('copay')}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("treatment", StringType(), False),
    StructField("is_graded", BooleanType(), True),
    StructField("grade_classification", StringType(), True),
    StructField("is_SI_basis", BooleanType(), True),
    StructField("SI_basis_description", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("treatment_value", DecimalType(8, 0), True),
    StructField("max_limit", DecimalType(8, 0), True),
    StructField("copay", DecimalType(8, 0), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in mt_rows:
    formatted = {}
    for col_name in schema_cols:
        val = row.get(col_name)
        field = schema[col_name]
        if isinstance(field.dataType, DecimalType) and val is not None:
            try:
                formatted[col_name] = Decimal(str(int(float(str(val).replace(",", "")))))
            except (ValueError, TypeError, ArithmeticError):
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
    modern_treatment_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    modern_treatment_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No modern treatment data found - empty DataFrame created")

display(modern_treatment_df)
print(f"\n\u2705 GMC Modern Treatment extraction complete! ({modern_treatment_df.count()} rows)")
