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

# DBTITLE 1,GMC Ailment Capping Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in ailment-wise capping/sub-limits.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any value not explicitly written in the document
- HALLUCINATE values that are not clearly stated
- SET copay unless EXPLICITLY stated against a specific ailment

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY what is EXPLICITLY written in the document.
- A single policy has MULTIPLE rows - one per ailment that has a capping/sub-limit.
- Only extract ailments that have an EXPLICIT cap/sub-limit or special coverage mentioned.
- If no ailment capping is mentioned at all, return an empty array [].

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_number: Extract the policy number.

3. ailment_name: STRING. The specific ailment/procedure name.
   Common ailments found in policies (use EXACT standard names):
   - "Cataract"
   - "ARMD" (Age-Related Macular Degeneration)
   - "Femto laser"
   - "Toric Lens" / "Multi focal Lens"
   - "Bio absorbable stent"
   - "CAPD" (Continuous Ambulatory Peritoneal Dialysis)
   - "Bariatric surgery"
   - "Functional Endoscopic Sinus Surgery"
   - "Nasal Sinus surgery"
   - "Hysterectomy"
   - "Hernia"
   - "Joint replacement" / "Knee Replacement" / "Total Replacement of Joints"
   - "Kidney stone" / "Kidney Stone" / "Urinary stone"
   - "Hydrocele"
   - "Appendix" / "Appendectomy" / "Appendicitis"
   - "Gall Bladder stone" / "Cholecystectomy"
   - "Piles" / "Haemorrhoids" / "Fissure"
   - "Cancer" / "Malignancy"
   - "Heart Attack" / "CABG" / "PTCA" / "Angioplasty" / "Open and Close Heart Surgery"
   - "Mental Illness" / "Mental illness"
   - "Neuro disorders" / "Multiple Sclerosis"
   - "Cerebro Vascular Strokes" / "Stroke" / "Paralysis"
   - "Liver Disorder"
   - "Genetic disorders"
   - "Sleep apnea"
   - "Encephalitis"
   - "Pacemaker implantation"
   - "Bone marrow transplant"
   - "KT Laser Prostate"
   - "Cosmetic surgery"
   - "Dental flap"
   - "Sterility"
   - "Obesity"
   - "Skin tumor"
   - "Grievous injury"
   - "Hazardous sports"
   - "Puberty and menopause disorder"
   - "Tonsillectomy"
   - "Sinusitis"

4. ailment_classification: STRING or null.
   - "Special coverage" - if the ailment is listed as a special/additional coverage
   - null - if it's a standard sub-limit/capping

5. is_graded: BOOLEAN or null.
   - true ONLY if ailment cap varies by employee grade.
   - null if not graded.

6. grade_classification: STRING.
   - Grade name if is_graded=true.
   - null if is_graded is null.

7. is_SI_basis: BOOLEAN or null.
   - true ONLY if ailment cap varies by Sum Insured slab.
   - null if not SI-based.

8. SI_basis_description: STRING.
   - SI slab in lakh notation ("3L", "5L") if is_SI_basis=true.
   - null if is_SI_basis is null.

9. payment_type: STRING or null. ENUM values:
   - "Percent of SI" - Cap is a percentage of Sum Insured (values typically 10-100)
   - "Flat" - Cap is a fixed amount (values like 20000, 50000, 100000)
   - null - if only copay is mentioned without a cap amount

10. cap_value: INTEGER or null.
    - For "Percent of SI": the percentage number (e.g., 50 = 50% of SI)
    - For "Flat": the flat cap amount (e.g., 50000)
    - null if no cap amount mentioned (only copay mentioned)
    - If value > 100 -> payment_type must be "Flat"
    - If value <= 100 -> payment_type is likely "Percent of SI"

11. copay: INTEGER or null.
    - Co-pay percentage ONLY if explicitly stated against this specific ailment.
    - null if copay is NOT mentioned for this ailment.
    - Do NOT assume or default to any value.
    - Common value: 50 (meaning 50% copay)

12. max_limit: INTEGER or null.
    - Absolute maximum cap amount if mentioned separately from cap_value.
    - null if no separate max limit mentioned.

CRITICAL RULES:

1. ONLY include ailments that are EXPLICITLY listed with a sub-limit, cap, or special coverage.
2. Do NOT create rows for ailments that are just mentioned as covered without a specific limit.
3. copay: ONLY fill if explicitly stated against that specific ailment (e.g., "Cataract with 50% copay").
   Leave null if not mentioned.
4. payment_type auto-detection:
   - cap_value > 100 -> "Flat" (it's an amount)
   - cap_value <= 100 -> "Percent of SI" (it's a percentage)
5. If the document lists ailments under "Disease-wise sub-limit" or "Ailment-wise capping" section,
   extract ALL ailments from that section.

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "ailment_name": "Cataract",
    "ailment_classification": null,
    "is_graded": null,
    "grade_classification": null,
    "is_SI_basis": null,
    "SI_basis_description": null,
    "payment_type": "Flat",
    "cap_value": 50000,
    "copay": null,
    "max_limit": null
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract ailment capping data - returns a LIST of rows
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
    ailment_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(ailment_rows)} ailment capping rows")
    for i, row in enumerate(ailment_rows):
        print(f"  Row {i+1}: {row.get('ailment_name')} | type={row.get('payment_type')} | cap={row.get('cap_value')} | copay={row.get('copay')} | max={row.get('max_limit')} | class={row.get('ailment_classification')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        ailment_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        ailment_rows = []
        print("NO VALID JSON FOUND - no ailment capping in this policy")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re
from collections import defaultdict

# Set entity_id and policy_number
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

if ailment_rows and ailment_rows[0].get("policy_number"):
    policy_number_extracted = ailment_rows[0]["policy_number"]

print("--- Ailment Capping Post-processing ---")

# --- Ailment name normalization map ---
AILMENT_NORMALIZE = {
    "cataract": "Cataract",
    "armd": "ARMD",
    "age-related macular degeneration": "ARMD",
    "age related macular": "ARMD",
    "macular degeneration": "ARMD",
    "femto laser": "Femto laser",
    "femtosecond": "Femto laser",
    "toric lens": "Toric Lens",
    "toric": "Toric Lens",
    "multi focal lens": "Multi focal Lens",
    "multifocal lens": "Multi focal Lens",
    "multifocal": "Multi focal Lens",
    "bio absorbable stent": "Bio absorbable stent",
    "bioabsorbable": "Bio absorbable stent",
    "capd": "CAPD",
    "continuous ambulatory peritoneal": "CAPD",
    "bariatric": "Bariatric surgery",
    "functional endoscopic sinus": "Functional Endoscopic Sinus Surgery",
    "fess": "Functional Endoscopic Sinus Surgery",
    "nasal sinus": "Nasal Sinus surgery",
    "hysterectomy": "Hysterectomy",
    "hernia": "Hernia",
    "joint replacement": "Joint replacement",
    "knee replacement": "Knee Replacement",
    "total replacement of joints": "Total Replacement of Joints",
    "kidney stone": "Kidney stone",
    "renal calculi": "Kidney stone",
    "urinary stone": "Urinary stone",
    "hydrocele": "Hydrocele",
    "appendix": "Appendix",
    "appendectomy": "Appendectomy",
    "appendicitis": "Appendicitis",
    "gall bladder": "Gall Bladder stone",
    "cholecystectomy": "Cholecystectomy",
    "piles": "Piles",
    "haemorrhoids": "Haemorrhoids",
    "hemorrhoids": "Haemorrhoids",
    "fissure": "Fissure",
    "cancer": "Cancer",
    "malignancy": "Malignancy",
    "heart attack": "Heart Attack",
    "myocardial infarction": "Heart Attack",
    "cabg": "CABG",
    "coronary artery bypass": "CABG",
    "ptca": "PTCA",
    "angioplasty": "Angioplasty",
    "open and close heart": "Open and Close Heart Surgery",
    "mental illness": "Mental illness",
    "psychiatric": "Mental illness",
    "neuro disorder": "Neuro disorders",
    "neurological": "Neuro disorders",
    "multiple sclerosis": "Multiple Sclerosis",
    "cerebro vascular": "Cerebro Vascular Strokes",
    "cerebrovascular": "Cerebro Vascular Strokes",
    "stroke": "Stroke",
    "paralysis": "Paralysis",
    "liver disorder": "Liver Disorder",
    "genetic disorder": "Genetic disorders",
    "sleep apnea": "Sleep apnea",
    "encephalitis": "Encephalitis",
    "pacemaker": "Pacemaker implantation",
    "bone marrow": "Bone marrow transplant",
    "kt laser": "KT Laser Prostate",
    "cosmetic surgery": "Cosmetic surgery",
    "dental flap": "Dental flap",
    "sterility": "Sterility",
    "obesity": "Obesity",
    "skin tumor": "Skin tumor",
    "grievous injury": "Grievous injury",
    "hazardous sport": "Hazardous sports",
    "puberty": "Puberty and menopause disorder",
    "menopause": "Puberty and menopause disorder",
    "tonsillectomy": "Tonsillectomy",
    "sinusitis": "Sinusitis",
    "milk teeth": "Milk teeth banking",
}

# --- STEP 1: Normalize and validate each row ---
validated_rows = []
for row in ailment_rows:
    row["entity_id"] = entity_id
    row["policy_number"] = policy_number_extracted
    
    # Normalize ailment_name
    ailment_raw = str(row.get("ailment_name", "")).strip()
    ailment_lower = ailment_raw.lower()
    
    matched = None
    for key, val in AILMENT_NORMALIZE.items():
        if key in ailment_lower:
            matched = val
            break
    
    if matched:
        row["ailment_name"] = matched
    else:
        row["ailment_name"] = ailment_raw  # keep as-is
    
    # ailment_classification: normalize
    ac = row.get("ailment_classification")
    if ac:
        ac_lower = str(ac).strip().lower()
        if ac_lower in ["special coverage", "special", "additional coverage", "additional"]:
            row["ailment_classification"] = "Special coverage"
        else:
            row["ailment_classification"] = None
    else:
        row["ailment_classification"] = None
    
    # payment_type: normalize and auto-detect
    pt = row.get("payment_type")
    cv = row.get("cap_value")
    
    if pt:
        pt_lower = str(pt).strip().lower()
        if pt_lower in ["percent of si", "percentage of si", "% of si"]:
            row["payment_type"] = "Percent of SI"
        elif pt_lower in ["flat", "fixed"]:
            row["payment_type"] = "Flat"
        else:
            row["payment_type"] = None
    else:
        row["payment_type"] = None
    
    # cap_value: validate and auto-detect payment_type
    if cv is not None:
        try:
            cv_num = int(float(str(cv).replace(",", "").replace("Rs.", "").replace("INR", "").strip()))
            row["cap_value"] = cv_num
            # Auto-detect payment_type if not set
            if row["payment_type"] is None:
                if cv_num > 100:
                    row["payment_type"] = "Flat"
                else:
                    row["payment_type"] = "Percent of SI"
            # Correct mismatch
            elif row["payment_type"] == "Percent of SI" and cv_num > 100:
                row["payment_type"] = "Flat"
            elif row["payment_type"] == "Flat" and cv_num <= 100:
                row["payment_type"] = "Percent of SI"
        except (ValueError, TypeError):
            row["cap_value"] = None
    else:
        row["cap_value"] = None
    
    # copay: ONLY keep if explicitly mentioned, otherwise null
    cp = row.get("copay")
    if cp is not None:
        try:
            cp_val = int(float(str(cp).replace("%", "").strip()))
            if cp_val == 0:
                row["copay"] = None  # 0 means not mentioned
            else:
                row["copay"] = cp_val
        except (ValueError, TypeError):
            row["copay"] = None
    else:
        row["copay"] = None
    
    # max_limit: validate
    ml = row.get("max_limit")
    if ml is not None:
        try:
            row["max_limit"] = int(float(str(ml).replace(",", "")))
        except (ValueError, TypeError):
            row["max_limit"] = None
    else:
        row["max_limit"] = None
    
    # is_graded / is_SI_basis: null if not applicable
    if not row.get("is_graded"):
        row["is_graded"] = None
        row["grade_classification"] = None
    if not row.get("is_SI_basis"):
        row["is_SI_basis"] = None
        row["SI_basis_description"] = None
    
    validated_rows.append(row)

# --- STEP 2: Deduplicate by ailment (keep first per ailment+grade+SI) ---
seen = set()
final_rows = []
for row in validated_rows:
    key = (row["ailment_name"], row.get("grade_classification"), row.get("SI_basis_description"))
    if key not in seen:
        seen.add(key)
        final_rows.append(row)

ailment_rows = final_rows

print(f"\n--- Final: {len(ailment_rows)} ailment capping rows ---")
if len(ailment_rows) == 0:
    print("  \u26a0\ufe0f No ailment capping found in this policy")
for i, row in enumerate(ailment_rows):
    print(f"  \u2705 Row {i+1}: {row['ailment_name']} | {row.get('payment_type')} | cap={row.get('cap_value')} | copay={row.get('copay')} | max={row.get('max_limit')} | class={row.get('ailment_classification')}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("ailment_name", StringType(), False),
    StructField("ailment_classification", StringType(), True),
    StructField("is_graded", BooleanType(), True),
    StructField("grade_classification", StringType(), True),
    StructField("is_SI_basis", BooleanType(), True),
    StructField("SI_basis_description", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("cap_value", DecimalType(8, 0), True),
    StructField("copay", DecimalType(8, 0), True),
    StructField("max_limit", DecimalType(8, 0), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in ailment_rows:
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
    ailment_capping_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    ailment_capping_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No ailment capping data found - empty DataFrame created")

display(ailment_capping_df)
print(f"\n\u2705 GMC Ailment Capping extraction complete! ({ailment_capping_df.count()} rows)")
