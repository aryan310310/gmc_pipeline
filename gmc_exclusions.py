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

# DBTITLE 1,GMC Exclusions Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in policy exclusions.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any exclusion not explicitly written in the document
- HALLUCINATE exclusions that are not clearly stated

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY things that are EXPLICITLY stated as "not covered", "excluded", "exclusion", "not payable", "not admissible".
- A single policy has MULTIPLE rows - one per exclusion.
- If no exclusions section is found, return an empty array [].

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_number: Extract the policy number.
3. exclusion_type: STRING. The specific exclusion name/description.

IMPORTANT - DO NOT INCLUDE these (they are tracked in other tables):
- Waiting period related: "1st year", "2nd year", "3rd year", "4th year", "30 day", "30 day waiting period", "PED", "specific waiting period", "specified diseases"
- Modern treatments (tracked in gmc_modern_treatment): "Robotic surgery", "Stem cell", "Stem cell therapy", "Stem cell transplant", "Immunotherapy", "Cochlear implant", "Balloon sinuplasty", "Cyber knife", "Gamma knife", "Lucentis", "Avastin", "Stereotactic", "Bronchial Thermoplasty", "IONM", "Intra renal surgeries", "Retrograde", "Femtolaser"
- Policy details (tracked in gmc_policy_details/addon): "Mental illness", "Mental Illness", "Domiciliary hospitalization", "Domiciliary Hospitalization", "LASIK", "Lasik surgery", "Infertility", "Infertility treatment", "COVID-19", "COVID home care", "AIDS", "AYUSH treatment", "Maternity", "Maternity Benefit", "Maternity expenses", "Organ donor", "Organ Donor Expense", "Gender Reassignment Surgery", "Gender reassignment"
- Ailment capping (tracked in gmc_ailment_capping): "Cataract", "Bariatric surgery", "Cancer", "Heart Attack"

ONLY INCLUDE exclusions like:
- "Cosmetic procedures" / "Cosmetic surgery" / "Cosmetic treatments"
- "Adventure sports" / "Hazardous sports"
- "Self inflicted injuries" / "Self injury" / "Attempted suicide"
- "War" / "Nuclear attacks" / "Nuclear terrorism" / "Biological & Chemical wars"
- "Breach of law" / "Treatment of breach of law"
- "Experimental treatments" / "Unproven treatments" / "Treatment on trial"
- "Nature cure" / "Nature cure treatments" / "Treatment in spas"
- "Rest cure" / "Rest cure & rehabiliation" / "Rehabilitation"
- "Alcoholism" / "Alcholism"
- "Sexual problems & STDs" / "STDs" / "Venereal diseases" / "Erectile dysfunction treatments"
- "Sterility" / "Male sterility"
- "Obesity" / "Weight control surgery" / "Weight loss surgery"
- "Non medical expenses" / "Dietary supplements" / "Expenses on vitamins"
- "Dental" / "Dental treatment"
- "OPD" / "OPD treatment" / "Outpatient treatment"
- "Contraception" / "Contraception hospitalization"
- "Termination of pregnancy" / "Voluntary termination of pregnancy" / "Medical Termination of Pregnancy"
- "Surrogacy" / "Gestational surrogacy"
- "Cosmetic vision treatment" / "Refractive error"
- "Treatment outside India"
- "Vaccination" / "Vaccinations"
- "Preventive care" / "Health checkup" / "Wellness"
- "Hearing aids" / "Prosthesis" / "Fitting of prosthesis"
- "Non allopathic Treatments" / "Homeopathic Medicine"
- "Flying activity" / "Involvement in naval, military or air force operation"
- "Fraudulent claims" / "Duplicate coverage"
- "Suicide" / "Intentional self injury"
- "Change of gender treatment"
- "Congenital external diseases" / "Congenital internal diseases"
- "Sleep apnea" / "Sleep disorders"
- "Puberty and menopause disorder"
- "Genetic disorders" / "Genetic Disorder"
- "Cerebral palsy" / "Cretinism" / "Mongolism" / "Subnormal intelligence"
- "Ozone therapy" / "Ozone Therapy"
- "Quantum Magnetic Resonance Therapy" / "RFQMR"
- "Rejuvenation therapy" / "Rejuvenation Therapy"
- "Room rent" (if room rent is excluded/not covered)
- "Non network hospitals" / "Excluded providers" / "Preferred provider network"
- "Doctors tariff" / "Non standard doctor fees" / "Surgeon fees"
- "Hormonal therapy" / "Hormonal Therapy"
- "Panchakarma"
- "Priapism"
- "Any device/instrument/machine contributing/replacing the function of an organ"
- "Enhanced External Counter Pulsation" / "Enhanced External Counter Pulsation Therapy"
- "AOY clause"

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "exclusion_type": "Cosmetic procedures"
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract exclusions data - returns a LIST of rows
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
    exclusion_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(exclusion_rows)} exclusion rows")
    for i, row in enumerate(exclusion_rows):
        print(f"  Row {i+1}: {row.get('exclusion_type')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        exclusion_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        exclusion_rows = []
        print("NO VALID JSON FOUND - no exclusions in this policy")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id and policy_number
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

if exclusion_rows and exclusion_rows[0].get("policy_number"):
    policy_number_extracted = exclusion_rows[0]["policy_number"]

print("--- Exclusions Post-processing ---")

# --- ITEMS TO IGNORE (tracked in other tables) ---
IGNORE_EXCLUSIONS = {
    # Waiting period related
    "1st year", "2nd year", "3rd year", "4th year", "30 day", "30 day waiting period",
    "ped", "specific waiting period", "specified diseases",
    # Modern treatments
    "robotic surgery", "robotic surgeries", "stem cell", "stem cell therapy",
    "stem cell transplant", "stem cell transplantation", "immunotherapy",
    "cochlear implant", "balloon sinuplasty", "ballon sinuplasty",
    "cyber knife", "gamma knife", "lucentis", "avastin",
    "stereotactic", "bronchial thermoplasty", "ionm",
    "intra renal surgeries", "intrarenal surgery", "retrograde",
    "retrograde intra renal surgery", "femtolaser", "femato laser surgery",
    # Policy details / addon
    "mental illness", "mental retardation", "mental intellectual disability",
    "domiciliary hospitalization", "domiciliary",
    "lasik", "lasik surgery", "infertility", "infertility treatment", "infertiliy",
    "covid-19", "covid home care", "aids",
    "ayush treatment", "ayush",
    "maternity", "maternity benefit", "maternity expenses",
    "organ donor", "organ donor expense",
    "gender reassignment surgery", "gender reassignment cover",
    "gender reassignment treatment", "gender reassignment",
    "gender transition treatments", "gender treatments", "gender treatment",
    "gender debility",
    "critical illness benefit", "attendant charges cover",
    "well baby & well mother expense",
    # Ailment capping
    "cataract", "bariatric surgery", "bariatic surgery", "cancer", "heart attack",
}

# --- Exclusion name normalization map ---
EXCLUSION_NORMALIZE = {
    "cosmetic procedures": "Cosmetic procedures",
    "cosmetic surgery": "Cosmetic surgery",
    "cosmetic treatments": "Cosmetic treatments",
    "cosmetic septoplasty": "Cosmetic septoplasty",
    "cosmetic vision treatment": "Cosmetic vision treatment",
    "adventure sports": "Adventure sports",
    "hazardous sports": "Hazardous sports",
    "self inflicted injuries": "Self inflicted injuries",
    "self inflicted": "Self inflicted injuries",
    "self injury": "Self inflicted injuries",
    "intentional self injury": "Intentional self injury",
    "attempted suicide": "Attempted suicide",
    "suicide": "Suicide",
    "war": "War",
    "nuclear attack": "Nuclear attacks",
    "nuclear terrorism": "Nuclear terrorism",
    "nuclear war": "Nuclear War",
    "biological & chemical war": "Biological & Chemical wars",
    "breach of law": "Breach of law",
    "experimental treatment": "Experimental treatments",
    "unproven treatment": "Unproven treatments",
    "treatment on trial": "Treatment on trial",
    "trial treatment": "Treatment on trial",
    "nature cure": "Nature cure",
    "treatment in spa": "Treatment in spas",
    "naturopathy": "Naturopathy treatment",
    "rest cure": "Rest cure",
    "rehabilitation": "Rehabilitation",
    "alcoholism": "Alcoholism",
    "alchohol": "Alcoholism",
    "alcholism": "Alcoholism",
    "sexual problem": "Sexual problems & STDs",
    "std": "STDs",
    "venereal disease": "Venereal diseases",
    "erectile dysfunction": "Erectile dysfunction treatments",
    "sterility": "Sterility",
    "male sterility": "Male sterility",
    "obesity": "Obesity",
    "weight control": "Weight control surgery",
    "weight loss surgery": "Weight loss surgery",
    "non medical expense": "Non medical expenses",
    "dietary supplement": "Dietary supplements",
    "expense on vitamin": "Expenses on vitamins",
    "dental treatment": "Dental treatment",
    "dental": "Dental",
    "opd treatment": "OPD treatment",
    "opd cover": "OPD coverage",
    "opd claim": "OPD claims",
    "outpatient treatment": "Outpatient treatment",
    "outpatient cover": "Outpatient cover",
    "contraception": "Contraception",
    "termination of pregnancy": "Termination of pregnancy",
    "voluntary termination": "Voluntary termination of pregnancy",
    "medical termination of pregnancy": "Medical Termination of Pregnancy",
    "surrogacy": "Surrogacy",
    "gestational surrogacy": "Gestational surrogacy",
    "refractive error": "Refractive error",
    "treatment outside india": "Treatment outside India",
    "vaccination": "Vaccination",
    "preventive care": "Preventive care",
    "health checkup": "Health checkup",
    "wellness": "Wellness",
    "hearing aid": "Hearing aids",
    "prosthesis": "Prosthesis",
    "fitting of prosthesis": "Fitting of prosthesis",
    "prosthetic cover": "Prosthetic cover",
    "non allopathic": "Non-allopathic Treatments",
    "homeopathic": "Homeopathic Medicine & Unani Treatment Cover",
    "flying activity": "Flying activity",
    "naval, military": "Involvement in naval, military or air force operation",
    "fraudulent claim": "Fraudulent claims",
    "duplicate coverage": "Duplicate coverage",
    "change of gender": "Change of gender treatment",
    "congenital external": "Congenital external diseases",
    "congenital internal": "Congenital internal diseases",
    "sleep apnea": "Sleep apnea",
    "sleep disorder": "Sleep disorders",
    "puberty": "Puberty and menopause disorder",
    "genetic disorder": "Genetic disorders",
    "cerebral palsy": "Cerebral palsy",
    "cretinism": "Cretinism",
    "mongolism": "Mongolism",
    "subnormal intelligence": "Subnormal intelligence",
    "ozone therapy": "Ozone therapy",
    "quantum magnetic": "Quantum Magnetic Resonance Therapy",
    "rfqmr": "RFQMR",
    "rejuvenation therapy": "Rejuvenation therapy",
    "non network hospital": "Non network hospitals",
    "excluded provider": "Excluded providers",
    "preferred provider": "Preferred provider network",
    "doctors tariff": "Doctors tariff",
    "non standard doctor": "Non standard doctor fees",
    "surgeon fee": "Surgeon fees",
    "hormonal therapy": "Hormonal therapy",
    "panchakarma": "Panchakarma",
    "priapism": "Priapism",
    "enhanced external counter pulsation": "Enhanced External Counter Pulsation",
    "aoy clause": "AOY clause",
    "comfort treatment": "Comfort treatments",
    "personal comfort": "Personal comfort and convenience",
    "health comfort": "Health comfort",
    "admin/registration": "Admin/Registration/Service/Misc",
    "registration": "Admin/Registration/Service/Misc",
    "holter monitoring": "Holter monitoring",
    "dementia": "Dementia",
    "alzheimer": "Alzheimer's disease",
    "parkinson": "Parkinson's disease",
    "multiple sclerosis": "Multiple Sclerosis",
    "encephalitis": "Encephalitis",
    "tubectomy": "Tubectomy",
}

# --- STEP 1: Normalize, filter ignored, and validate ---
validated_rows = []
for row in exclusion_rows:
    row["entity_id"] = entity_id
    row["policy_number"] = policy_number_extracted
    
    # Get exclusion_type
    exc_raw = str(row.get("exclusion_type", "")).strip()
    exc_lower = exc_raw.lower()
    
    # SKIP if in ignore list
    if exc_lower in IGNORE_EXCLUSIONS:
        print(f"  \u274c Skipped (tracked in other table): '{exc_raw}'")
        continue
    
    # Also skip partial matches for ignore items
    skip = False
    for ignore_item in IGNORE_EXCLUSIONS:
        if ignore_item in exc_lower:
            print(f"  \u274c Skipped (partial match to '{ignore_item}'): '{exc_raw}'")
            skip = True
            break
    if skip:
        continue
    
    # Normalize exclusion name
    matched = None
    for key, val in EXCLUSION_NORMALIZE.items():
        if key in exc_lower:
            matched = val
            break
    
    if matched:
        row["exclusion_type"] = matched
    else:
        row["exclusion_type"] = exc_raw  # keep as-is
    
    validated_rows.append(row)

# --- STEP 2: Deduplicate ---
seen = set()
final_rows = []
for row in validated_rows:
    key = row["exclusion_type"]
    if key not in seen:
        seen.add(key)
        final_rows.append(row)

exclusion_rows = final_rows

print(f"\n--- Final: {len(exclusion_rows)} exclusion rows ---")
if len(exclusion_rows) == 0:
    print("  \u26a0\ufe0f No exclusions found in this policy (or all are tracked elsewhere)")
for i, row in enumerate(exclusion_rows):
    print(f"  \u2705 Row {i+1}: {row['exclusion_type']}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("exclusion_type", StringType(), False)
])

# Convert rows to proper types
formatted_rows = []
for row in exclusion_rows:
    formatted = {
        "entity_id": entity_id,
        "policy_number": row.get("policy_number") or policy_number_extracted,
        "exclusion_type": str(row.get("exclusion_type", "")).strip()
    }
    if formatted["exclusion_type"]:
        formatted_rows.append(formatted)

# Create DataFrame
if formatted_rows:
    exclusions_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    exclusions_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No exclusions data found - empty DataFrame created")

display(exclusions_df)
print(f"\n\u2705 GMC Exclusions extraction complete! ({exclusions_df.count()} rows)")
