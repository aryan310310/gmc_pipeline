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

# DBTITLE 1,GMC Policy Addon Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in add-on/additional benefits.

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
- limit_type STRICT ENUM: ["Within SI", "Additional", "Within Maternity"] - ONLY these values allowed
- payment_type STRICT ENUM: ["Percent of SI", "Flat"] - ONLY these values allowed
- if field missing return null
- return booleans as true/false only

IMPORTANT - FIELD EXTRACTION RULES:

1. entity_id: Always set to null (will be assigned in post-processing)

2. policy_number: Extract the policy number from the document.

3. benefit_portability: Is benefit portability/continuity mentioned?
   Look for: "portability", "continuity benefit", "credit for waiting period"

4. dual_coverage_allowed: Is dual/double coverage allowed?
   Look for: "dual coverage", "double cover", "multiple policies"

5. unmarried_girl_cover_no_age_capping: Is there no age limit for unmarried daughters?
   Look for: "unmarried daughter", "no age capping", "age limit waived"

6. specially_abled_child_cover_no_age_capping: Is there no age limit for specially abled children?
   Look for: "specially abled", "disabled child", "handicapped", "no age capping"

7. sibling_cover: Are siblings covered?
   Look for: "sibling", "brother", "sister" in coverage context

8. AIDS_covered: Is AIDS/HIV treatment covered?
   Look for: "AIDS", "HIV", "Human Immunodeficiency"

9. COVID19_covered: Is COVID-19 treatment covered?
   Look for: "COVID", "Corona", "SARS-CoV"

10. quarantine_expense_covered: Are quarantine expenses covered?
    Look for: "quarantine", "isolation expenses"

11. MTP_coverage: Is Medical Termination of Pregnancy covered?
    Look for: "MTP", "Medical Termination of Pregnancy", "abortion"

12. emergency_hospitalisation_covered: Is emergency hospitalization covered?
    Look for: "emergency hospitalization", "emergency admission"

13. mobility_aid_covered / mobility_aid_limit_type / mobility_aid_payment_type / mobility_aid_value:
    Look for: "mobility aid", "wheelchair", "prosthetic", "orthopedic appliance"

14. maternity_expense_life_threatening: What is the maternity life-threatening provision?
    Look for: "life threatening", "ectopic", "complications of pregnancy"
    Return the text description or null.

15. gender_reassignment_surgery_covered / gender_reassignment_limit_type / gender_reassignment_payment_type / gender_reassignment_value:
    Look for: "gender reassignment", "sex change", "gender affirmation"

16. stem_cell_preservation_covered / stem_cell_preservation_limit_type / stem_cell_preservation_payment_type / stem_cell_preservation_value:
    Look for: "stem cell", "cord blood", "preservation"

17. no_deduction_on_death_NME: No deduction on death for non-medical expenses?
    Look for: "no deduction on death", "NME waived on death"

18. widow_widower_family_coverage_on_death: Continued coverage for family after employee death?
    Look for: "widow", "widower", "family coverage on death", "continued coverage"

19. genetic_disorder_hospitalization_covered / genetic_disorder_hospitalization_limit_type / genetic_disorder_hospitalization_payment_type / genetic_disorder_hospitalization_value:
    Look for: "genetic disorder", "hereditary disease", "genetic condition"

20. ectopic_pregnancy_covered / ectopic_pregnancy_limit_type / ectopic_pregnancy_payment_type / ectopic_pregnancy_value:
    Look for: "ectopic pregnancy"

21. autism_covered / autism_limit_type / autism_payment_type / autism_value:
    Look for: "autism", "ASD", "autism spectrum"

22. ADHD_covered / ADHD_limit_type / ADHD_payment_type / ADHD_value:
    Look for: "ADHD", "Attention Deficit"

23. advanced_equipment_cost_covered / advanced_equipment_limit_type / advanced_equipment_payment_type / advanced_equipment_cost:
    Look for: "advanced equipment", "robotic surgery", "advanced technology"

24. critical_illness_covered / critical_illness_limit_type / critical_illness_payment_type / critical_illness_value:
    Look for: "critical illness", "CI cover", "dread disease"

Extract ALL fields exactly as below:

{
  "entity_id": null,
  "policy_number": null,
  "benefit_portability": null,
  "dual_coverage_allowed": null,
  "unmarried_girl_cover_no_age_capping": null,
  "specially_abled_child_cover_no_age_capping": null,
  "sibling_cover": null,
  "AIDS_covered": null,
  "COVID19_covered": null,
  "quarantine_expense_covered": null,
  "MTP_coverage": null,
  "emergency_hospitalisation_covered": null,
  "mobility_aid_covered": null,
  "mobility_aid_limit_type": null,
  "mobility_aid_payment_type": null,
  "mobility_aid_value": null,
  "maternity_expense_life_threatening": null,
  "gender_reassignment_surgery_covered": null,
  "gender_reassignment_limit_type": null,
  "gender_reassignment_payment_type": null,
  "gender_reassignment_value": null,
  "stem_cell_preservation_covered": null,
  "stem_cell_preservation_limit_type": null,
  "stem_cell_preservation_payment_type": null,
  "stem_cell_preservation_value": null,
  "no_deduction_on_death_NME": null,
  "widow_widower_family_coverage_on_death": null,
  "genetic_disorder_hospitalization_covered": null,
  "genetic_disorder_hospitalization_limit_type": null,
  "genetic_disorder_hospitalization_payment_type": null,
  "genetic_disorder_hospitalization_value": null,
  "ectopic_pregnancy_covered": null,
  "ectopic_pregnancy_limit_type": null,
  "ectopic_pregnancy_payment_type": null,
  "ectopic_pregnancy_value": null,
  "autism_covered": null,
  "autism_limit_type": null,
  "autism_payment_type": null,
  "autism_value": null,
  "ADHD_covered": null,
  "ADHD_limit_type": null,
  "ADHD_payment_type": null,
  "ADHD_value": null,
  "advanced_equipment_cost_covered": null,
  "advanced_equipment_limit_type": null,
  "advanced_equipment_payment_type": null,
  "advanced_equipment_cost": null,
  "critical_illness_covered": null,
  "critical_illness_limit_type": null,
  "critical_illness_payment_type": null,
  "critical_illness_value": null
}
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
# Extract GMC policy addon data using the shared LLM function
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
    "benefit_portability", "dual_coverage_allowed", "unmarried_girl_cover_no_age_capping",
    "specially_abled_child_cover_no_age_capping", "sibling_cover", "AIDS_covered",
    "COVID19_covered", "quarantine_expense_covered", "MTP_coverage",
    "emergency_hospitalisation_covered", "mobility_aid_covered",
    "gender_reassignment_surgery_covered", "stem_cell_preservation_covered",
    "no_deduction_on_death_NME", "widow_widower_family_coverage_on_death",
    "genetic_disorder_hospitalization_covered", "ectopic_pregnancy_covered",
    "autism_covered", "ADHD_covered", "advanced_equipment_cost_covered",
    "critical_illness_covered"
]

# Apply boolean normalization
data = apply_boolean_normalization(data, boolean_columns)

# --- PDF KEYWORD VERIFICATION ---
# Override LLM hallucinations: if keyword not found in PDF text, set to FALSE
# This prevents the LLM from inferring coverage not explicitly stated

KEYWORD_VERIFICATION = {
    "AIDS_covered": r'(?i)(AIDS|HIV|Human\s*Immunodeficiency)',
    "COVID19_covered": r'(?i)(COVID|Corona\s*virus|SARS[\-\s]CoV)',
    "MTP_coverage": r'(?i)(MTP|Medical\s*Termination\s*of\s*Pregnancy|abortion)',
    "quarantine_expense_covered": r'(?i)(quarantine|isolation\s*expense)',
    "emergency_hospitalisation_covered": r'(?i)(emergency\s*hospitali|emergency\s*admission|domestic\s*emergency\s*medical|domestic\s*medical\s*emergenc|medical\s*emergenc\s*assist|emergency\s*medical\s*assist)',
    "benefit_portability": r'(?i)(portability|continuity\s*benefit|credit\s*for\s*waiting)',
    "dual_coverage_allowed": r'(?i)(dual\s*cover|double\s*cover|multiple\s*polic)',
    "unmarried_girl_cover_no_age_capping": r'(?i)(unmarried\s*(daughter|girl)|no\s*age\s*cap)',
    "specially_abled_child_cover_no_age_capping": r'(?i)(specially\s*abled|disabled\s*child|handicapped)',
    "sibling_cover": r'(?i)(sibling|brother|sister)',
    "mobility_aid_covered": r'(?i)(mobility\s*aid|wheelchair|prosthetic|orthop)',
    "gender_reassignment_surgery_covered": r'(?i)(gender\s*reassignment|sex\s*change|gender\s*affirm)',
    "stem_cell_preservation_covered": r'(?i)(stem\s*cell|cord\s*blood)',
    "no_deduction_on_death_NME": r'(?i)(no\s*deduction\s*on\s*death|NME\s*waiv)',
    "widow_widower_family_coverage_on_death": r'(?i)(widow|widower|family\s*coverage\s*on\s*death|continued\s*coverage)',
    "genetic_disorder_hospitalization_covered": r'(?i)(genetic\s*disorder|hereditary\s*disease)',
    "ectopic_pregnancy_covered": r'(?i)(ectopic\s*pregnan)',
    "autism_covered": r'(?i)(autism|ASD|autism\s*spectrum)',
    "ADHD_covered": r'(?i)(ADHD|Attention\s*Deficit)',
    "advanced_equipment_cost_covered": r'(?i)(advanced\s*equipment|robotic\s*surg|advanced\s*technolog)',
    "critical_illness_covered": r'(?i)(critical\s*illness|CI\s*cover|dread\s*disease)',
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

# --- MATERNITY EXPENSE LIFE THREATENING LOGIC ---
# Check if maternity is covered in the PDF
MATERNITY_PATTERN = r'(?i)(maternity\s*benefit|maternity\s*cover)'
LIFE_THREATENING_MATERNITY_PATTERN = r'(?i)(maternit.{0,30}(life\s*threaten|emergenc|full\s*sum|family\s*sum|FSI|upto\s*SI|within\s*SI))'

if re.search(MATERNITY_PATTERN, text):
    # Maternity is covered
    if re.search(LIFE_THREATENING_MATERNITY_PATTERN, text):
        # Explicitly mentions maternity in life-threatening/emergency covered up to SI/FSI
        data["maternity_expense_life_threatening"] = "FSI"
        print("  \u2705 maternity_expense_life_threatening: Life-threatening maternity at full SI found \u2192 FSI")
    else:
        # Maternity covered but no explicit life-threatening mention -> default Within Maternity
        data["maternity_expense_life_threatening"] = "Within Maternity"
        print("  \u2705 maternity_expense_life_threatening: Maternity covered, no explicit life-threatening \u2192 Within Maternity")
else:
    data["maternity_expense_life_threatening"] = None
    print("  \u2139\ufe0f maternity_expense_life_threatening: No maternity coverage found \u2192 null")

# Validate limit_type and payment_type enums
ALLOWED_LIMIT_TYPES = ["Within SI", "Additional", "Within Maternity"]
ALLOWED_PAYMENT_TYPES = ["Percent of SI", "Flat"]

# Benefit groups: (covered_field, limit_type, payment_type, value_field)
benefit_groups = [
    ("mobility_aid_covered", "mobility_aid_limit_type", "mobility_aid_payment_type", "mobility_aid_value"),
    ("gender_reassignment_surgery_covered", "gender_reassignment_limit_type", "gender_reassignment_payment_type", "gender_reassignment_value"),
    ("stem_cell_preservation_covered", "stem_cell_preservation_limit_type", "stem_cell_preservation_payment_type", "stem_cell_preservation_value"),
    ("genetic_disorder_hospitalization_covered", "genetic_disorder_hospitalization_limit_type", "genetic_disorder_hospitalization_payment_type", "genetic_disorder_hospitalization_value"),
    ("ectopic_pregnancy_covered", "ectopic_pregnancy_limit_type", "ectopic_pregnancy_payment_type", "ectopic_pregnancy_value"),
    ("autism_covered", "autism_limit_type", "autism_payment_type", "autism_value"),
    ("ADHD_covered", "ADHD_limit_type", "ADHD_payment_type", "ADHD_value"),
    ("advanced_equipment_cost_covered", "advanced_equipment_limit_type", "advanced_equipment_payment_type", "advanced_equipment_cost"),
    ("critical_illness_covered", "critical_illness_limit_type", "critical_illness_payment_type", "critical_illness_value"),
]

for covered_field, lt_field, pt_field, val_field in benefit_groups:
    # If covered is false, nullify sub-fields (limit_type, payment_type, value)
    if data.get(covered_field) is not True:
        data[lt_field] = None
        data[pt_field] = None
        data[val_field] = None
    else:
        # Validate enums
        if data.get(lt_field) and data[lt_field] not in ALLOWED_LIMIT_TYPES:
            data[lt_field] = None
        if data.get(pt_field) and data[pt_field] not in ALLOWED_PAYMENT_TYPES:
            data[pt_field] = None
        # If covered=true but details missing, default to Within SI / Percent of SI / 100
        if data.get(covered_field) is True:
            if not data.get(lt_field):
                data[lt_field] = "Within SI"
            if not data.get(pt_field):
                data[pt_field] = "Percent of SI"
            if not data.get(val_field):
                data[val_field] = 100

# Payment type correction: if value > 100, it's Flat
for _, lt_field, pt_field, val_field in benefit_groups:
    val = data.get(val_field)
    if val is not None:
        try:
            if int(val) > 100:
                data[pt_field] = "Flat"
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
    StructField("benefit_portability", BooleanType(), True),
    StructField("dual_coverage_allowed", BooleanType(), True),
    StructField("unmarried_girl_cover_no_age_capping", BooleanType(), True),
    StructField("specially_abled_child_cover_no_age_capping", BooleanType(), True),
    StructField("sibling_cover", BooleanType(), True),
    StructField("AIDS_covered", BooleanType(), True),
    StructField("COVID19_covered", BooleanType(), True),
    StructField("quarantine_expense_covered", BooleanType(), True),
    StructField("MTP_coverage", BooleanType(), True),
    StructField("emergency_hospitalisation_covered", BooleanType(), True),
    StructField("mobility_aid_covered", BooleanType(), True),
    StructField("mobility_aid_limit_type", StringType(), True),
    StructField("mobility_aid_payment_type", StringType(), True),
    StructField("mobility_aid_value", DecimalType(8, 0), True),
    StructField("maternity_expense_life_threatening", StringType(), True),
    StructField("gender_reassignment_surgery_covered", BooleanType(), True),
    StructField("gender_reassignment_limit_type", StringType(), True),
    StructField("gender_reassignment_payment_type", StringType(), True),
    StructField("gender_reassignment_value", DecimalType(8, 0), True),
    StructField("stem_cell_preservation_covered", BooleanType(), True),
    StructField("stem_cell_preservation_limit_type", StringType(), True),
    StructField("stem_cell_preservation_payment_type", StringType(), True),
    StructField("stem_cell_preservation_value", DecimalType(8, 0), True),
    StructField("no_deduction_on_death_NME", BooleanType(), True),
    StructField("widow_widower_family_coverage_on_death", BooleanType(), True),
    StructField("genetic_disorder_hospitalization_covered", BooleanType(), True),
    StructField("genetic_disorder_hospitalization_limit_type", StringType(), True),
    StructField("genetic_disorder_hospitalization_payment_type", StringType(), True),
    StructField("genetic_disorder_hospitalization_value", DecimalType(8, 0), True),
    StructField("ectopic_pregnancy_covered", BooleanType(), True),
    StructField("ectopic_pregnancy_limit_type", StringType(), True),
    StructField("ectopic_pregnancy_payment_type", StringType(), True),
    StructField("ectopic_pregnancy_value", DecimalType(8, 0), True),
    StructField("autism_covered", BooleanType(), True),
    StructField("autism_limit_type", StringType(), True),
    StructField("autism_payment_type", StringType(), True),
    StructField("autism_value", DecimalType(8, 0), True),
    StructField("ADHD_covered", BooleanType(), True),
    StructField("ADHD_limit_type", StringType(), True),
    StructField("ADHD_payment_type", StringType(), True),
    StructField("ADHD_value", DecimalType(8, 0), True),
    StructField("advanced_equipment_cost_covered", BooleanType(), True),
    StructField("advanced_equipment_limit_type", StringType(), True),
    StructField("advanced_equipment_payment_type", StringType(), True),
    StructField("advanced_equipment_cost", DecimalType(8, 0), True),
    StructField("critical_illness_covered", BooleanType(), True),
    StructField("critical_illness_limit_type", StringType(), True),
    StructField("critical_illness_payment_type", StringType(), True),
    StructField("critical_illness_value", DecimalType(8, 0), True)
])

# Filter data to only schema columns and convert Decimal types
schema_cols = [f.name for f in schema.fields]
filtered_data = {}
for col_name in schema_cols:
    val = data.get(col_name)
    # Convert numeric values to Decimal for DecimalType fields
    field = schema[col_name]
    if isinstance(field.dataType, DecimalType) and val is not None:
        try:
            filtered_data[col_name] = Decimal(str(int(val)))
        except (ValueError, TypeError):
            filtered_data[col_name] = None
    else:
        filtered_data[col_name] = val

# Create DataFrame
addon_df = spark.createDataFrame([filtered_data], schema=schema)
display(addon_df)

print("\n✅ GMC Policy Addon extraction complete!")
