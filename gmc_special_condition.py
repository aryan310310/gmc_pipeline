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

# DBTITLE 1,GMC Special Condition Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in special conditions and unique coverages.

Return ONLY valid JSON.

DO NOT:
- add explanations
- add markdown
- add comments
- add ```json
- INFER or ASSUME any condition not explicitly written in the document
- HALLUCINATE values that are not clearly stated

CRITICAL EXTRACTION PRINCIPLE:
- Extract ONLY special/unique conditions, coverages, or rules that are EXPLICITLY stated in the document.
- These are things that don't fit neatly into standard tables (maternity, room rent, waiting period, copay, ailments, exclusions, modern treatments).
- Look for UNIQUE, RARE, or SPECIAL clauses/conditions with a specific flat amount or percent of SI.
- If nothing special is found, return an empty array [].

FIELD DEFINITIONS:

1. entity_id: Always null.
2. policy_numbers: Extract the policy number (STRING).

3. special_condition_code: STRING. A short snake_case code for the condition.
   Known codes (use these if matching):
   - "Health_Check-up" - Health check-up benefit with specific amount
   - "Annual_Check_up" - Annual check-up with frequency/amount
   - "health_check_up" - Health check-up (alternative)
   - "Home_Care" - Home care/domiciliary treatment with specific days/limit
   - "Lab_test" - Lab test cost coverage
   - "PCOS_PCOD" - PCOS/PCOD specific coverage
   - "child_vaccination" - Child vaccination benefit
   - "costumery_charges" - Reasonable & customary charges clause
   - "domestic_emergency" - Domestic emergency medical assistance
   - "impairment" - Drug/stimulant impairment clause
   - "non_payable_item" - Non-medical items coverage
   - "post_hospital_enhancment" - Extended post-hospitalization (beyond standard)
   - "attempted_suicide" - Attempted suicide waiting period clause
   - "Miscarriage" - Miscarriage coverage
   - "Missed_abortion" - Missed abortion coverage
   - "Still Birth" - Still birth coverage
   - "Post-partum haemorrhage" - Post-partum haemorrhage
   - "Retained placental membrane" - Retained placental membrane
   - "Abnormal_presentation" - Abnormal presentation coverage
   - "PCOS_PCOD" - PCOS/PCOD treatment coverage

   For NEW/UNRECOGNIZED conditions: create a descriptive snake_case code.

4. special_condition_description: STRING or null.
   - A short description of what the condition covers.
   - Examples: "per employee", "Home care treatment", "PCOS PCOD", "Health check up covered for employee"

5. special_condition_rule: STRING or null.
   - The specific value, amount, or rule.
   - Examples: "5000", "10000", "Actuals", "14 days", "5% of SI", "Within SI- 15000",
     "10K within Maternity limit", "Waived off", "Applicable", "100 years", "120 days", "30L"
   - Use the exact value/text as written in the document.
   - null if no specific value/rule mentioned.

WHAT TO LOOK FOR:
- Unique flat amounts for specific benefits not tracked elsewhere
- Special day limits (e.g., "Home care: 14 days")
- Percentage of SI for rare conditions (e.g., "5% of SI for drug impairment")
- Extended periods (e.g., "post-hospitalization enhanced to 120 days for cancer")
- Special conditions around maternity complications (miscarriage, still birth, PPH, abnormal presentation)
- Health check-up amounts per employee or per family
- Child vaccination limits
- Non-medical item coverage for specific grades
- Disabled child age extensions
- Domestic emergency amounts
- Any coverage with a specific numeric value that doesn't belong in other tables

DO NOT INCLUDE (tracked in other tables):
- Standard maternity limits (gmc_maternity table)
- Room rent percentages (gmc_room_rent table)
- Waiting period waivers (gmc_waiting_period table)
- Co-pay percentages (gmc_copay table)
- Modern treatment percentages (gmc_modern_treatment table)
- Ailment-wise caps (gmc_ailment_capping table)
- Standard exclusions (gmc_exclusions table)
- SI slabs (gmc_si table)
- OPD benefits (gmc_opd_addon table)
- Family capping (gmc_fam_capping table)

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_numbers": null,
    "special_condition_code": "Health_Check-up",
    "special_condition_description": "per employee",
    "special_condition_rule": "5000"
  }
]
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract special conditions - returns a LIST of rows
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
    sc_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(sc_rows)} special condition rows")
    for i, row in enumerate(sc_rows):
        print(f"  Row {i+1}: {row.get('special_condition_code')} | desc={row.get('special_condition_description')} | rule={row.get('special_condition_rule')}")
else:
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        sc_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        sc_rows = []
        print("NO VALID JSON FOUND - no special conditions in this policy")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id and policy_number
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

if sc_rows and sc_rows[0].get("policy_numbers"):
    policy_number_extracted = sc_rows[0]["policy_numbers"]

print("--- Special Condition Post-processing ---")

# --- ITEMS TO REJECT (already tracked in other tables) ---
# These keywords in code or description mean the row belongs elsewhere
REJECT_KEYWORDS = [
    # gmc_policy_details / gmc_policy_addon
    "ayush", "well_baby", "well baby", "wellbaby", "well_mother", "well mother", "wellmother",
    "maternity", "pre_post_natal", "pre post natal", "infertility", "surrogacy",
    "corporate_buffer", "corporate buffer", "ambulance", "air_ambulance",
    "day_care", "day care", "lasik", "domiciliary", "psychiatric",
    "organ_donor", "organ donor", "congenital", "terrorism", "dental",
    "claim_settlement", "claim settlement", "automatic_si", "midterm",
    "lgbtq", "partner_covered", "parents_covered", "cross_combi",
    # gmc_modern_treatment
    "modern_treatment", "modern treatment", "robotic", "stem_cell", "stem cell",
    "immunotherapy", "cochlear", "balloon", "sinuplasty", "cyber_knife", "cyber knife",
    "gamma_knife", "gamma knife", "lucentis", "avastin", "bariatric",
    "bronchial", "stereotactic", "ionm", "vaporisation", "oral_chemotherapy",
    "intravitreous", "intravitreal",
    # gmc_waiting_period
    "waiting_period", "waiting period", "ped", "pre_existing", "pre-existing",
    # gmc_copay
    "copay", "co_pay", "co-pay",
    # gmc_room_rent
    "room_rent", "room rent", "icu",
    # gmc_si
    "sum_insured", "sum insured",
    # gmc_exclusions
    "exclusion",
    # gmc_ailment_capping
    "cataract", "cancer", "heart_attack", "heart attack",
    # gmc_opd_addon
    "opd",
    # gmc_fam_capping
    "family_capping", "family capping", "max_allowed",
    # gmc_demographics
    "demographic", "lives_covered", "lives covered",
    # gmc_maternity / gmc_maternity_addon
    "normal_delivery", "c_section", "c-section", "caesarean", "twin",
    # Other items already in policy_addon
    "covid", "aids", "gender_reassignment", "gender reassignment",
    "mobility_aid", "critical_illness", "critical illness",
    "emergency_hosp", "domestic_emergency", "domestic emergency",
    # Already tracked fields
    "baby_day", "baby day", "parental_enrol", "parental enrol",
    "lock.?in",
]

# --- Known special_condition_codes for normalization ---
KNOWN_CODES = {
    "health_check-up": "Health_Check-up",
    "health check-up": "Health_Check-up",
    "health_check_up": "health_check_up",
    "health check up": "health_check_up",
    "annual_check_up": "Annual_Check_up",
    "annual check up": "Annual_Check_up",
    "home_care": "Home_Care",
    "home care": "Home_Care",
    "lab_test": "Lab_test",
    "lab test": "Lab_test",
    "pcos_pcod": "PCOS_PCOD",
    "pcos": "PCOS_PCOD",
    "pcod": "PCOS_PCOD",
    "child_vaccination": "child_vaccination",
    "child vaccination": "child_vaccination",
    "costumery_charges": "costumery_charges",
    "customary charges": "costumery_charges",
    "reasonable & customary": "costumery_charges",
    "reasonable & costumery": "costumery_charges",
    "impairment": "impairment",
    "non_payable_item": "non_payable_item",
    "non payable": "non_payable_item",
    "non-payable": "non_payable_item",
    "non medical item": "non_payable_item",
    "post_hospital_enhancment": "post_hospital_enhancment",
    "post hospital": "post_hospital_enhancment",
    "attempted_suicide": "attempted_suicide",
    "attempted suicide": "attempted_suicide",
    "miscarriage": "Miscarriage",
    "missed_abortion": "Missed_abortion",
    "missed abortion": "Missed_abortion",
    "still birth": "Still Birth",
    "still_birth": "Still Birth",
    "stillbirth": "Still Birth",
    "post-partum haemorrhage": "Post-partum haemorrhage",
    "post partum": "Post-partum haemorrhage",
    "pph": "Post-partum haemorrhage",
    "retained placental": "Retained placental membrane",
    "abnormal_presentation": "Abnormal_presentation",
    "abnormal presentation": "Abnormal_presentation",
    "disabled_child": "disabled_child",
    "disabled child": "disabled_child",
}

# --- STEP 1: Filter out items that belong in other tables ---
filtered_rows = []
for row in sc_rows:
    code_raw = str(row.get("special_condition_code", "")).strip()
    desc_raw = str(row.get("special_condition_description", "")).strip()
    combined = (code_raw + " " + desc_raw).lower()
    
    rejected = False
    for keyword in REJECT_KEYWORDS:
        if re.search(keyword, combined, re.IGNORECASE):
            print(f"  \u274c Rejected (belongs in other table - matched '{keyword}'): {code_raw}")
            rejected = True
            break
    
    if not rejected:
        filtered_rows.append(row)

# --- STEP 2: Normalize remaining rows ---
validated_rows = []
for row in filtered_rows:
    row["entity_id"] = entity_id
    row["policy_numbers"] = policy_number_extracted
    
    # Normalize special_condition_code
    code_raw = str(row.get("special_condition_code", "")).strip()
    code_lower = code_raw.lower()
    
    matched_code = None
    for key, val in KNOWN_CODES.items():
        if key in code_lower:
            matched_code = val
            break
    
    if matched_code:
        row["special_condition_code"] = matched_code
    else:
        # Create snake_case code from raw
        row["special_condition_code"] = re.sub(r'[^a-zA-Z0-9]+', '_', code_raw).strip('_')
    
    # Trim description
    desc = row.get("special_condition_description")
    if desc:
        desc_str = str(desc).strip()
        if desc_str.lower() in ["none", "null", ""]:
            row["special_condition_description"] = None
        else:
            row["special_condition_description"] = desc_str
    else:
        row["special_condition_description"] = None
    
    # Trim rule
    rule = row.get("special_condition_rule")
    if rule:
        rule_str = str(rule).strip()
        if rule_str.lower() in ["none", "null", ""]:
            row["special_condition_rule"] = None
        else:
            row["special_condition_rule"] = rule_str
    else:
        row["special_condition_rule"] = None
    
    # Only keep rows that have at least a code AND (description or rule)
    if row["special_condition_code"] and (row["special_condition_description"] or row["special_condition_rule"]):
        validated_rows.append(row)
    else:
        print(f"  \u26a0\ufe0f Skipped (no description or rule): {row['special_condition_code']}")

# --- STEP 3: Deduplicate by code+description ---
seen = set()
final_rows = []
for row in validated_rows:
    key = (row["special_condition_code"], row.get("special_condition_description"))
    if key not in seen:
        seen.add(key)
        final_rows.append(row)

sc_rows = final_rows

print(f"\n--- Final: {len(sc_rows)} special condition rows ---")
if len(sc_rows) == 0:
    print("  \u2705 No special conditions found (all extracted items belong in other tables or policy has none)")
for i, row in enumerate(sc_rows):
    print(f"  \u2705 Row {i+1}: {row['special_condition_code']} | {row.get('special_condition_description')} | rule={row.get('special_condition_rule')}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *

schema = StructType([
    StructField("entity_id", LongType(), True),
    StructField("policy_numbers", StringType(), True),
    StructField("special_condition_code", StringType(), True),
    StructField("special_condition_description", StringType(), True),
    StructField("special_condition_rule", StringType(), True)
])

# Convert rows to proper types
formatted_rows = []
for row in sc_rows:
    formatted = {
        "entity_id": entity_id,
        "policy_numbers": row.get("policy_numbers") or policy_number_extracted,
        "special_condition_code": row.get("special_condition_code"),
        "special_condition_description": row.get("special_condition_description"),
        "special_condition_rule": row.get("special_condition_rule")
    }
    formatted_rows.append(formatted)

# Create DataFrame
if formatted_rows:
    special_condition_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    special_condition_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No special condition data found - empty DataFrame created")

display(special_condition_df)
print(f"\n\u2705 GMC Special Condition extraction complete! ({special_condition_df.count()} rows)")
