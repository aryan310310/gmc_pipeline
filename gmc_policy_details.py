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

# DBTITLE 1,Rules
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
- If a benefit is NOT mentioned at all in the document → set _covered = false and all related fields to null.
- If a benefit is listed under "Exclusions" or "Not Covered" → set _covered = false and all related fields to null.
- Do NOT infer coverage from general statements or other benefits.
- Do NOT copy values from one benefit to another.

Rules:
- covered/included/applicable = true
- excluded/not covered/not applicable = false
- amount fields should contain numbers only
- percentages should contain numeric values only
- if field missing return null
- return booleans as true/false only

IMPORTANT - FAMILY DEFINITION rules:
- family_def: Extract the FULL text describing who is covered under the policy.
- Look for wordings like: "Self + Spouse + Children + Parents", "Employee + Spouse + 2 Children + 2 Parents", "1+5", "Self, Spouse, Kids and Parents", "Employee and dependents" etc.
- ALWAYS extract this field. Search the ENTIRE document including headers, schedule pages, member details, and coverage summaries.
- Common locations: Policy Schedule, Coverage Details, Member Eligibility section, Family Floater definition.
- If the document mentions "Self + Spouse + Children + Parents" or similar → extract that full text.
- If you see a floater structure like "1+5" or "1+3" → extract it.
- Do NOT return null for family_def unless truly no family information exists anywhere in the document.

- CRITICAL: ALWAYS include the EXACT number of children stated in the document.
  For example: if document says "6 dependent children" → write "Employee + Spouse + 6 dependent children" NOT just "Employee + Spouse + Dependent Children".
  The exact child count is essential. Never omit it.
- If you see a notation like "1+7" or "(1+7)" → ALSO extract it alongside the full text.

IMPORTANT - PARTNER_COVERED rules:
- partner_covered means coverage for "domestic partner" or "live-in partner" (NOT spouse).
- Set partner_covered = true ONLY if the document EXPLICITLY mentions "partner", "live-in partner", or "domestic partner" as a covered relationship.
- "Spouse" coverage does NOT make partner_covered = true. These are DIFFERENT concepts.
- If the document only mentions "Spouse" → partner_covered = false.
- Do NOT confuse company names containing "partner" with coverage for domestic partners.



IMPORTANT - CLAIM SETTLEMENT & DOCUMENT SUBMISSION DAYS rules:
- claim_settlement_days: Number of days the insurer has to settle/process a claim after receiving all documents.
  Look for phrases like: "claim settlement", "claim processing", "settlement of claim", "claim submission", "claim intimation", "intimation of claim within X days", "claims to be submitted within X days".
- document_submission_days: Number of days within which the insured must submit documents/bills after discharge.
  Look for phrases like: "document submission", "submission of documents", "documents to be submitted within X days", "bills submission", "discharge documents within X days", "intimation within X days of discharge".
- Extract the NUMBER OF DAYS only.
- Do NOT confuse these with pre/post hospitalization days or midterm addition days.
- If neither is explicitly mentioned → set to null.

IMPORTANT - payment_type rules (GENERAL - for non-maternity benefits):
- If a specific rupee/currency amount is mentioned (e.g. Rs.1500, Rs.2000, Rs.5000, 3 Lakhs) → set to "Flat"
- If a percentage of Sum Insured is mentioned (e.g. 100% of SI, up to SI) → set to "Percent of SI"
- If the benefit says "as per actuals" or "full coverage" or "covered within SI" with no separate limit → set to "Percent of SI" and value to 100

IMPORTANT - MATERNITY SUB-BENEFITS payment_type rules (for pre_post_natal, well_baby, well_mother ONLY):
- These benefits are ALWAYS relative to the maternity limit, NOT the Sum Insured.
- payment_type for these fields can ONLY be: "Percent of Maternity" or "Flat"
- "Percent of SI" is FORBIDDEN for pre_post_natal, well_baby, and well_mother.
- If the document says "covered", "within maternity", "as per maternity limit", or does not specify a separate flat amount → set payment_type to "Percent of Maternity" and expense/value to 100.
- If a specific flat rupee amount is mentioned (e.g. Rs.5000, Rs.10000) → set payment_type to "Flat" and the EXACT rupee amount as expense.
- limit_type for these fields MUST always be "Within Maternity".

CRITICAL - "Flat" vs "Percent of Maternity" for maternity sub-benefits:
- "Percent of Maternity" with expense=100 means: covered up to 100% of the maternity limit (no separate cap)
- "Flat" with expense=5000 means: covered up to Rs.5000 specifically (a separate flat cap)
- When payment_type is "Flat", the expense MUST be the EXACT rupee amount from the document. Flat amounts are ALWAYS > 100 (typically Rs.5000, Rs.10000, Rs.25000, Rs.50000 etc.)
- expense=100 is ONLY valid when payment_type is "Percent of Maternity". NEVER set expense=100 when payment_type is "Flat".
- If you cannot find the exact flat amount in the document, set expense to null (do NOT guess or use 100).

IMPORTANT - limit_type STRICT ENUM (ONLY these values are allowed):
- "Within SI" → benefit is covered within the Sum Insured (no additional/separate limit)
- "Additional" → benefit is over and above / additional to Sum Insured
- "Within Maternity" → ONLY for maternity sub-benefits (pre_post_natal, well_baby, well_mother) if within maternity limit

FORBIDDEN limit_type values (NEVER use these):
- "Sub-limit" → NOT ALLOWED, use "Within SI" instead
- "Sublimit" → NOT ALLOWED
- "Sub limit" → NOT ALLOWED
- Any other value not in the list above → NOT ALLOWED

IMPORTANT - WELL BABY & WELL MOTHER rules:
- Set well_baby_covered or well_mother_covered to true ONLY if the document EXPLICITLY and SEPARATELY mentions "well baby", "new born baby expenses", "well mother", or "post-delivery mother care" as a distinct covered benefit.
- Do NOT infer well_baby or well_mother coverage from general maternity coverage.
- If the document only says "maternity covered" without separately mentioning well baby or well mother → set well_baby_covered = false and well_mother_covered = false.
- If maternity is covered but well_baby/well_mother are NOT separately mentioned → they are false with all related fields as null.
- Even when covered: limit_type MUST be "Within Maternity" and payment_type MUST be "Percent of Maternity" or "Flat".

IMPORTANT - PRE/POST NATAL rules:
- Set pre_post_natal_covered to true ONLY if the document EXPLICITLY mentions "pre-natal", "post-natal", "ante-natal", or "pre & post natal" expenses as covered.
- If covered: limit_type MUST be "Within Maternity", payment_type MUST be "Percent of Maternity" or "Flat".
- If a specific rupee amount is stated (e.g. "Rs.5000", "Rs.5,000", "5000/-", "5K") → payment_type = "Flat", expense = that exact amount (e.g. 5000).
- If no separate amount is stated and it just says "covered" or "within maternity" → payment_type = "Percent of Maternity", expense = 100.
- NEVER set expense=100 when payment_type is "Flat". If payment_type is "Flat", expense must be the actual rupee amount (always > 100).
- Do NOT infer pre/post natal coverage from general maternity coverage alone.

IMPORTANT - PSYCHIATRIC AILMENTS rules:
- Set psychiatric_ailments_opd_covered or psychiatric_ailments_ipd_covered to true ONLY if the document EXPLICITLY mentions "psychiatric", "mental illness", "mental health", or "psychological" coverage with a specific amount or coverage statement.
- Do NOT assume psychiatric coverage from general "all diseases covered" or similar broad statements.
- If psychiatric ailments are not explicitly mentioned with specific terms → set both _covered = false and all related fields to null.
- Do NOT invent or assume any amount (like 30000) for psychiatric coverage.

IMPORTANT - CONGENITAL CONDITIONS rules:
- Set internal_congenital_covered or external_congenital_covered to true ONLY if the document EXPLICITLY mentions "congenital" conditions with clear coverage language AND specifies how they are covered (limit type, amount, etc.).
- If document says "congenital diseases excluded" or lists them under exclusions → set to false.
- If congenital conditions are NOT mentioned at all → set to false with all related fields as null.
- Do NOT infer congenital coverage from general statements.
- CRITICAL for external_congenital: set to true ONLY if the document EXPLICITLY mentions "external congenital" with specific coverage details. A general mention of "congenital" alone is NOT enough for external_congenital_covered=true.
- external_congenital_only_life_threatening = true ONLY if document explicitly says "only life-threatening" or "life-threatening only" for external congenital.

IMPORTANT - MIDTERM ADDITION rules:
- midterm_addition_spouse_limit_days: Number of days within which a newly married spouse can be added mid-term to the policy.
- midterm_addition_child_limit_days: Number of days within which a newborn baby/child can be added mid-term to the policy.
- Set these fields to a numeric value ONLY if the document EXPLICITLY contains phrases like:
  "mid-term inclusion", "mid term addition", "midterm addition", "midterm enrollment",
  "addition of spouse", "addition of newborn", "newborn baby inclusion",
  "mid-term entry", "addition of new member"
  followed by a specific number of days.
- Do NOT confuse "pre-hospitalization 30 days" or "post-hospitalization 60 days" or "waiting period" or "moratorium" or "claim intimation days" with midterm addition days. These are COMPLETELY DIFFERENT concepts.
- Do NOT invent or assume any number of days (like 30, 45, 60, 90) for midterm addition.
- If the document does NOT explicitly mention mid-term addition/inclusion of spouse or child with specific day counts → set BOTH fields to null.
- If the document mentions SEPARATE days for spouse and child, extract each separately.
- If only ONE combined value is given (e.g. "mid-term inclusion: within 30 days"), put that SAME value in BOTH fields.

IMPORTANT - payment_type and limit_type combined rules:
- For EVERY benefit where _covered = true, you MUST also determine payment_type and limit_type.
- If a benefit is NOT covered (false), set ALL its related payment_type, limit_type, and value fields to null.

  Examples:
  - "Ambulance charges: Rs.1500 per hospitalization" → payment_type: "Flat", ambulance_charge: 1500
  - "Day care procedures: Covered" (no separate sub-limit) → payment_type: "Percent of SI", limit_type: "Within SI", value: 100
  - "AYUSH: Up to Rs.3,00,000" → payment_type: "Flat", limit_type: "Within SI", value: 300000
  - "Organ donor: Covered within SI" → payment_type: "Percent of SI", limit_type: "Within SI", value: 100
  - "Pre-hospitalization: 30 days, covered" → payment_type: "Percent of SI", limit_type: "Within SI", value: 100, day_limit: 30
  - "Pre & Post natal: Covered within maternity" → payment_type: "Percent of Maternity", limit_type: "Within Maternity", expense: 100
  - "Pre & Post natal: Rs.5000" → payment_type: "Flat", limit_type: "Within Maternity", expense: 5000
  - "Pre & Post natal: Rs.10,000/-" → payment_type: "Flat", limit_type: "Within Maternity", expense: 10000
  - "Well baby: Covered" → payment_type: "Percent of Maternity", limit_type: "Within Maternity", expense: 100
  - "Mid-term inclusion within 30 days" → midterm_addition_spouse_limit_days: 30, midterm_addition_child_limit_days: 30
  - "Spouse addition: 30 days, Newborn addition: 90 days" → midterm_addition_spouse_limit_days: 30, midterm_addition_child_limit_days: 90
  - Document has NO mention of midterm/mid-term addition → midterm_addition_spouse_limit_days: null, midterm_addition_child_limit_days: null
  - "Claim settlement within 30 days" → claim_settlement_days: 30
  - "Documents to be submitted within 15 days of discharge" → document_submission_days: 15

IMPORTANT - LASIK/Refractive Surgery:
- Search ALL sections of the document including "Other Terms & Conditions", "Special Conditions", "Annexures", and general clauses.
- Wordings like "Lasik surgery (+/-X) to be covered" or "LASIK covered" or "refractive surgery covered" means lasik_surgery_covered = true.
- Extract the diopter range from the +/- value (e.g. "+/-5" means lasik_surgery_range = "+-5", "+/-7.5" means "+-7.5").
- If LASIK is covered but no explicit sub-limit is mentioned, set payment_type to "Percent of SI", limit_type to "Within SI", value to 100.

Extract ALL fields exactly as below:

{
  "entity_id": null,
  "policy_number": null,
  "family_def": null,
  "family_size": null,
  "partner_covered": null,
  "LGBTQ_partner_covered": null,
  "parents_covered": null,
  "cross_combi_parents": null,
  "cross_combi_parents_single_set": null,
  "maternity_coverage": null,
  "maternity_child_limit": null,
  "baby_day_one_coverage": null,
  "pre_post_natal_covered": null,
  "pre_post_natal_payment_type": null,
  "pre_post_natal_limit_type": null,
  "pre_post_natal_expense": null,
  "well_baby_covered": null,
  "well_baby_payment_type": null,
  "well_baby_limit_type": null,
  "well_baby_expense": null,
  "well_mother_covered": null,
  "well_mother_payment_type": null,
  "well_mother_limit_type": null,
  "well_mother_expense": null,
  "infertility_covered": null,
  "infertility_payment_type": null,
  "infertility_limit_type": null,
  "infertility_expense": null,
  "surrogacy_covered": null,
  "surrogacy_payment_type": null,
  "surrogacy_limit_type": null,
  "surrogacy_expense": null,
  "corporate_buffer_maintained": null,
  "corporate_buffer_per_policy": null,
  "corporate_buffer_per_family": null,
  "ambulance_charges_covered": null,
  "ambulance_payment_type": null,
  "ambulance_charge": null,
  "ambulance_charge_intercity": null,
  "air_ambulance_covered": null,
  "air_ambulance_payment_type": null,
  "air_ambulance_charge": null,
  "cardiac_ambulance_charges_covered": null,
  "cardiac_ambulance_payment_type": null,
  "cardiac_ambulance_charge": null,
  "day_care_procedure_covered": null,
  "day_care_procedure_payment_type": null,
  "day_care_procedure_limit_type": null,
  "day_care_procedure_value": null,
  "pre_hospitalization_covered": null,
  "pre_hospitalization_day_limit": null,
  "pre_hospitalization_limit_type": null,
  "pre_hospitalization_payment_type": null,
  "pre_hospitalization_value": null,
  "post_hospitalization_covered": null,
  "post_hospitalization_day_limit": null,
  "post_hospitalization_limit_type": null,
  "post_hospitalization_payment_type": null,
  "post_hospitalization_value": null,
  "lasik_surgery_covered": null,
  "lasik_surgery_range": null,
  "lasik_surgery_limit_type": null,
  "lasik_surgery_payment_type": null,
  "lasik_surgery_value": null,
  "ayush_treatment_covered": null,
  "ayush_treatment_limit_type": null,
  "ayush_treatment_payment_type": null,
  "ayush_treatment_value": null,
  "domiciliary_expense_covered": null,
  "domiciliary_expense_day_limit": null,
  "domiciliary_expense_limit_type": null,
  "domiciliary_expense_payment_type": null,
  "domiciliary_expense_value": null,
  "psychiatric_ailments_opd_covered": null,
  "psychiatric_ailments_ipd_covered": null,
  "psychiatric_ailments_limit_type": null,
  "psychiatric_ailments_payment_type": null,
  "psychiatric_ailments_opd_value": null,
  "psychiatric_ailments_ipd_value": null,
  "organ_donor_covered": null,
  "organ_donor_limit_type": null,
  "organ_donor_payment_type": null,
  "organ_donor_value": null,
  "internal_congenital_covered": null,
  "internal_congenital_limit_type": null,
  "internal_congenital_payment_type": null,
  "internal_congenital_value": null,
  "external_congenital_covered": null,
  "external_congenital_only_life_threatening": null,
  "external_congenital_limit_type": null,
  "external_congenital_payment_type": null,
  "external_congenital_value": null,
  "terrorism_coverage": null,
  "dental_accidental_cover": null,
  "claim_settlement_days": null,
  "document_submission_days": null,
  "automatic_SI_reinstatement": null,
  "automatic_SI_reinstatement_cycle": null,
  "automatic_sum_insured_reinstatement_percent": null,
  "midterm_addition_spouse_limit_days": null,
  "midterm_addition_child_limit_days": null
}
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
# Extract policy coverage data using the shared LLM function
data = extract_with_llm(prompt, text)

if data:
    print(f"Extracted {len(data)} fields")
else:
    raise ValueError("LLM extraction failed - no valid JSON returned")

# COMMAND ----------

# DBTITLE 1,Post-extraction validation and enum enforcement
# ========== POST-EXTRACTION VALIDATION ==========
# This cell enforces strict enum values and nullifies hallucinated data
# Run this AFTER json parsing (Cell 10/11) and BEFORE creating the DataFrame

import re as _re

ALLOWED_LIMIT_TYPES = {"Within SI", "Additional", "Within Maternity"}
ALLOWED_PAYMENT_TYPES = {"Flat", "Percent of SI", "Percent of Maternity"}

# Maternity sub-benefits can ONLY have these payment types
MATERNITY_SUB_BENEFIT_PAYMENT_TYPES = {"Flat", "Percent of Maternity"}
MATERNITY_SUB_BENEFIT_FIELDS = {
    "pre_post_natal_payment_type",
    "well_baby_payment_type",
    "well_mother_payment_type"
}

# Define benefit groups: (covered_key, [related_fields])
BENEFIT_GROUPS = {
    "ambulance": {
        "covered_key": "ambulance_charges_covered",
        "fields": ["ambulance_payment_type", "ambulance_charge", "ambulance_charge_intercity"]
    },
    "air_ambulance": {
        "covered_key": "air_ambulance_covered",
        "fields": ["air_ambulance_payment_type", "air_ambulance_charge"]
    },
    "cardiac_ambulance": {
        "covered_key": "cardiac_ambulance_charges_covered",
        "fields": ["cardiac_ambulance_payment_type", "cardiac_ambulance_charge"]
    },
    "day_care": {
        "covered_key": "day_care_procedure_covered",
        "fields": ["day_care_procedure_payment_type", "day_care_procedure_limit_type", "day_care_procedure_value"]
    },
    "pre_hosp": {
        "covered_key": "pre_hospitalization_covered",
        "fields": ["pre_hospitalization_day_limit", "pre_hospitalization_limit_type", "pre_hospitalization_payment_type", "pre_hospitalization_value"]
    },
    "post_hosp": {
        "covered_key": "post_hospitalization_covered",
        "fields": ["post_hospitalization_day_limit", "post_hospitalization_limit_type", "post_hospitalization_payment_type", "post_hospitalization_value"]
    },
    "lasik": {
        "covered_key": "lasik_surgery_covered",
        "fields": ["lasik_surgery_range", "lasik_surgery_limit_type", "lasik_surgery_payment_type", "lasik_surgery_value"]
    },
    "ayush": {
        "covered_key": "ayush_treatment_covered",
        "fields": ["ayush_treatment_limit_type", "ayush_treatment_payment_type", "ayush_treatment_value"]
    },
    "domiciliary": {
        "covered_key": "domiciliary_expense_covered",
        "fields": ["domiciliary_expense_day_limit", "domiciliary_expense_limit_type", "domiciliary_expense_payment_type", "domiciliary_expense_value"]
    },
    "psychiatric_opd": {
        "covered_key": "psychiatric_ailments_opd_covered",
        "fields": ["psychiatric_ailments_limit_type", "psychiatric_ailments_payment_type", "psychiatric_ailments_opd_value"]
    },
    "psychiatric_ipd": {
        "covered_key": "psychiatric_ailments_ipd_covered",
        "fields": ["psychiatric_ailments_limit_type", "psychiatric_ailments_payment_type", "psychiatric_ailments_ipd_value"]
    },
    "organ_donor": {
        "covered_key": "organ_donor_covered",
        "fields": ["organ_donor_limit_type", "organ_donor_payment_type", "organ_donor_value"]
    },
    "external_congenital": {
        "covered_key": "external_congenital_covered",
        "fields": ["external_congenital_only_life_threatening", "external_congenital_limit_type", "external_congenital_payment_type", "external_congenital_value"]
    },
    "infertility": {
        "covered_key": "infertility_covered",
        "fields": ["infertility_payment_type", "infertility_limit_type", "infertility_expense"]
    },
    "surrogacy": {
        "covered_key": "surrogacy_covered",
        "fields": ["surrogacy_payment_type", "surrogacy_limit_type", "surrogacy_expense"]
    },
    "pre_post_natal": {
        "covered_key": "pre_post_natal_covered",
        "fields": ["pre_post_natal_payment_type", "pre_post_natal_limit_type", "pre_post_natal_expense"]
    },
    "well_baby": {
        "covered_key": "well_baby_covered",
        "fields": ["well_baby_payment_type", "well_baby_limit_type", "well_baby_expense"]
    },
    "well_mother": {
        "covered_key": "well_mother_covered",
        "fields": ["well_mother_payment_type", "well_mother_limit_type", "well_mother_expense"]
    },
}

# All boolean _covered fields in the schema
ALL_COVERED_FIELDS = [
    "partner_covered", "LGBTQ_partner_covered", "parents_covered",
    "cross_combi_parents", "cross_combi_parents_single_set",
    "maternity_coverage", "baby_day_one_coverage",
    "pre_post_natal_covered", "well_baby_covered", "well_mother_covered",
    "infertility_covered", "surrogacy_covered", "corporate_buffer_maintained",
    "ambulance_charges_covered", "air_ambulance_covered",
    "cardiac_ambulance_charges_covered", "day_care_procedure_covered",
    "pre_hospitalization_covered", "post_hospitalization_covered",
    "lasik_surgery_covered", "ayush_treatment_covered",
    "domiciliary_expense_covered", "psychiatric_ailments_opd_covered",
    "psychiatric_ailments_ipd_covered", "organ_donor_covered",
    "internal_congenital_covered", "external_congenital_covered",
    "external_congenital_only_life_threatening",
    "terrorism_coverage", "dental_accidental_cover",
    "automatic_SI_reinstatement"
]

# Maternity sub-benefit expense fields and their payment_type counterparts
MATERNITY_EXPENSE_FIELDS = {
    "pre_post_natal_expense": "pre_post_natal_payment_type",
    "well_baby_expense": "well_baby_payment_type",
    "well_mother_expense": "well_mother_payment_type",
}

# ===== PDF TEXT VERIFICATION PATTERNS =====

MIDTERM_KEYWORDS_PATTERN = _re.compile(
    r'mid[\s\-]?term\s*(inclusion|addition|entr|enrol)',
    _re.IGNORECASE
)

CLAIM_SETTLEMENT_KEYWORDS = [
    r'claim\s*settlement',
    r'claim\s*processing',
    r'settle.*claim',
    r'settlement\s*of\s*claim',
    r'claim\s*submission',
    r'claim\s*intimation',
]

DOCUMENT_SUBMISSION_KEYWORDS = [
    r'document\s*submission',
    r'submission\s*of\s*document',
    r'bills?\s*submission',
    r'discharge.*document.*within',
    r'document.*intimat',
]

WELL_BABY_KEYWORDS_PATTERN = _re.compile(
    r'well\s*baby|new\s*born\s*baby\s*(expense|cover|benefit|charge)|neo\s*natal\s*(expense|cover|benefit)|baby\s*care\s*(expense|cover|benefit)',
    _re.IGNORECASE
)

WELL_MOTHER_KEYWORDS_PATTERN = _re.compile(
    r'well\s*mother|mother\s*(expense|cover|benefit|charge)|post\s*natal\s*mother\s*(expense|cover|benefit)',
    _re.IGNORECASE
)

# INFERTILITY: keywords that indicate coverage
INFERTILITY_KEYWORDS_PATTERN = _re.compile(
    r'infertility|in[\s\-]?vitro|\bIVF\b|fertility\s*treatment',
    _re.IGNORECASE
)

# EXTERNAL CONGENITAL + LIFE THREATENING
EXTERNAL_CONGENITAL_PATTERN = _re.compile(
    r'external\s*congenital|congenital\s*external|congenital.*(?:external|anomal)',
    _re.IGNORECASE
)
LIFE_THREATENING_PATTERN = _re.compile(
    r'life\s*threaten|life[\s\-]?threatening',
    _re.IGNORECASE
)

# AUTOMATIC REINSTATEMENT CYCLE
REINSTATEMENT_CYCLE_PATTERN = _re.compile(
    r'reinstat.*?(\d+)\s*(?:days|day)|(?:(\d+)\s*(?:days|day)).*?reinstat',
    _re.IGNORECASE
)


def _value_near_keywords(pdf_text, keywords_list, value, window=300):
    """Check if the extracted numeric value appears within window characters of any keyword."""
    if not pdf_text or value is None:
        return False
    value_str = str(int(value))
    for kw_pattern in keywords_list:
        for m in _re.finditer(kw_pattern, pdf_text, _re.IGNORECASE):
            start = max(0, m.start() - window)
            end = min(len(pdf_text), m.end() + window)
            context = pdf_text[start:end]
            number_pattern = _re.compile(r'(?<!\d)' + value_str + r'(?!\d)')
            if number_pattern.search(context):
                return True
    return False


def _partner_in_coverage_context(pdf_text):
    """Check if 'partner' is EXPLICITLY mentioned as a covered relationship.
    
    EXCLUDES:
    - 'Spouse/Partner' (just a label for spouse)
    - 'Partner Details/Name/Code' (broker/company info)
    - Company names containing 'partner'
    
    INCLUDES:
    - 'live-in partner', 'domestic partner'
    - 'partner' standalone in coverage context (not combined with spouse)
    """
    if not pdf_text:
        return False
    
    # First check for definitive patterns that ALWAYS indicate partner coverage
    definitive_patterns = [
        r'live[\s\-]?in\s*partner',
        r'domestic\s*partner',
        r'partner\s*(?:is\s*)?cover',
        r'(?:coverage|covered)\s+(?:for\s+)?(?:.*?\s+)?partner',
    ]
    for pat in definitive_patterns:
        if _re.search(pat, pdf_text, _re.IGNORECASE):
            return True
    
    # Find all "partner" occurrences and check context
    for m in _re.finditer(r'\bpartner\b', pdf_text, _re.IGNORECASE):
        near_context = pdf_text[max(0, m.start()-40):min(len(pdf_text), m.end()+40)].lower()
        
        # SKIP: "Spouse/Partner" - this is just a label for spouse, NOT partner coverage
        if _re.search(r'spouse\s*/\s*partner|spouse/partner', near_context):
            continue
        
        # SKIP: Company/broker context (Partner Details, Partner Name, Partner Code)
        if _re.search(r'partner\s*(detail|name|code|id|number)|partner\s*\n\s*(name|code|detail)', near_context):
            continue
        
        # SKIP: Company names (partners, partnership, pvt, ltd, etc.)
        if _re.search(r'partners(?:hip)?|pvt|ltd|limited|llp|inc|corp|company|firm|private', near_context):
            continue
        
        # If we get here, "partner" is mentioned without exclusions - check broader context
        broad_context = pdf_text[max(0, m.start()-200):min(len(pdf_text), m.end()+200)].lower()
        coverage_words = _re.compile(r'cover|member|eligible|insured|dependent|includ|family|floater')
        if coverage_words.search(broad_context):
            return True
    
    return False


def validate_and_fix(data, pdf_text=""):
    fixes_applied = []
    
    # 0. UNIVERSAL RULE: Convert all null _covered booleans to false
    for field in ALL_COVERED_FIELDS:
        if field in data and data[field] is None:
            data[field] = False
            fixes_applied.append(f"  FIXED: {field} was null -> false")
    
    # 1. UNIVERSAL RULE: internal_congenital is ALWAYS covered
    data["internal_congenital_covered"] = True
    data["internal_congenital_limit_type"] = "Within SI"
    data["internal_congenital_payment_type"] = "Percent of SI"
    data["internal_congenital_value"] = 100
    fixes_applied.append(f"  APPLIED: internal_congenital -> covered=true, Within SI, Percent of SI, 100 (universal rule)")
    
    # 2. EXTERNAL CONGENITAL: check PDF for "external congenital" + "life threatening"
    if pdf_text:
        has_ext_congenital = bool(EXTERNAL_CONGENITAL_PATTERN.search(pdf_text))
        has_life_threatening = bool(LIFE_THREATENING_PATTERN.search(pdf_text))
        
        if has_ext_congenital and has_life_threatening:
            if data.get("external_congenital_covered") != True:
                fixes_applied.append(f"  FIXED: external_congenital_covered -> true (PDF has 'external congenital' + 'life threatening')")
            data["external_congenital_covered"] = True
            data["external_congenital_only_life_threatening"] = True
            if data.get("external_congenital_limit_type") is None:
                data["external_congenital_limit_type"] = "Within SI"
            if data.get("external_congenital_payment_type") is None:
                data["external_congenital_payment_type"] = "Percent of SI"
            if data.get("external_congenital_value") is None:
                data["external_congenital_value"] = 100
        else:
            if data.get("external_congenital_covered") != False:
                fixes_applied.append(f"  FIXED: external_congenital_covered -> false (keywords not found in PDF)")
            data["external_congenital_covered"] = False
            data["external_congenital_only_life_threatening"] = False
            data["external_congenital_limit_type"] = None
            data["external_congenital_payment_type"] = None
            data["external_congenital_value"] = None
    
    # 3. PARTNER_COVERED: ONLY true if "partner" explicitly in COVERAGE context
    #    Excludes: "Spouse/Partner" labels, "Partner Details" broker info, company names
    if pdf_text:
        if data.get("partner_covered") == True:
            if not _partner_in_coverage_context(pdf_text):
                fixes_applied.append(f"  FIXED: partner_covered was True but 'partner' NOT in coverage context in PDF -> false")
                data["partner_covered"] = False
    
    # 4. INFERTILITY: if keyword found in PDF (not under exclusions), force covered=true
    if pdf_text and INFERTILITY_KEYWORDS_PATTERN.search(pdf_text):
        infertility_context = ""
        for m in _re.finditer(INFERTILITY_KEYWORDS_PATTERN, pdf_text):
            start = max(0, m.start() - 200)
            end = min(len(pdf_text), m.end() + 200)
            infertility_context += pdf_text[start:end].lower()
        
        is_excluded = bool(_re.search(r'not\s*cover|excluded|exclusion|waiting\s*period.*infertility', infertility_context))
        is_covered = bool(_re.search(r'cover|included|payable|applicable|within\s*si|percent', infertility_context))
        
        if is_covered and not is_excluded:
            if data.get("infertility_covered") != True:
                fixes_applied.append(f"  FIXED: infertility_covered was {data.get('infertility_covered')} but 'infertility' found as COVERED in PDF -> true")
                data["infertility_covered"] = True
    
    # 5. AUTOMATIC REINSTATEMENT CYCLE: if true, extract cycle days from PDF
    if data.get("automatic_SI_reinstatement") == True and pdf_text:
        cycle_match = REINSTATEMENT_CYCLE_PATTERN.search(pdf_text)
        if cycle_match:
            days = cycle_match.group(1) or cycle_match.group(2)
            if days and data.get("automatic_SI_reinstatement_cycle") is None:
                data["automatic_SI_reinstatement_cycle"] = int(days)
                fixes_applied.append(f"  FIXED: automatic_SI_reinstatement_cycle was null -> {days} (found near 'reinstatement' in PDF)")
    
    # 6. Fix invalid limit_type values
    limit_type_fields = [k for k in data.keys() if k.endswith("_limit_type")]
    for field in limit_type_fields:
        if data[field] is not None and data[field] not in ALLOWED_LIMIT_TYPES:
            fixes_applied.append(f"  FIXED: {field} was '{data[field]}' -> 'Within SI'")
            data[field] = "Within SI"
    
    # 7. Fix invalid payment_type values (general)
    payment_type_fields = [k for k in data.keys() if k.endswith("_payment_type")]
    for field in payment_type_fields:
        if data[field] is not None and data[field] not in ALLOWED_PAYMENT_TYPES:
            fixes_applied.append(f"  FIXED: {field} was '{data[field]}' -> null")
            data[field] = None
    
    # 8. MATERNITY SUB-BENEFITS: force correct payment_type and limit_type
    for field in MATERNITY_SUB_BENEFIT_FIELDS:
        if data.get(field) is not None and data[field] not in MATERNITY_SUB_BENEFIT_PAYMENT_TYPES:
            fixes_applied.append(f"  FIXED: {field} was '{data[field]}' -> 'Percent of Maternity' (maternity sub-benefit)")
            data[field] = "Percent of Maternity"
    
    for field in ["pre_post_natal_limit_type", "well_baby_limit_type", "well_mother_limit_type"]:
        if data.get(field) is not None and data[field] != "Within Maternity":
            fixes_applied.append(f"  FIXED: {field} was '{data[field]}' -> 'Within Maternity' (maternity sub-benefit)")
            data[field] = "Within Maternity"
    
    # 9. MATERNITY SUB-BENEFIT EXPENSE SANITY CHECK
    for expense_field, pt_field in MATERNITY_EXPENSE_FIELDS.items():
        pt_val = data.get(pt_field)
        exp_val = data.get(expense_field)
        if pt_val == "Flat" and exp_val is not None:
            try:
                if float(exp_val) <= 100:
                    fixes_applied.append(f"  FIXED: {expense_field} was {exp_val} with payment_type='Flat' -> null")
                    data[expense_field] = None
            except (ValueError, TypeError):
                pass
        elif pt_val == "Percent of Maternity" and exp_val is None:
            data[expense_field] = 100
            fixes_applied.append(f"  FIXED: {expense_field} was null with payment_type='Percent of Maternity' -> 100")
    
    # 10. Nullify all related fields when covered = false
    for group_name, group_info in BENEFIT_GROUPS.items():
        covered_key = group_info["covered_key"]
        if covered_key in data and data[covered_key] == False:
            for field in group_info["fields"]:
                if field in data and data[field] is not None:
                    fixes_applied.append(f"  FIXED: {field} was '{data[field]}' but {covered_key}=false -> null")
                    data[field] = None
    
    # 11. Special: if both psychiatric OPD and IPD are false, nullify shared fields
    if data.get("psychiatric_ailments_opd_covered") == False and data.get("psychiatric_ailments_ipd_covered") == False:
        for field in ["psychiatric_ailments_limit_type", "psychiatric_ailments_payment_type", 
                      "psychiatric_ailments_opd_value", "psychiatric_ailments_ipd_value"]:
            if data.get(field) is not None:
                fixes_applied.append(f"  FIXED: {field} was '{data[field]}' but both psychiatric covered=false -> null")
                data[field] = None
    
    # ===== PDF TEXT VERIFICATION CHECKS =====
    
    # 12. MIDTERM ADDITION
    spouse_days = data.get("midterm_addition_spouse_limit_days")
    child_days = data.get("midterm_addition_child_limit_days")
    if spouse_days is not None or child_days is not None:
        if pdf_text and not MIDTERM_KEYWORDS_PATTERN.search(pdf_text):
            if spouse_days is not None:
                fixes_applied.append(f"  FIXED: midterm_addition_spouse_limit_days was {spouse_days} -> null (keyword not in PDF)")
                data["midterm_addition_spouse_limit_days"] = None
            if child_days is not None:
                fixes_applied.append(f"  FIXED: midterm_addition_child_limit_days was {child_days} -> null (keyword not in PDF)")
                data["midterm_addition_child_limit_days"] = None
        else:
            if spouse_days is not None and not _value_near_keywords(pdf_text, [r'mid[\s\-]?term'], spouse_days, window=400):
                fixes_applied.append(f"  FIXED: midterm_addition_spouse_limit_days was {spouse_days} -> null (value not near keyword)")
                data["midterm_addition_spouse_limit_days"] = None
                data["midterm_addition_child_limit_days"] = None
            elif spouse_days is not None and child_days is None:
                data["midterm_addition_child_limit_days"] = spouse_days
                fixes_applied.append(f"  FIXED: midterm_addition_child_limit_days -> {spouse_days} (copied from spouse)")
            elif child_days is not None and spouse_days is None:
                data["midterm_addition_spouse_limit_days"] = child_days
                fixes_applied.append(f"  FIXED: midterm_addition_spouse_limit_days -> {child_days} (copied from child)")
    
    # 13. CLAIM SETTLEMENT DAYS
    claim_val = data.get("claim_settlement_days")
    if claim_val is not None and pdf_text:
        if not _value_near_keywords(pdf_text, CLAIM_SETTLEMENT_KEYWORDS, claim_val, window=300):
            fixes_applied.append(f"  FIXED: claim_settlement_days was {claim_val} -> null (value not near keyword)")
            data["claim_settlement_days"] = None
    
    # 14. DOCUMENT SUBMISSION DAYS
    doc_val = data.get("document_submission_days")
    if doc_val is not None and pdf_text:
        if not _value_near_keywords(pdf_text, DOCUMENT_SUBMISSION_KEYWORDS, doc_val, window=300):
            fixes_applied.append(f"  FIXED: document_submission_days was {doc_val} -> null (value not near keyword)")
            data["document_submission_days"] = None
    
    # 15. WELL BABY: keyword must exist in PDF
    if data.get("well_baby_covered") == True:
        if pdf_text and not WELL_BABY_KEYWORDS_PATTERN.search(pdf_text):
            fixes_applied.append(f"  FIXED: well_baby_covered -> false (keyword not in PDF)")
            data["well_baby_covered"] = False
            data["well_baby_payment_type"] = None
            data["well_baby_limit_type"] = None
            data["well_baby_expense"] = None
    
    # 16. WELL MOTHER: keyword must exist in PDF
    if data.get("well_mother_covered") == True:
        if pdf_text and not WELL_MOTHER_KEYWORDS_PATTERN.search(pdf_text):
            fixes_applied.append(f"  FIXED: well_mother_covered -> false (keyword not in PDF)")
            data["well_mother_covered"] = False
            data["well_mother_payment_type"] = None
            data["well_mother_limit_type"] = None
            data["well_mother_expense"] = None
    
    # Print validation report
    if fixes_applied:
        print(f"VALIDATION: {len(fixes_applied)} fix(es) applied:")
        for fix in fixes_applied:
            print(fix)
    else:
        print("VALIDATION: All values passed - no fixes needed.")
    
    return data

# Apply validation - pass the PDF text so we can verify keywords
data = validate_and_fix(data, pdf_text=text)
print("\nValidated output (key fields):")
print(f"  family_def: {data.get('family_def')}")
print(f"  external_congenital_covered: {data.get('external_congenital_covered')}")
print(f"  external_congenital_only_life_threatening: {data.get('external_congenital_only_life_threatening')}")
print(f"  internal_congenital_covered: {data.get('internal_congenital_covered')}")
print(f"  partner_covered: {data.get('partner_covered')}")
print(f"  infertility_covered: {data.get('infertility_covered')}")
print(f"  well_baby_covered: {data.get('well_baby_covered')}")
print(f"  well_mother_covered: {data.get('well_mother_covered')}")
print(f"  automatic_SI_reinstatement: {data.get('automatic_SI_reinstatement')}")
print(f"  automatic_SI_reinstatement_cycle: {data.get('automatic_SI_reinstatement_cycle')}")
print(f"  claim_settlement_days: {data.get('claim_settlement_days')}")
print(f"  document_submission_days: {data.get('document_submission_days')}")
print(f"  midterm_addition_spouse_limit_days: {data.get('midterm_addition_spouse_limit_days')}")
print(f"  midterm_addition_child_limit_days: {data.get('midterm_addition_child_limit_days')}")

# COMMAND ----------

# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
data["entity_id"] = entity_id

# COMMAND ----------

# DBTITLE 1,Boolean normalization
# Normalize all boolean fields using shared utility
boolean_columns = [
    "partner_covered",
    "LGBTQ_partner_covered",
    "parents_covered",
    "cross_combi_parents",
    "cross_combi_parents_single_set",
    "maternity_coverage",
    "baby_day_one_coverage",
    "pre_post_natal_covered",
    "well_baby_covered",
    "well_mother_covered",
    "infertility_covered",
    "surrogacy_covered",
    "corporate_buffer_maintained",
    "ambulance_charges_covered",
    "air_ambulance_covered",
    "cardiac_ambulance_charges_covered",
    "day_care_procedure_covered",
    "pre_hospitalization_covered",
    "post_hospitalization_covered",
    "lasik_surgery_covered",
    "ayush_treatment_covered",
    "domiciliary_expense_covered",
    "psychiatric_ailments_opd_covered",
    "psychiatric_ailments_ipd_covered",
    "organ_donor_covered",
    "internal_congenital_covered",
    "external_congenital_covered",
    "external_congenital_only_life_threatening",
    "terrorism_coverage",
    "dental_accidental_cover",
    "automatic_SI_reinstatement"
]

data = apply_boolean_normalization(data, boolean_columns)

# COMMAND ----------

schema = StructType([
    StructField("entity_id", IntegerType(), True),
    StructField("policy_number", StringType(), True),
    StructField("family_def", StringType(), True),
    StructField("family_size", ShortType(), True),
    StructField("partner_covered", BooleanType(), True),
    StructField("LGBTQ_partner_covered", BooleanType(), True),
    StructField("parents_covered", BooleanType(), True),
    StructField("cross_combi_parents", BooleanType(), True),
    StructField("cross_combi_parents_single_set", BooleanType(), True),
    StructField("maternity_coverage", BooleanType(), True),
    StructField("maternity_child_limit", ByteType(), True),
    StructField("baby_day_one_coverage", BooleanType(), True),
    StructField("pre_post_natal_covered", BooleanType(), True),
    StructField("pre_post_natal_payment_type", StringType(), True),
    StructField("pre_post_natal_limit_type", StringType(), True),
    StructField("pre_post_natal_expense", DecimalType(8, 0), True),
    StructField("well_baby_covered", BooleanType(), True),
    StructField("well_baby_payment_type", StringType(), True),
    StructField("well_baby_limit_type", StringType(), True),
    StructField("well_baby_expense", DecimalType(8, 0), True),
    StructField("well_mother_covered", BooleanType(), True),
    StructField("well_mother_payment_type", StringType(), True),
    StructField("well_mother_limit_type", StringType(), True),
    StructField("well_mother_expense", DecimalType(8, 0), True),
    StructField("infertility_covered", BooleanType(), True),
    StructField("infertility_payment_type", StringType(), True),
    StructField("infertility_limit_type", StringType(), True),
    StructField("infertility_expense", DecimalType(8, 0), True),
    StructField("surrogacy_covered", BooleanType(), True),
    StructField("surrogacy_payment_type", StringType(), True),
    StructField("surrogacy_limit_type", StringType(), True),
    StructField("surrogacy_expense", DecimalType(8, 0), True),
    StructField("corporate_buffer_maintained", BooleanType(), True),
    StructField("corporate_buffer_per_policy", DecimalType(14, 2), True),
    StructField("corporate_buffer_per_family", StringType(), True),
    StructField("ambulance_charges_covered", BooleanType(), True),
    StructField("ambulance_payment_type", StringType(), True),
    StructField("ambulance_charge", DecimalType(8, 0), True),
    StructField("ambulance_charge_intercity", DecimalType(8, 0), True),
    StructField("air_ambulance_covered", BooleanType(), True),
    StructField("air_ambulance_payment_type", StringType(), True),
    StructField("air_ambulance_charge", DecimalType(8, 0), True),
    StructField("cardiac_ambulance_charges_covered", BooleanType(), True),
    StructField("cardiac_ambulance_payment_type", StringType(), True),
    StructField("cardiac_ambulance_charge", DecimalType(8, 0), True),
    StructField("day_care_procedure_covered", BooleanType(), True),
    StructField("day_care_procedure_payment_type", StringType(), True),
    StructField("day_care_procedure_limit_type", StringType(), True),
    StructField("day_care_procedure_value", DecimalType(8, 0), True),
    StructField("pre_hospitalization_covered", BooleanType(), True),
    StructField("pre_hospitalization_day_limit", ShortType(), True),
    StructField("pre_hospitalization_limit_type", StringType(), True),
    StructField("pre_hospitalization_payment_type", StringType(), True),
    StructField("pre_hospitalization_value", DecimalType(8, 0), True),
    StructField("post_hospitalization_covered", BooleanType(), True),
    StructField("post_hospitalization_day_limit", ShortType(), True),
    StructField("post_hospitalization_limit_type", StringType(), True),
    StructField("post_hospitalization_payment_type", StringType(), True),
    StructField("post_hospitalization_value", DecimalType(8, 0), True),
    StructField("lasik_surgery_covered", BooleanType(), True),
    StructField("lasik_surgery_range", StringType(), True),
    StructField("lasik_surgery_limit_type", StringType(), True),
    StructField("lasik_surgery_payment_type", StringType(), True),
    StructField("lasik_surgery_value", DecimalType(8, 0), True),
    StructField("ayush_treatment_covered", BooleanType(), True),
    StructField("ayush_treatment_limit_type", StringType(), True),
    StructField("ayush_treatment_payment_type", StringType(), True),
    StructField("ayush_treatment_value", DecimalType(8, 0), True),
    StructField("domiciliary_expense_covered", BooleanType(), True),
    StructField("domiciliary_expense_day_limit", ShortType(), True),
    StructField("domiciliary_expense_limit_type", StringType(), True),
    StructField("domiciliary_expense_payment_type", StringType(), True),
    StructField("domiciliary_expense_value", DecimalType(8, 0), True),
    StructField("psychiatric_ailments_opd_covered", BooleanType(), True),
    StructField("psychiatric_ailments_ipd_covered", BooleanType(), True),
    StructField("psychiatric_ailments_limit_type", StringType(), True),
    StructField("psychiatric_ailments_payment_type", StringType(), True),
    StructField("psychiatric_ailments_opd_value", DecimalType(8, 0), True),
    StructField("psychiatric_ailments_ipd_value", DecimalType(8, 0), True),
    StructField("organ_donor_covered", BooleanType(), True),
    StructField("organ_donor_limit_type", StringType(), True),
    StructField("organ_donor_payment_type", StringType(), True),
    StructField("organ_donor_value", DecimalType(8, 0), True),
    StructField("internal_congenital_covered", BooleanType(), True),
    StructField("internal_congenital_limit_type", StringType(), True),
    StructField("internal_congenital_payment_type", StringType(), True),
    StructField("internal_congenital_value", DecimalType(8, 0), True),
    StructField("external_congenital_covered", BooleanType(), True),
    StructField("external_congenital_only_life_threatening", BooleanType(), True),
    StructField("external_congenital_limit_type", StringType(), True),
    StructField("external_congenital_payment_type", StringType(), True),
    StructField("external_congenital_value", DecimalType(8, 0), True),
    StructField("terrorism_coverage", BooleanType(), True),
    StructField("dental_accidental_cover", BooleanType(), True),
    StructField("claim_settlement_days", ShortType(), True),
    StructField("document_submission_days", ShortType(), True),
    StructField("automatic_SI_reinstatement", BooleanType(), True),
    StructField("automatic_SI_reinstatement_cycle", ByteType(), True),
    StructField("automatic_sum_insured_reinstatement_percent", ByteType(), True),
    StructField("midterm_addition_spouse_limit_days", ShortType(), True),
    StructField("midterm_addition_child_limit_days", ShortType(), True)
])

# COMMAND ----------

# DBTITLE 1,Post-processing and DataFrame creation
import decimal
import re

# Only keep columns defined in the schema
schema_cols = [f.name for f in schema.fields]
filtered_data = {k: data.get(k) for k in schema_cols}

# --- Post-processing rules ---

# 1. Family definition: parse from LLM text + cross-check with booleans
# For child/parent counts, use LLM's family_def text.
# If children mentioned without explicit count, check PDF text near family section.
# Use 1+N notation from either source (unambiguous).

family_def_raw = (filtered_data.get("family_def") or "").lower()

def _extract_child_count_from_pdf_family_section(pdf_text):
    """Search for child count in the 'Max Members Per Family' / family composition section.
    
    PRIORITIZES:
    1. 'Members Allowed Per Family' table (most accurate for per-family count)
    2. 'Family Composition' section
    3. Explicit 'N children' in family context
    
    EXCLUDES:
    - 'Demographic Summary' / 'Lives Count' (these are TOTAL lives, not per-family)
    """
    if not pdf_text:
        return None
    
    search_area = pdf_text[:10000].lower()  # Extend to 10000 chars
    
    # PRIORITY 1: Find "Members Allowed Per Family" or similar section
    # Look for Child count specifically in this context
    per_family_patterns = [
        r'(?:member|members)\s*allowed\s*per\s*family.*?child(?:ren)?\s*\n?\s*(\d+)',
        r'per\s*family.*?child(?:ren)?\s*\n?\s*(\d+)',
        r'(?:max|maximum)\s*(?:no\.?\s*of\s*)?member.*?child(?:ren)?\s*\n?\s*(\d+)',
        r'family\s*composition.*?child(?:ren)?\s*\n?\s*(\d+)',
    ]
    
    for pat in per_family_patterns:
        m = re.search(pat, search_area, re.DOTALL)
        if m:
            val = int(m.group(1))
            if 1 <= val <= 20:
                return val
    
    # PRIORITY 2: "N dependent children" or "N children covered" (explicit count)
    explicit_patterns = [
        r'(\d+)\s*(?:dependent\s*)?(?:child|children|kids)\s*(?:covered|included|eligible|insured|allowed)',
        r'(?:upto|up\s*to|maximum|max)\s*(\d+)\s*(?:dependent\s*)?(?:child|children)',
        r'(?:self|employee|spouse).*?(\d+)\s*(?:dependent\s*)?(?:child|children)',
    ]
    
    for pat in explicit_patterns:
        matches = re.findall(pat, search_area)
        if matches:
            counts = [int(x) for x in matches if 1 <= int(x) <= 20]
            if counts:
                return max(counts)
    
    # PRIORITY 3: Table format "Child\n6" but ONLY if NOT in demographic/lives context
    # Find all "child\nN" patterns and check context
    for m in re.finditer(r'child(?:ren)?\s*\n\s*(\d+)', search_area):
        val = int(m.group(1))
        if not (1 <= val <= 20):
            continue
        # Check if this is in a "Lives Count" / "Demographic" section (EXCLUDE)
        context_before = search_area[max(0, m.start()-300):m.start()]
        if re.search(r'lives\s*count|demographic|total\s*lives|no\.?\s*of\s*lives', context_before):
            continue  # Skip - this is total lives, not per-family
        # Check if this is in a per-family section (INCLUDE)
        if re.search(r'per\s*family|allowed|member|composition|eligib|max', context_before):
            return val
    
    return None


def _infer_family_from_booleans(filtered_data):
    """Infer family_def and family_size from boolean coverage fields."""
    has_parents = filtered_data.get("parents_covered") == True
    has_spouse = filtered_data.get("partner_covered") == True
    if has_parents:
        filtered_data["family_def"] = "ESCP"
        filtered_data["family_size"] = 6  # 1+1+2+2
        print("  FAMILY SIZE: Inferred from booleans (parents_covered=true) -> ESCP, 6")
    elif has_spouse:
        filtered_data["family_def"] = "ESC"
        filtered_data["family_size"] = 4  # 1+1+2
        print("  FAMILY SIZE: Inferred from booleans (partner_covered=true) -> ESC, 4")
    else:
        filtered_data["family_def"] = "ESCP"
        filtered_data["family_size"] = 6
        print("  FAMILY SIZE: Fallback -> ESCP, 6")

# Check if the text has useful keywords
has_useful_keywords = bool(re.search(
    r'spouse|child|children|kids|kid|parent|\d\s*\+\s*\d',
    family_def_raw
))

if not family_def_raw.strip() or not has_useful_keywords:
    # Text is empty or useless -> infer from booleans
    _infer_family_from_booleans(filtered_data)
else:
    # --- Parse from LLM's family_def text ---
    
    # 1+N notation (most reliable)
    plus_match = re.search(r'(?:^|\D)(1)\s*\+\s*(\d+)', family_def_raw)
    plus_total = (1 + int(plus_match.group(2))) if plus_match else None
    
    # If no 1+N in LLM text, also check full PDF text for it (unambiguous pattern)
    if plus_total is None and 'text' in dir():
        pdf_plus = re.search(r'(?:family|floater|member|coverage|insured).*?(?:^|\D)(1)\s*\+\s*(\d+)', text[:5000], re.IGNORECASE | re.DOTALL)
        if pdf_plus:
            plus_total = 1 + int(pdf_plus.group(2))
            print(f"  FAMILY SIZE: Found 1+N notation in PDF text -> {plus_total}")
    
    # Children: find ALL matches in family_def text and take MAX
    child_matches = re.findall(r'(\d+)\s*(?:dependent\s*)?(?:child|children|kids|kid)', family_def_raw)
    num_children = max([int(x) for x in child_matches]) if child_matches else 0
    
    # If LLM text mentions children but NO count, try extracting from PDF family section
    if num_children == 0 and re.search(r'child|children|kids|kid', family_def_raw):
        pdf_child_count = _extract_child_count_from_pdf_family_section(text if 'text' in dir() else '')
        if pdf_child_count:
            num_children = pdf_child_count
            print(f"  FAMILY SIZE: LLM didn't include child count, found {num_children} in PDF family section")

    # Parents
    parent_match = re.search(r'(\d+)\s*(?:dependent\s*)?parent', family_def_raw)
    if parent_match:
        num_parents = int(parent_match.group(1))
    elif "one set of parent" in family_def_raw or "1 set of parent" in family_def_raw:
        num_parents = 2
    elif "parent" in family_def_raw:
        num_parents = 2
    else:
        num_parents = 0

    # Spouse
    has_spouse = "spouse" in family_def_raw or num_children > 0 or num_parents > 0
    has_children_mentioned = num_children > 0 or bool(re.search(r'child|children|kids|kid', family_def_raw))

    # Cross-check: if parents_covered=true but parsing missed parents
    if filtered_data.get("parents_covered") == True and num_parents == 0:
        num_parents = 2
    
    # Build abbreviation
    if num_parents > 0 or filtered_data.get("parents_covered") == True:
        filtered_data["family_def"] = "ESCP"
    elif has_children_mentioned:
        filtered_data["family_def"] = "ESC"
    elif has_spouse:
        filtered_data["family_def"] = "ES"
    else:
        filtered_data["family_def"] = "E"

    # Calculate family_size
    if plus_total is not None:
        # Use 1+N as the total BUT cross-check with component sum
        component_sum = 1 + (1 if has_spouse else 0) + num_children + num_parents
        # If component parsing gives a HIGHER value, components are more specific
        if component_sum > plus_total:
            filtered_data["family_size"] = component_sum
            print(f"  FAMILY SIZE: Components ({component_sum}) > 1+N ({plus_total}) -> 1+{1 if has_spouse else 0}+{num_children}+{num_parents}={component_sum}")
        else:
            filtered_data["family_size"] = plus_total
            print(f"  FAMILY SIZE: Using 1+N notation -> {plus_total}")
    else:
        # No 1+N, use parsed components
        if has_children_mentioned and num_children == 0:
            num_children = 2  # default if mentioned without count AND not found in PDF
        family_size = 1
        if has_spouse:
            family_size += 1
        family_size += num_children
        family_size += num_parents
        filtered_data["family_size"] = family_size
        print(f"  FAMILY SIZE: Parsed -> 1(self) + {1 if has_spouse else 0}(spouse) + {num_children}(children) + {num_parents}(parents) = {family_size}")

    # SAFETY NET
    if filtered_data["family_def"] == "E" and filtered_data.get("family_size", 0) <= 1:
        if filtered_data.get("parents_covered") == True or filtered_data.get("partner_covered") == True:
            print("  FAMILY SIZE: Parsing gave E but booleans contradict -> re-inferring")
            _infer_family_from_booleans(filtered_data)

# 2. maternity_child_limit
if filtered_data.get("maternity_coverage") == True:
    mcl = filtered_data.get("maternity_child_limit")
    if mcl is None or (isinstance(mcl, (int, float)) and (mcl > 127 or mcl < 1)):
        filtered_data["maternity_child_limit"] = 2

# 3. well_baby and well_mother
if filtered_data.get("well_baby_covered") == True:
    if not filtered_data.get("well_baby_payment_type"):
        filtered_data["well_baby_payment_type"] = "Percent of Maternity"
    if not filtered_data.get("well_baby_limit_type"):
        filtered_data["well_baby_limit_type"] = "Within Maternity"
    if filtered_data.get("well_baby_expense") is None and filtered_data.get("well_baby_payment_type") == "Percent of Maternity":
        filtered_data["well_baby_expense"] = 100

if filtered_data.get("well_mother_covered") == True:
    if not filtered_data.get("well_mother_payment_type"):
        filtered_data["well_mother_payment_type"] = "Percent of Maternity"
    if not filtered_data.get("well_mother_limit_type"):
        filtered_data["well_mother_limit_type"] = "Within Maternity"
    if filtered_data.get("well_mother_expense") is None and filtered_data.get("well_mother_payment_type") == "Percent of Maternity":
        filtered_data["well_mother_expense"] = 100

# 4. pre_post_natal
if filtered_data.get("pre_post_natal_covered") == True:
    if not filtered_data.get("pre_post_natal_payment_type"):
        filtered_data["pre_post_natal_payment_type"] = "Percent of Maternity"
    if not filtered_data.get("pre_post_natal_limit_type"):
        filtered_data["pre_post_natal_limit_type"] = "Within Maternity"
    if filtered_data.get("pre_post_natal_expense") is None and filtered_data.get("pre_post_natal_payment_type") == "Percent of Maternity":
        filtered_data["pre_post_natal_expense"] = 100

# 5. corporate_buffer_per_family
if filtered_data.get("corporate_buffer_maintained") == True:
    filtered_data["corporate_buffer_per_family"] = "FSI"

# 6. lasik_surgery
if filtered_data.get("lasik_surgery_covered") == True:
    if not filtered_data.get("lasik_surgery_range"):
        filtered_data["lasik_surgery_range"] = "+-7.5"
    if not filtered_data.get("lasik_surgery_limit_type"):
        filtered_data["lasik_surgery_limit_type"] = "Within SI"
    if not filtered_data.get("lasik_surgery_payment_type"):
        filtered_data["lasik_surgery_payment_type"] = "Percent of SI"
    if filtered_data.get("lasik_surgery_value") is None:
        filtered_data["lasik_surgery_value"] = 100

# 6a. domiciliary_expense
if filtered_data.get("domiciliary_expense_covered") == True:
    if not filtered_data.get("domiciliary_expense_limit_type"):
        filtered_data["domiciliary_expense_limit_type"] = "Within SI"
    if not filtered_data.get("domiciliary_expense_payment_type"):
        filtered_data["domiciliary_expense_payment_type"] = "Percent of SI"
    if filtered_data.get("domiciliary_expense_value") is None:
        filtered_data["domiciliary_expense_value"] = 100

# 6b. psychiatric_ailments
if filtered_data.get("psychiatric_ailments_opd_covered") == True or filtered_data.get("psychiatric_ailments_ipd_covered") == True:
    if not filtered_data.get("psychiatric_ailments_limit_type"):
        filtered_data["psychiatric_ailments_limit_type"] = "Within SI"
    if not filtered_data.get("psychiatric_ailments_payment_type"):
        filtered_data["psychiatric_ailments_payment_type"] = "Percent of SI"
    if filtered_data.get("psychiatric_ailments_opd_covered") == True and filtered_data.get("psychiatric_ailments_opd_value") is None:
        filtered_data["psychiatric_ailments_opd_value"] = 100
    if filtered_data.get("psychiatric_ailments_ipd_covered") == True and filtered_data.get("psychiatric_ailments_ipd_value") is None:
        filtered_data["psychiatric_ailments_ipd_value"] = 100

# 7. Fix payment_type: if value > 100 it's flat
payment_type_value_pairs = [
    ("ayush_treatment_payment_type", "ayush_treatment_value"),
    ("infertility_payment_type", "infertility_expense"),
    ("surrogacy_payment_type", "surrogacy_expense"),
    ("ambulance_payment_type", "ambulance_charge"),
    ("air_ambulance_payment_type", "air_ambulance_charge"),
    ("cardiac_ambulance_payment_type", "cardiac_ambulance_charge"),
    ("day_care_procedure_payment_type", "day_care_procedure_value"),
    ("pre_hospitalization_payment_type", "pre_hospitalization_value"),
    ("post_hospitalization_payment_type", "post_hospitalization_value"),
    ("lasik_surgery_payment_type", "lasik_surgery_value"),
    ("domiciliary_expense_payment_type", "domiciliary_expense_value"),
    ("psychiatric_ailments_payment_type", "psychiatric_ailments_opd_value"),
    ("organ_donor_payment_type", "organ_donor_value"),
    ("internal_congenital_payment_type", "internal_congenital_value"),
    ("external_congenital_payment_type", "external_congenital_value"),
    ("pre_post_natal_payment_type", "pre_post_natal_expense"),
    ("well_baby_payment_type", "well_baby_expense"),
    ("well_mother_payment_type", "well_mother_expense"),
]

for pt_field, val_field in payment_type_value_pairs:
    val = filtered_data.get(val_field)
    if val is not None and float(val) > 100:
        filtered_data[pt_field] = "Flat"

# --- Convert fields to match schema types ---
for field in schema.fields:
    val = filtered_data.get(field.name)
    if val is None:
        continue
    if isinstance(field.dataType, DecimalType):
        filtered_data[field.name] = decimal.Decimal(str(val))
    elif isinstance(field.dataType, ByteType):
        try:
            int_val = int(val)
            if not (-128 <= int_val <= 127):
                filtered_data[field.name] = None
        except (ValueError, TypeError):
            filtered_data[field.name] = None
    elif isinstance(field.dataType, ShortType):
        try:
            int_val = int(val)
            if not (-32768 <= int_val <= 32767):
                filtered_data[field.name] = None
        except (ValueError, TypeError):
            filtered_data[field.name] = None

insurance_df = spark.createDataFrame([filtered_data], schema=schema)
display(insurance_df)
