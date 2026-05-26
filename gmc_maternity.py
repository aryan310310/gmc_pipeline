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

# DBTITLE 1,GMC Maternity Extraction Prompt
prompt = """
You are an expert health insurance GMC policy extractor specializing in Maternity benefit limits.

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
- If maternity is NOT mentioned at all → return an EMPTY list: []
- A single policy can have MULTIPLE maternity rows for different combinations of:
  * delivery_type (normal vs c-section)
  * hospital_location (metro vs non-metro)
  * grade_classification (if graded by employee grade)
  * SI_basis_description (if different limits per SI slab)

Rules:
- Return a JSON ARRAY of objects
- amount fields should contain numbers only (no commas, no Rs., no INR, NO DECIMALS)
- entity_id: Always set to null (assigned in post-processing)
- policy_number: Extract from the document

FIELD DEFINITIONS:

1. entity_id: Always null.

2. policy_number: Extract the policy number.

3. delivery_type: Type of delivery. STRICT ENUM:
   - "normal" → Normal/vaginal delivery
   - "c-section" → Caesarean/C-section/surgical delivery
   If the PDF mentions BOTH normal and c-section with DIFFERENT amounts → create separate rows for each.
   If only ONE amount mentioned for all deliveries → create rows for BOTH "normal" and "c-section" with same amount.

4. hospital_location: Hospital location type. STRICT ENUM:
   - "metro" → Metro city hospital
   - "non-metro" → Non-metro/tier-2/tier-3 city hospital
   If the PDF mentions BOTH metro and non-metro with DIFFERENT amounts → create separate rows for each.
   If only ONE amount mentioned regardless of location → create rows for BOTH "metro" and "non-metro" with same amount.

5. is_graded: Boolean.
   - true if maternity limits differ by employee grade/designation/level
   - false if same limit for all employees (or if differentiated only by SI slab)

6. grade_classification: The grade/designation name IF is_graded=true.
   Examples: "Director", "Executive", "Managerial", "Operational", "Technical", "Department Head", "Function Head"
   - null if is_graded=false

7. is_SI_basis: Boolean.
   - true if maternity limits differ based on the Sum Insured slab/option (e.g., 5L SI gets 50K maternity, 10L SI gets 75K)
   - false if maternity limit is independent of SI

8. SI_basis_description: The SI slab/option description IF is_SI_basis=true.
   Examples: "3L", "5L", "10L", "base", "gold", "platinum", "diamond", "<5L", "1.5L-4.5L"
   - null if is_SI_basis=false

9. maternity_payment_type: How the maternity limit is paid. STRICT ENUM:
   - "Flat" → Fixed rupee amount (e.g., Rs. 50000)
   - null → If maternity is covered within SI (no separate sub-limit, just "covered")

10. maternity_value: The maternity limit amount.
    - If payment_type is "Flat" → the rupee amount (e.g., 50000)
    - null if no specific amount mentioned (just "covered within SI")
    Extract as INTEGER only.

IMPORTANT RULES FOR MULTIPLE ROWS:
- Create ONE row per unique combination of (delivery_type, hospital_location, grade_classification, SI_basis_description).
- If policy says "Normal: Rs.50000, C-section: Rs.75000" with no metro/non-metro distinction:
  → 4 rows: normal+metro, normal+non-metro, c-section+metro, c-section+non-metro
- If policy says "Maternity: Rs.50000" (no distinction at all):
  → 4 rows: all with same value (normal+metro, normal+non-metro, c-section+metro, c-section+non-metro)
- If policy is GRADED (different grades get different amounts):
  → rows for each grade × delivery_type × hospital_location
- If policy is SI_BASIS (different SI slabs get different amounts):
  → rows for each SI_slab × delivery_type × hospital_location
- entity_id and policy_number remain SAME across all rows.

HOW TO IDENTIFY MATERNITY:
- Look for: "Maternity", "Maternity Benefit", "Normal Delivery", "Caesarean", "C-section"
- Graded: different amounts for different designations/grades
- SI basis: different amounts mentioned alongside SI slabs/options

RETURN FORMAT:
[
  {
    "entity_id": null,
    "policy_number": null,
    "delivery_type": "normal",
    "hospital_location": "metro",
    "is_graded": false,
    "grade_classification": null,
    "is_SI_basis": false,
    "SI_basis_description": null,
    "maternity_payment_type": "Flat",
    "maternity_value": null
  }
]

If NO maternity benefit is found in the document, return: []
"""

# COMMAND ----------

# DBTITLE 1,Call LLM for extraction
import json
import re

# Extract maternity data - returns a LIST of rows
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
    maternity_rows = json.loads(match.group(0))
    print(f"JSON LOADED SUCCESSFULLY")
    print(f"Extracted {len(maternity_rows)} maternity rows")
    for i, row in enumerate(maternity_rows):
        print(f"\n  Row {i+1}:")
        for k, v in row.items():
            print(f"    {k}: {v}")
else:
    # Try as single object
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        single = json.loads(match.group(0))
        maternity_rows = [single]
        print(f"JSON LOADED (single object) - 1 row")
    else:
        maternity_rows = []
        print("NO VALID JSON FOUND - assuming no maternity benefits")

# COMMAND ----------

# DBTITLE 1,Post-processing and validation
import re

# Set entity_id and policy_number for all rows
# Set entity_id from orchestrator param or default
_entity_param = dbutils.widgets.get("entity_id")
entity_id = int(_entity_param) if _entity_param else 101
policy_number_extracted = None

# Get policy_number from first row
if maternity_rows and maternity_rows[0].get("policy_number"):
    policy_number_extracted = maternity_rows[0]["policy_number"]

# --- PDF KEYWORD VERIFICATION ---
MATERNITY_PATTERN = r'(?i)(maternit|normal\s*deliver|caesarean|c.?section)'

print("--- PDF Maternity Keyword Check ---")
has_maternity_in_pdf = bool(re.search(MATERNITY_PATTERN, text))
print(f"  Maternity mentioned in PDF: {has_maternity_in_pdf}")

if not has_maternity_in_pdf and maternity_rows:
    print(f"  \u26a0\ufe0f LLM returned {len(maternity_rows)} rows but NO maternity keyword in PDF \u2192 clearing all rows")
    maternity_rows = []

# --- VALIDATION ---
ALLOWED_DELIVERY_TYPES = ["normal", "c-section"]
ALLOWED_HOSPITAL_LOCATIONS = ["metro", "non-metro"]
ALLOWED_PAYMENT_TYPES = ["Flat"]  # Only Flat or null

print("\n--- Row Validation ---")
validated_rows = []
for i, row in enumerate(maternity_rows):
    # Set entity_id and policy_number
    row["entity_id"] = entity_id
    if policy_number_extracted:
        row["policy_number"] = policy_number_extracted
    
    # Validate delivery_type
    dt = row.get("delivery_type", "").strip().lower()
    if "c-section" in dt or "caesarean" in dt or "caesar" in dt or "surgical" in dt:
        row["delivery_type"] = "c-section"
    elif "normal" in dt or "vaginal" in dt:
        row["delivery_type"] = "normal"
    elif dt not in ALLOWED_DELIVERY_TYPES:
        row["delivery_type"] = "normal"  # default
    
    # Validate hospital_location
    hl = row.get("hospital_location", "").strip().lower()
    if "non" in hl or "tier" in hl:
        row["hospital_location"] = "non-metro"
    elif "metro" in hl:
        row["hospital_location"] = "metro"
    elif hl not in ALLOWED_HOSPITAL_LOCATIONS:
        row["hospital_location"] = "metro"  # default
    
    # Validate is_graded
    if row.get("is_graded") is True:
        if not row.get("grade_classification"):
            row["grade_classification"] = f"Grade{i+1}"
    else:
        row["is_graded"] = False
        row["grade_classification"] = None
    
    # Validate is_SI_basis
    if row.get("is_SI_basis") is True:
        if not row.get("SI_basis_description"):
            row["SI_basis_description"] = None
    else:
        row["is_SI_basis"] = False
        row["SI_basis_description"] = None
    
    # Validate maternity_payment_type
    pt = row.get("maternity_payment_type")
    if pt:
        pt_clean = pt.strip().lower()
        if pt_clean == "flat":
            row["maternity_payment_type"] = "Flat"
        else:
            row["maternity_payment_type"] = None
    
    # If payment type is Flat but value > 0, keep it
    # If value is 0 or None, set payment type to None
    val = row.get("maternity_value")
    if val is not None:
        try:
            val_int = int(val)
            if val_int <= 0:
                row["maternity_value"] = None
                row["maternity_payment_type"] = None
            else:
                row["maternity_value"] = val_int
                row["maternity_payment_type"] = "Flat"
        except (ValueError, TypeError):
            row["maternity_value"] = None
    else:
        row["maternity_payment_type"] = None
    
    validated_rows.append(row)
    print(f"  \u2705 Row {i+1}: {row['delivery_type']} / {row['hospital_location']} / grade={row.get('grade_classification')} / SI={row.get('SI_basis_description')} / val={row.get('maternity_value')}")

maternity_rows = validated_rows

# --- EXPAND ROWS ---
# If LLM only returned rows for one delivery_type or one location, expand to cover all combinations
def expand_rows(rows):
    """Ensure all delivery_type x hospital_location combinations exist for each grade/SI_basis."""
    if not rows:
        return rows
    
    # Group by (grade_classification, SI_basis_description) to find unique value groups
    groups = {}
    for row in rows:
        key = (row.get("grade_classification"), row.get("SI_basis_description"))
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    
    expanded = []
    for key, group_rows in groups.items():
        # Find which delivery_type x hospital_location combos exist
        existing_combos = set()
        for r in group_rows:
            existing_combos.add((r["delivery_type"], r["hospital_location"]))
        
        all_combos = [("normal", "metro"), ("normal", "non-metro"), ("c-section", "metro"), ("c-section", "non-metro")]
        
        # Use first row as template for missing combos
        template = group_rows[0].copy()
        
        for combo in all_combos:
            if combo in existing_combos:
                # Find and keep existing row
                for r in group_rows:
                    if r["delivery_type"] == combo[0] and r["hospital_location"] == combo[1]:
                        expanded.append(r)
                        break
            else:
                # Create new row from template
                new_row = template.copy()
                new_row["delivery_type"] = combo[0]
                new_row["hospital_location"] = combo[1]
                expanded.append(new_row)
    
    return expanded

maternity_rows = expand_rows(maternity_rows)

print(f"\n--- Final (after expansion): {len(maternity_rows)} maternity rows ---")
for i, row in enumerate(maternity_rows):
    print(f"  Row {i+1}: {row['delivery_type']} / {row['hospital_location']} / grade={row.get('grade_classification')} / SI={row.get('SI_basis_description')} / type={row.get('maternity_payment_type')} / val={row.get('maternity_value')}")

# COMMAND ----------

# DBTITLE 1,Schema definition and DataFrame creation
from pyspark.sql.types import *
from decimal import Decimal

schema = StructType([
    StructField("entity_id", IntegerType(), False),
    StructField("policy_number", StringType(), False),
    StructField("delivery_type", StringType(), False),
    StructField("hospital_location", StringType(), False),
    StructField("is_graded", BooleanType(), True),
    StructField("grade_classification", StringType(), True),
    StructField("is_SI_basis", BooleanType(), True),
    StructField("SI_basis_description", StringType(), True),
    StructField("maternity_payment_type", StringType(), True),
    StructField("maternity_value", DecimalType(8, 0), True)
])

# Convert rows to proper types
schema_cols = [f.name for f in schema.fields]
formatted_rows = []

for row in maternity_rows:
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
    maternity_df = spark.createDataFrame(formatted_rows, schema=schema)
else:
    maternity_df = spark.createDataFrame([], schema=schema)
    print("\u26a0\ufe0f No maternity data found - empty DataFrame created")

display(maternity_df)
print(f"\n\u2705 GMC Maternity extraction complete! ({maternity_df.count()} rows)")
