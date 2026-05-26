# Databricks notebook source
# DBTITLE 1,Common PDF Extractor - Shared Utilities
# MAGIC %md
# MAGIC # Common PDF Extractor - Shared Utilities
# MAGIC This notebook is called by all table-specific extraction notebooks via `%run`.
# MAGIC It provides: PDF path input, text extraction, OpenAI client, and shared utilities.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install pymupdf openai typing_extensions --upgrade -q

# COMMAND ----------

# DBTITLE 1,PDF Path Widget
# === SET PDF PATH HERE (only place to change) ===
# Widget receives value from orchestrator OR can be set manually
try:
    dbutils.widgets.text("pdf_path", "", "PDF File Path")
except:
    pass  # Widget may already exist from parent notebook

pdf_path = dbutils.widgets.get("pdf_path")

# Fallback: hardcode path if widget is empty
if not pdf_path:
    pdf_path = "/Workspace/Users/aryan.more@edmeinsurance.com/pdfs/01 XBPAT (BT) GMC Policy Copy 2025-26.pdf"

print(f"Processing PDF: {pdf_path}")

# COMMAND ----------

# DBTITLE 1,PyMuPDF Text Extraction
import fitz

doc = fitz.open(pdf_path)
text = ""
for page in doc:
    text += page.get_text()

print(f"Extracted {len(text)} characters from {len(doc)} pages")
print(text[:3000])

# COMMAND ----------

# DBTITLE 1,OpenAI Client Setup
import subprocess, sys, importlib

# Force upgrade typing_extensions to fix Omit serialization issue with openai
try:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", "typing_extensions", "-q"])
except Exception:
    pass  # Already installed by orchestrator or %pip cell

# Reload typing_extensions to pick up new version
try:
    import typing_extensions
    importlib.reload(typing_extensions)
except Exception:
    pass

# Remove cached openai modules to force clean import
import sys as _sys
for mod_name in list(_sys.modules.keys()):
    if 'openai' in mod_name:
        del _sys.modules[mod_name]

from openai import OpenAI

client = OpenAI(
    api_key="DBRICKS_SECRET"
)


# COMMAND ----------

# DBTITLE 1,LLM Extraction Function
import json
import re

def extract_with_llm(prompt_text, pdf_text, model="gpt-4o-mini", max_chars=250000):
    """Call OpenAI LLM with a prompt and PDF text, return parsed JSON dict."""
    truncated_text = pdf_text[:max_chars]
    
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": prompt_text},
            {"role": "user", "content": truncated_text}
        ],
        temperature=0
    )
    
    json_output = response.choices[0].message.content
    json_output = json_output.replace("```json", "").replace("```", "").strip()
    
    match = re.search(r"\{.*\}", json_output, re.DOTALL)
    if match:
        data = json.loads(match.group(0))
        print("JSON LOADED SUCCESSFULLY")
        return data
    else:
        print("NO VALID JSON FOUND")
        return None

# COMMAND ----------

# DBTITLE 1,Shared Utility Functions
import decimal
from pyspark.sql.types import *

def normalize_boolean(v):
    """Normalize various boolean representations to Python bool."""
    if v is None:
        return None
    v = str(v).strip().lower()
    return v in ["true", "yes", "covered", "included", "applicable"]

def fix_payment_type_by_value(data, payment_type_value_pairs):
    """If value > 100, override payment_type to 'Flat'."""
    for pt_field, val_field in payment_type_value_pairs:
        val = data.get(val_field)
        if val is not None:
            try:
                if float(val) > 100:
                    data[pt_field] = "Flat"
            except (ValueError, TypeError):
                pass
    return data

def enforce_schema_types(data, schema):
    """Convert fields to match PySpark schema types with overflow protection."""
    for field in schema.fields:
        val = data.get(field.name)
        if val is None:
            continue
        if isinstance(field.dataType, DecimalType):
            try:
                data[field.name] = decimal.Decimal(str(val))
            except:
                data[field.name] = None
        elif isinstance(field.dataType, ByteType):
            try:
                int_val = int(val)
                if not (-128 <= int_val <= 127):
                    data[field.name] = None
            except (ValueError, TypeError):
                data[field.name] = None
        elif isinstance(field.dataType, ShortType):
            try:
                int_val = int(val)
                if not (-32768 <= int_val <= 32767):
                    data[field.name] = None
            except (ValueError, TypeError):
                data[field.name] = None
    return data

def apply_boolean_normalization(data, boolean_columns):
    """Normalize all boolean fields in the data dict."""
    for col in boolean_columns:
        if col in data:
            data[col] = normalize_boolean(data.get(col))
    return data

print("Shared utilities loaded successfully")
print(f"PDF text available: {len(text)} characters")

# COMMAND ----------

# DBTITLE 1,How to Use
# MAGIC %md
# MAGIC ## How to Use
# MAGIC In your table-specific notebook, add this as the FIRST cell:
# MAGIC ```python
# MAGIC %run ./common_pdf_extractor
# MAGIC ```
# MAGIC Then you have access to: `text`, `client`, `extract_with_llm()`, `normalize_boolean()`, `fix_payment_type_by_value()`, `enforce_schema_types()`, `apply_boolean_normalization()`