<<<<<<< HEAD
<p align="center">
  <h1 align="center">🏥 GMC Extraction Pipeline</h1>
  <p align="center">
    <strong>AI-Powered Insurance Policy Data Extraction using LLMs + Databricks</strong>
  </p>
  <p align="center">
    <a href="#architecture">Architecture</a> •
    <a href="#features">Features</a> •
    <a href="#notebooks">Notebooks</a> •
    <a href="#tech-stack">Tech Stack</a> •
    <a href="#usage">Usage</a>
  </p>
</p>

---

## 📋 Overview

An end-to-end **intelligent document processing pipeline** that extracts structured data from Group Mediclaim (GMC) insurance policy PDFs using **GPT-4o-mini** and loads it into a **Delta Lake** data warehouse on Databricks.

The pipeline transforms unstructured PDF documents into a normalized, queryable star-schema — enabling downstream analytics, benchmarking, and reporting for the Employee Benefits (EB) insurance domain.

---

## 🏗️ Architecture <a name="architecture"></a>

```
=======
🏥 GMC Extraction Pipeline
AI-Powered Insurance Policy Data Extraction using LLMs + Databricks

Architecture • Features • Notebooks • Tech Stack • Usage

📋 Overview
An end-to-end intelligent document processing pipeline that extracts structured data from Group Mediclaim (GMC) insurance policy PDFs using GPT-4o-mini and loads it into a Delta Lake data warehouse on Databricks.

The pipeline transforms unstructured PDF documents into a normalized, queryable star-schema — enabling downstream analytics, benchmarking, and reporting for the Employee Benefits (EB) insurance domain.

🏗️ Architecture
>>>>>>> 5b7cb45 (Add README for GMC Extraction Pipeline)
┌─────────────────────────────────────────────────────────────────────┐
│                        GMC EXTRACTION PIPELINE                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   📄 PDF Documents                                                    │
│        │                                                              │
│        ▼                                                              │
│   ┌──────────────────────┐                                           │
│   │  common_pdf_extractor │ ◄── Shared utilities & LLM client        │
│   └──────────┬───────────┘                                           │
│              │                                                        │
│              ▼                                                        │
│   ┌──────────────────────────────────────────────────┐               │
│   │          MODULAR EXTRACTION NOTEBOOKS             │               │
│   ├──────────────────────────────────────────────────┤               │
│   │  gmc_master        │  gmc_policy_details         │               │
│   │  gmc_policy_addon  │  gmc_policy_additional_payout│              │
│   │  gmc_si            │  gmc_maternity              │               │
│   │  gmc_maternity_addon│  gmc_opd_addon             │               │
│   │  gmc_demographics  │  gmc_fam_capping            │               │
│   │  gmc_room_rent     │  gmc_copay                  │               │
│   │  gmc_waiting_period │  gmc_modern_treatment      │               │
│   │  gmc_ailment_capping│  gmc_exclusions            │               │
│   │  gmc_special_condition│                          │               │
│   └──────────────────────┬───────────────────────────┘               │
│                          │                                            │
│                          ▼                                            │
│   ┌──────────────────────────────────────────────────┐               │
│   │              DELTA LAKE (Bronze Layer)            │               │
│   │         eb-analytics.benchmark_bronze.*           │               │
│   └──────────────────────────────────────────────────┘               │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
<<<<<<< HEAD
```

---

## ✨ Features <a name="features"></a>

| Feature | Description |
|---------|-------------|
| **LLM-Powered Extraction** | Uses GPT-4o-mini with domain-specific prompts for zero-shot structured extraction |
| **Modular Design** | 17 independent extraction notebooks, each targeting a specific policy section |
| **Shared Utilities** | Common PDF extractor with reusable functions (`extract_with_llm`, `normalize_boolean`, `enforce_schema_types`) |
| **Schema Enforcement** | PySpark StructType schemas with type validation and overflow protection |
| **Parameterized Execution** | Supports orchestrated batch runs via `dbutils.widgets` (pdf_path, entity_id) |
| **Robust Text Extraction** | PyMuPDF-based PDF parsing with multi-page support |
| **Insurer Normalization** | Maps extracted insurer names to a standardized reference list (25+ insurers) |
| **Boolean Standardization** | Handles varied representations (Yes/No, Covered/Not Covered, True/False) |
| **Delta Lake Storage** | ACID-compliant writes to Unity Catalog managed tables |

---

## 📂 Project Structure <a name="notebooks"></a>

```
=======
✨ Features
Feature	Description
LLM-Powered Extraction	Uses GPT-4o-mini with domain-specific prompts for zero-shot structured extraction
Modular Design	17 independent extraction notebooks, each targeting a specific policy section
Shared Utilities	Common PDF extractor with reusable functions (extract_with_llm, normalize_boolean, enforce_schema_types)
Schema Enforcement	PySpark StructType schemas with type validation and overflow protection
Parameterized Execution	Supports orchestrated batch runs via dbutils.widgets (pdf_path, entity_id)
Robust Text Extraction	PyMuPDF-based PDF parsing with multi-page support
Insurer Normalization	Maps extracted insurer names to a standardized reference list (25+ insurers)
Boolean Standardization	Handles varied representations (Yes/No, Covered/Not Covered, True/False)
Delta Lake Storage	ACID-compliant writes to Unity Catalog managed tables

📂 Project Structure
>>>>>>> 5b7cb45 (Add README for GMC Extraction Pipeline)
gmc_extraction_pipeline/
│
├── 📄 README.md                      # This file
├── 🔧 common_pdf_extractor           # Shared utilities & LLM client
│
├── ── Core Policy ──────────────────────────────────────────
│   ├── gmc_master                    # Policy header: insurer, dates, premium, TPA
│   ├── gmc_policy_details            # Coverage details: maternity, ambulance, AYUSH, etc.
│   ├── gmc_policy_addon              # Add-on benefits: AIDS, COVID, gender reassignment
│   ├── gmc_policy_additional_payout  # Hospital cash, nursing allowance
│
├── ── Financial ────────────────────────────────────────────
│   ├── gmc_si                        # Sum Insured structure & grading
│   ├── gmc_copay                     # Co-payment clauses
│   ├── gmc_room_rent                 # Room rent limits & sub-limits
│
├── ── Benefits & Coverage ──────────────────────────────────
│   ├── gmc_maternity                 # Maternity benefit limits by delivery type
│   ├── gmc_maternity_addon           # Extended maternity benefits
│   ├── gmc_opd_addon                 # OPD coverage add-ons
│   ├── gmc_modern_treatment          # Modern treatment coverage
│
├── ── Restrictions & Conditions ────────────────────────────
│   ├── gmc_fam_capping               # Family size & age capping rules
│   ├── gmc_demographics              # Employee count & demographics
│   ├── gmc_waiting_period            # Waiting period conditions
│   ├── gmc_ailment_capping           # Disease-specific sub-limits
│   ├── gmc_exclusions                # Policy exclusions
│   └── gmc_special_condition         # Special conditions & clauses
<<<<<<< HEAD
```

---

## 🛠️ Tech Stack <a name="tech-stack"></a>

| Layer | Technology |
|-------|------------|
| **Compute** | Databricks (Serverless / Interactive Clusters) |
| **AI/LLM** | OpenAI GPT-4o-mini (structured extraction) |
| **PDF Parsing** | PyMuPDF (fitz) |
| **Data Processing** | Apache Spark (PySpark) |
| **Storage** | Delta Lake on Unity Catalog |
| **Orchestration** | Databricks Widgets + Notebook chaining |
| **Language** | Python 3.x |

---

## 🚀 Usage <a name="usage"></a>

### Standalone Mode (Single PDF)

1. Open any extraction notebook (e.g., `gmc_master`)
2. Set the `pdf_path` widget to your PDF location:
   ```
   /Workspace/Users/<your-user>/pdfs/<policy-file>.pdf
   ```
3. Run All cells — extracted data is written to Delta tables

### Orchestrated Mode (Batch Processing)

Pass parameters programmatically:
```python
=======

🛠️ Tech Stack
Layer	Technology
Compute	Databricks (Serverless / Interactive Clusters)
AI/LLM	OpenAI GPT-4o-mini (structured extraction)
PDF Parsing	PyMuPDF (fitz)
Data Processing	Apache Spark (PySpark)
Storage	Delta Lake on Unity Catalog
Orchestration	Databricks Widgets + Notebook chaining
Language	Python 3.x

🚀 Usage
Standalone Mode (Single PDF)
Open any extraction notebook (e.g., gmc_master)
Set the pdf_path widget to your PDF location:
/Workspace/Users/<your-user>/pdfs/<policy-file>.pdf
Run All cells — extracted data is written to Delta tables
Orchestrated Mode (Batch Processing)
Pass parameters programmatically:

>>>>>>> 5b7cb45 (Add README for GMC Extraction Pipeline)
dbutils.notebook.run(
    "gmc_master",
    timeout_seconds=600,
    arguments={"pdf_path": "/path/to/policy.pdf", "entity_id": "101"}
)
<<<<<<< HEAD
```

---

## 📊 Data Flow

```
=======
📊 Data Flow
>>>>>>> 5b7cb45 (Add README for GMC Extraction Pipeline)
PDF Document
    │
    ├─► PyMuPDF extracts raw text
    │
    ├─► GPT-4o-mini parses structured fields (JSON)
    │
    ├─► Post-processing: type casting, normalization, validation
    │
    └─► PySpark DataFrame ──► Delta Table (Bronze Layer)
                                  │
                                  ├─► benchmark_bronze.gmc_master
                                  ├─► benchmark_bronze.gmc_policy_details
                                  ├─► benchmark_bronze.gmc_si
                                  └─► ... (17 tables)
<<<<<<< HEAD
```

---

## 🔑 Key Design Decisions

1. **Modular over Monolithic** — Each policy section is an independent notebook, enabling parallel development and selective re-runs
2. **LLM with Schema Enforcement** — AI extracts data flexibly; PySpark schemas enforce strict types downstream
3. **Fallback Loading** — `common_pdf_extractor` loads via filesystem first, falls back to Workspace API for cold-start serverless environments
4. **Idempotent Writes** — Truncate-and-insert pattern ensures repeatability without duplicates

---

## 👤 Author

**Aryan More**  
Data Engineer @ EDME Insurance  
📧 aryan.more@edmeinsurance.com

---

## 📝 License

Internal use only — EDME Insurance Broking Pvt. Ltd.
=======
🔑 Key Design Decisions
Modular over Monolithic — Each policy section is an independent notebook, enabling parallel development and selective re-runs
LLM with Schema Enforcement — AI extracts data flexibly; PySpark schemas enforce strict types downstream
Fallback Loading — common_pdf_extractor loads via filesystem first, falls back to Workspace API for cold-start serverless environments
Idempotent Writes — Truncate-and-insert pattern ensures repeatability without duplicates
👤 Author
Aryan More
Data Engineer @ EDME Insurance
📧 aryan.more@edmeinsurance.com

📝 License
Internal use only — EDME Insurance
>>>>>>> 5b7cb45 (Add README for GMC Extraction Pipeline)
