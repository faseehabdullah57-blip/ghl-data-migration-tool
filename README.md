# GoHighLevel Data Migration Tool (API v2)

An enterprise-grade Python data ingestion and transformation pipeline designed to read legacy CRM exports (CSV files from **ActiveCampaign**, **HubSpot**, **Infusionsoft / Keap**, or **Salesforce**), validate and standardize records, dynamically provision missing custom fields (`contact` and `opportunity` models), deduplicate existing contacts, and sequentially migrate **Contacts -> Opportunities (Deals) -> Historical Notes** into **GoHighLevel (LeadConnector) API v2**.

---

## Business Value for Digital Marketing Agencies

Onboarding new clients to an agency's GoHighLevel sub-account is frequently delayed by messy manual CSV imports that break custom fields, duplicate contacts, lose sales pipeline deals, or fail to migrate historical tags and notes. This automation tool:
- **Reduces onboarding time from days to minutes** through repeatable, config-driven schema translation across Contacts, Opportunities, and Notes.
- **Prevents CRM data corruption** by enforcing strict E.164 phone standardization, RFC-compliant email validation, and custom field type coercion (`MONETORY`, `NUMERICAL`, `DATE`, `SINGLE_OPTIONS`, `TEXT`) before making API calls.
- **Preserves historical segmentation & deal history** by looking up existing GHL contacts via email and phone, merging legacy CRM tags with existing GHL tags, and linking Opportunities and Notes via a shared in-memory `contact_id_cache`.
- **Eliminates silent import failures** by generating row-by-row CSV error audit reports (`data/output/error_report.csv`, `data/output/opportunity_error_report.csv`, `data/output/note_error_report.csv`) containing exact validation and API error reasons ready for spreadsheet remediation.

---

## Repository Architecture

```text
ghl-data-migration-tool/
├── config/
│   ├── field_mappings.json          # Contact standard & custom field mappings
│   └── opportunity_mappings.json    # Pipeline stage IDs & Opportunity field mappings
├── data/
│   ├── input/
│   │   ├── sample_legacy_contacts.csv
│   │   ├── sample_opportunities.csv
│   │   └── sample_notes.csv
│   └── output/
│       ├── error_report.csv
│       ├── opportunity_error_report.csv
│       └── note_error_report.csv
├── src/
│   ├── api/
│   │   └── ghl_client.py            # GHL API v2 client with tenacity backoff & burst throttling
│   ├── engine/
│   │   ├── mapper.py                # JSON schema mapper & custom field type coercion engine
│   │   ├── validator.py             # Pandas CSV loader & Pydantic v2 Contact payload models
│   │   ├── opportunity_migrator.py  # Opportunity validation, contactId lookup & upsert engine
│   │   └── note_migrator.py         # Historical Note validation & contact attachment engine
│   └── main.py                      # Multi-stage CLI entrypoint & migration orchestrator
├── tests/
│   └── test_migration_pipeline.py   # Automated unit & end-to-end pipeline test suite
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Key Features

1. **Configurable Schema Mapping Engine (`src/engine/mapper.py`)**
   - Maps arbitrary legacy CSV headers (e.g., `"First_Name"`, `"Custom_Field_123"`, `"Lifetime_Value_USD"`) to GoHighLevel API v2 standard fields and typed custom fields via `config/field_mappings.json`.
   - Supports both `model: "contact"` and `model: "opportunity"` custom field provisioning.
2. **Pandas & Pydantic v2 Validation (`src/engine/validator.py`)**
   - Loads CSVs with `pandas` (`dtype=str`, `keep_default_na=False`) so leading zeroes in postal codes (e.g., `02139`) and phone numbers are never corrupted.
   - Standardizes domestic and international phone numbers to **ITU-T E.164** format (e.g., `(415) 555-0142` -> `+14155550142`, `+44 20 7946 0958` -> `+442079460958`) while stripping extensions (`ext. 204`).
3. **Sequential Multi-Object Migration (`Contacts -> Opportunities -> Notes`)**
   - **`OpportunityMigrator` (`src/engine/opportunity_migrator.py`)**: Validates deal rows via `OpportunityPayload`, maps legacy stage names to GHL `pipelineStageId`, resolves `contactId` from the shared `contact_id_cache` (or `client.find_contact()`), upserts deals via `POST /opportunities/upsert`, and logs rejected deals to `data/output/opportunity_error_report.csv`.
   - **`NoteMigrator` (`src/engine/note_migrator.py`)**: Validates note rows via `NotePayload`, resolves `contactId`, attaches notes via `POST /contacts/{contactId}/notes` (preserving legacy `Created_At` timestamps), and logs rejected notes to `data/output/note_error_report.csv`.
4. **Rate-Limit Aware Ingestion (`SlidingWindowThrottle` + `tenacity`)**
   - Proactively throttles burst traffic to stay within GoHighLevel API v2 burst limits (100 requests / 10 seconds) and monitors `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `X-RateLimit-Daily-Remaining` headers.
   - Reactively retries `HTTP 429 Too Many Requests` and transient `5xx` errors using `tenacity` exponential backoff with jitter while respecting `Retry-After` headers.

---

## Quick Start

### 1. Install Dependencies

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure Credentials

Copy `.env.example` to `.env` and populate your GoHighLevel v2 Private Integration Token (or OAuth2 Bearer Token) and Sub-Account Location ID:

```bash
cp .env.example .env
```

```ini
GHL_API_KEY=pit-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
GHL_LOCATION_ID=loc_xxxxxxxxxxxxxxxxxxxx
```

### 3. Run Multi-Object Dry-Run Mode (No Live Credentials Required)

Validate Contacts, Opportunities, and Notes end-to-end locally before touching your live GHL sub-account:

```bash
python -m src.main --dry-run \
  --opportunities data/input/sample_opportunities.csv \
  --notes data/input/sample_notes.csv
```

### 4. Run Live Multi-Object Migration

```bash
python -m src.main \
  --input data/input/sample_legacy_contacts.csv \
  --config config/field_mappings.json \
  --opportunities data/input/sample_opportunities.csv \
  --opportunity-config config/opportunity_mappings.json \
  --notes data/input/sample_notes.csv
```

---

## Docker Usage

Build the container image:

```bash
docker build -t ghl-data-migration-tool:latest .
```

Run a dry-run migration inside Docker:

```bash
docker run --rm ghl-data-migration-tool:latest \
  --dry-run \
  --opportunities data/input/sample_opportunities.csv \
  --notes data/input/sample_notes.csv
```

Run a live migration mounting local `data/` and `.env`:

```bash
docker run --rm --env-file .env \
  -v "$(pwd)/data:/app/data" \
  ghl-data-migration-tool:latest \
  --input data/input/sample_legacy_contacts.csv \
  --opportunities data/input/sample_opportunities.csv \
  --notes data/input/sample_notes.csv
```

---

## Example Terminal Output

```text
========================================================================
  GOHIGHLEVEL DATA MIGRATION EXECUTION SUMMARY [DRY-RUN (SIMULATED)]
========================================================================
  [1] CONTACTS MIGRATION
      Total Processed       : 15
      Created               : 8
      Updated (Merged)      : 2
      Failed                : 5
        - Validation Errors : 5
        - API Errors        : 0
      Custom Fields Synced  : 6
      Success Rate          : 66.67%
      Contact Error Log     : data/output/error_report.csv
------------------------------------------------------------------------
  [2] OPPORTUNITIES (DEALS) MIGRATION
      Total Processed       : 3
      Upserted              : 2
      Failed                : 1
        - Validation Errors : 0
        - Unmatched Contact : 1
        - API Errors        : 0
      Custom Fields Synced  : 3
      Opportunity Error Log : data/output/opportunity_error_report.csv
------------------------------------------------------------------------
  [3] HISTORICAL NOTES MIGRATION
      Total Processed       : 3
      Created               : 2
      Failed                : 1
        - Validation Errors : 0
        - Unmatched Contact : 1
        - API Errors        : 0
      Notes Error Log       : data/output/note_error_report.csv
------------------------------------------------------------------------
  Total Elapsed Time        : 0.06s
========================================================================
```

---

## Running Tests

```bash
pytest -v
```
