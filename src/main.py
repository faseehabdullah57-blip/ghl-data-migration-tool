"""CLI entrypoint and orchestration pipeline for GoHighLevel Data Migration Tool."""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, Field

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - lightweight offline fallback

    def load_dotenv(dotenv_path: Optional[str | Path] = None, **_kwargs: Any) -> bool:
        path = Path(dotenv_path or ".env")
        if not path.exists():
            return False
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, val = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip("'\""))
        return True

from src.api.ghl_client import GHLAPIError, GHLClient
from src.engine.mapper import FieldMapper
from src.engine.note_migrator import NoteMigrationStats, NoteMigrator
from src.engine.opportunity_migrator import (
    OpportunityMigrationStats,
    OpportunityMigrator,
)
from src.engine.validator import ContactValidator, ValidationErrorRecord

logger = logging.getLogger(__name__)


class MigrationSummary(BaseModel):
    """Pydantic model representing the final execution metrics of a migration run."""

    total_processed: int = Field(default=0, ge=0)
    created: int = Field(default=0, ge=0)
    updated: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    validation_failures: int = Field(default=0, ge=0)
    api_failures: int = Field(default=0, ge=0)
    custom_fields_ensured: int = Field(default=0, ge=0)
    duration_seconds: float = Field(default=0.0, ge=0.0)
    dry_run: bool = Field(default=False)
    error_report_path: Optional[str] = Field(default=None)
    opportunities_stats: Optional[OpportunityMigrationStats] = Field(default=None)
    notes_stats: Optional[NoteMigrationStats] = Field(default=None)

    @property
    def success_rate(self) -> float:
        if self.total_processed == 0:
            return 0.0
        succeeded = self.created + self.updated
        return round((succeeded / self.total_processed) * 100.0, 2)

    def format_terminal_report(self) -> str:
        """Render a clean unified terminal summary table for Contacts, Opportunities, and Notes."""
        mode_label = "DRY-RUN (SIMULATED)" if self.dry_run else "LIVE GHL API v2"
        lines = [
            "",
            "=" * 72,
            f"  GOHIGHLEVEL DATA MIGRATION EXECUTION SUMMARY [{mode_label}]",
            "=" * 72,
            "  [1] CONTACTS MIGRATION",
            f"      Total Processed       : {self.total_processed}",
            f"      Created               : {self.created}",
            f"      Updated (Merged)      : {self.updated}",
            f"      Failed                : {self.failed}",
            f"        - Validation Errors : {self.validation_failures}",
            f"        - API Errors        : {self.api_failures}",
            f"      Custom Fields Synced  : {self.custom_fields_ensured}",
            f"      Success Rate          : {self.success_rate:.2f}%",
        ]
        if self.error_report_path:
            lines.append(f"      Contact Error Log     : {self.error_report_path}")

        if self.opportunities_stats is not None:
            opp = self.opportunities_stats
            lines.extend(
                [
                    "-" * 72,
                    "  [2] OPPORTUNITIES (DEALS) MIGRATION",
                    f"      Total Processed       : {opp.total_processed}",
                    f"      Upserted              : {opp.upserted}",
                    f"      Failed                : {opp.failed}",
                    f"        - Validation Errors : {opp.validation_failures}",
                    f"        - Unmatched Contact : {opp.unmatched_contact_failures}",
                    f"        - API Errors        : {opp.api_failures}",
                    f"      Custom Fields Synced  : {opp.custom_fields_ensured}",
                ]
            )
            if opp.error_report_path:
                lines.append(f"      Opportunity Error Log : {opp.error_report_path}")

        if self.notes_stats is not None:
            notes = self.notes_stats
            lines.extend(
                [
                    "-" * 72,
                    "  [3] HISTORICAL NOTES MIGRATION",
                    f"      Total Processed       : {notes.total_processed}",
                    f"      Created               : {notes.created}",
                    f"      Failed                : {notes.failed}",
                    f"        - Validation Errors : {notes.validation_failures}",
                    f"        - Unmatched Contact : {notes.unmatched_contact_failures}",
                    f"        - API Errors        : {notes.api_failures}",
                ]
            )
            if notes.error_report_path:
                lines.append(f"      Notes Error Log       : {notes.error_report_path}")

        lines.extend(
            [
                "-" * 72,
                f"  Total Elapsed Time        : {self.duration_seconds:.2f}s",
                "=" * 72,
                "",
            ]
        )
        return "\n".join(lines)


class MigrationOrchestrator:
    """
    Coordinates sequential CRM migration: Contacts -> Opportunities -> Historical Notes,
    sharing a unified `contact_id_cache` across all stages.
    """

    def __init__(
        self,
        input_csv_path: str | Path,
        config_path: str | Path,
        error_report_path: str | Path,
        ghl_client: GHLClient,
        opportunities_csv_path: Optional[str | Path] = None,
        opportunity_config_path: str | Path = "config/opportunity_mappings.json",
        opportunity_error_report_path: str | Path = "data/output/opportunity_error_report.csv",
        notes_csv_path: Optional[str | Path] = None,
        note_error_report_path: str | Path = "data/output/note_error_report.csv",
    ) -> None:
        self.input_csv_path = Path(input_csv_path)
        self.config_path = Path(config_path)
        self.error_report_path = Path(error_report_path)
        self.ghl_client = ghl_client

        self.opportunities_csv_path = (
            Path(opportunities_csv_path) if opportunities_csv_path else None
        )
        self.opportunity_config_path = Path(opportunity_config_path)
        self.opportunity_error_report_path = Path(opportunity_error_report_path)

        self.notes_csv_path = Path(notes_csv_path) if notes_csv_path else None
        self.note_error_report_path = Path(note_error_report_path)

        # Shared contact lookup cache (email / E.164 phone / digits -> GHL contactId)
        self.contact_id_cache: dict[str, str] = self.ghl_client.contact_id_cache

    @staticmethod
    def write_error_report(
        failed_records: list[ValidationErrorRecord],
        output_path: Path,
        source_columns: list[str],
    ) -> None:
        """
        Export rejected rows along with validation/API failure metadata to a clean CSV
        using pandas.
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        audit_columns = [
            "row_number",
            "failure_stage",
            "status_code",
            "error_reason",
            "api_response",
            "source_email",
            "source_phone",
        ]
        ordered_cols = audit_columns + [
            col for col in source_columns if col not in audit_columns
        ]

        rows_to_write: list[dict[str, Any]] = []
        for record in failed_records:
            row_dict: dict[str, Any] = {
                "row_number": record.row_number,
                "failure_stage": record.failure_stage,
                "status_code": record.status_code if record.status_code is not None else "",
                "error_reason": record.error_reason,
                "api_response": record.api_response or "",
                "source_email": record.source_email or "",
                "source_phone": record.source_phone or "",
            }
            for col in source_columns:
                if col not in row_dict:
                    row_dict[col] = record.raw_row.get(col, "")
            rows_to_write.append(row_dict)

        error_df = pd.DataFrame(rows_to_write, columns=ordered_cols)
        error_df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(
            "Saved %d failed row(s) to error report: %s",
            len(rows_to_write),
            output_path,
        )

    def _register_contact_in_cache(
        self,
        email: Optional[str],
        phone: Optional[str],
        contact_id: str,
    ) -> None:
        """Populate shared `contact_id_cache` with email and phone keys."""
        if not contact_id:
            return
        if email:
            self.contact_id_cache[email.strip().lower()] = contact_id
        if phone:
            clean_phone = phone.strip()
            self.contact_id_cache[clean_phone] = contact_id
            digits = re.sub(r"\D", "", clean_phone)
            if digits:
                self.contact_id_cache[digits] = contact_id
                self.contact_id_cache[f"+{digits}"] = contact_id

    def run(self) -> MigrationSummary:
        """Execute the end-to-end migration pipeline (Contacts -> Opportunities -> Notes)."""
        start_time = time.perf_counter()

        # ---------------------------------------------------------------------
        # Stage 1: Contacts Migration
        # ---------------------------------------------------------------------
        mapper = FieldMapper.from_json_file(self.config_path)
        df = ContactValidator.load_csv(self.input_csv_path)
        source_columns = list(df.columns)

        col_inspection = mapper.inspect_columns(source_columns)
        if col_inspection["unmapped_source_columns"]:
            logger.info(
                "Unmapped CSV columns ignored: %s",
                ", ".join(col_inspection["unmapped_source_columns"]),
            )
        if col_inspection["missing_configured_columns"]:
            logger.warning(
                "Configured columns not present in CSV: %s",
                ", ".join(col_inspection["missing_configured_columns"]),
            )

        custom_fields_ensured = 0
        if mapper.settings.auto_create_custom_fields and mapper.config.custom_fields:
            resolved_cf_ids = self.ghl_client.ensure_custom_fields(
                mapper.config.custom_fields,
                model="contact",
            )
            mapper.register_custom_field_ids(resolved_cf_ids)
            custom_fields_ensured = len(resolved_cf_ids)

        validator = ContactValidator(mapper=mapper)
        validation_result = validator.validate_dataframe(df)

        all_failed_records: list[ValidationErrorRecord] = list(
            validation_result.failed_records
        )
        validation_failures_count = len(validation_result.failed_records)
        api_failures_count = 0
        created_count = 0
        updated_count = 0

        for item in validation_result.valid_records:
            try:
                upsert_res = self.ghl_client.upsert_contact(
                    payload=item.payload,
                    merge_tags=mapper.settings.merge_tags,
                    deduplicate_by=mapper.settings.deduplicate_by,
                )
                self._register_contact_in_cache(
                    email=item.payload.email,
                    phone=item.payload.phone,
                    contact_id=upsert_res.contact_id,
                )
                if upsert_res.action == "CREATED":
                    created_count += 1
                else:
                    updated_count += 1
            except GHLAPIError as exc:
                api_failures_count += 1
                logger.error(
                    "Row %d failed GHL API upsert (%s): %s",
                    item.row_number,
                    item.payload.email or item.payload.phone,
                    exc,
                )
                all_failed_records.append(
                    ValidationErrorRecord(
                        row_number=item.row_number,
                        failure_stage="API_ERROR",
                        status_code=exc.status_code,
                        error_reason=exc.message,
                        api_response=exc.response_body,
                        source_email=item.payload.email,
                        source_phone=item.payload.phone,
                        raw_row=item.raw_row,
                    )
                )

        all_failed_records.sort(key=lambda r: r.row_number)

        self.write_error_report(
            failed_records=all_failed_records,
            output_path=self.error_report_path,
            source_columns=source_columns,
        )

        # ---------------------------------------------------------------------
        # Stage 2: Opportunities (Deals) Migration (Optional)
        # ---------------------------------------------------------------------
        opportunities_stats: Optional[OpportunityMigrationStats] = None
        if self.opportunities_csv_path is not None:
            opp_migrator = OpportunityMigrator(
                config_path=self.opportunity_config_path,
                ghl_client=self.ghl_client,
                contact_id_cache=self.contact_id_cache,
                error_report_path=self.opportunity_error_report_path,
            )
            opportunities_stats = opp_migrator.migrate_csv(
                self.opportunities_csv_path
            )

        # ---------------------------------------------------------------------
        # Stage 3: Historical Notes Migration (Optional)
        # ---------------------------------------------------------------------
        notes_stats: Optional[NoteMigrationStats] = None
        if self.notes_csv_path is not None:
            note_migrator = NoteMigrator(
                ghl_client=self.ghl_client,
                contact_id_cache=self.contact_id_cache,
                error_report_path=self.note_error_report_path,
            )
            notes_stats = note_migrator.migrate_csv(self.notes_csv_path)

        elapsed = round(time.perf_counter() - start_time, 3)
        total_failed = validation_failures_count + api_failures_count

        summary = MigrationSummary(
            total_processed=validation_result.total_rows,
            created=created_count,
            updated=updated_count,
            failed=total_failed,
            validation_failures=validation_failures_count,
            api_failures=api_failures_count,
            custom_fields_ensured=custom_fields_ensured,
            duration_seconds=elapsed,
            dry_run=self.ghl_client.dry_run,
            error_report_path=str(self.error_report_path),
            opportunities_stats=opportunities_stats,
            notes_stats=notes_stats,
        )

        print(summary.format_terminal_report())
        return summary


def build_arg_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="ghl-data-migration-tool",
        description=(
            "Enterprise GoHighLevel Data Migration Script (API v2) — "
            "Validates, maps, deduplicates, and upserts legacy CRM Contacts, Opportunities, and Notes."
        ),
    )
    parser.add_argument(
        "-i",
        "--input",
        default="data/input/sample_legacy_contacts.csv",
        help="Path to the input legacy CRM Contacts CSV file (default: data/input/sample_legacy_contacts.csv).",
    )
    parser.add_argument(
        "-c",
        "--config",
        default="config/field_mappings.json",
        help="Path to the JSON contact field mapping configuration (default: config/field_mappings.json).",
    )
    parser.add_argument(
        "-e",
        "--error-output",
        default="data/output/error_report.csv",
        help="Path to save the contact CSV error audit report (default: data/output/error_report.csv).",
    )
    parser.add_argument(
        "--opportunities",
        default=None,
        help="Optional path to legacy Opportunities (Deals) CSV file (e.g., data/input/sample_opportunities.csv).",
    )
    parser.add_argument(
        "--opportunity-config",
        default="config/opportunity_mappings.json",
        help="Path to the JSON opportunity mapping configuration (default: config/opportunity_mappings.json).",
    )
    parser.add_argument(
        "--opportunity-error-output",
        default="data/output/opportunity_error_report.csv",
        help="Path to save the opportunity error audit report (default: data/output/opportunity_error_report.csv).",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional path to legacy Contact Notes CSV file (e.g., data/input/sample_notes.csv).",
    )
    parser.add_argument(
        "--note-error-output",
        default="data/output/note_error_report.csv",
        help="Path to save the note error audit report (default: data/output/note_error_report.csv).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="GoHighLevel API v2 Private Integration Token or OAuth Access Token (overrides GHL_API_KEY env var).",
    )
    parser.add_argument(
        "--location-id",
        default=None,
        help="GoHighLevel Sub-Account Location ID (overrides GHL_LOCATION_ID env var).",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="GoHighLevel API v2 Base URL (default: https://services.leadconnectorhq.com).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run full validation, mapping, and simulated GHL deduplication without making external HTTP calls.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level (default: INFO).",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    """Main CLI execution entrypoint."""
    load_dotenv()
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    api_key = args.api_key or os.getenv("GHL_API_KEY", "")
    location_id = args.location_id or os.getenv("GHL_LOCATION_ID", "")
    base_url = args.base_url or os.getenv(
        "GHL_BASE_URL", "https://services.leadconnectorhq.com"
    )
    burst_limit = int(os.getenv("GHL_BURST_LIMIT", "90"))
    burst_window = float(os.getenv("GHL_BURST_WINDOW_SECONDS", "10.0"))

    if not args.dry_run and (not api_key or not location_id):
        logger.error(
            "Missing GoHighLevel credentials. Set GHL_API_KEY and GHL_LOCATION_ID "
            "environment variables, pass --api-key and --location-id, or use --dry-run."
        )
        return 1

    if args.dry_run:
        api_key = api_key or "dry-run-token"
        location_id = location_id or "loc_dryrun_subaccount"

    client = GHLClient(
        api_key=api_key,
        location_id=location_id,
        base_url=base_url,
        burst_limit=burst_limit,
        burst_window_seconds=burst_window,
        dry_run=args.dry_run,
    )

    orchestrator = MigrationOrchestrator(
        input_csv_path=args.input,
        config_path=args.config,
        error_report_path=args.error_output,
        ghl_client=client,
        opportunities_csv_path=args.opportunities,
        opportunity_config_path=args.opportunity_config,
        opportunity_error_report_path=args.opportunity_error_output,
        notes_csv_path=args.notes,
        note_error_report_path=args.note_error_output,
    )

    try:
        orchestrator.run()
        return 0
    except Exception as exc:
        logger.exception("Migration terminated due to unhandled error: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
