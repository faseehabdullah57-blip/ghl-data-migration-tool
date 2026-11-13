"""Historical Contact Note validation and ingestion engine for GoHighLevel API v2."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Literal, Optional

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from src.api.ghl_client import GHLAPIError, GHLClient
from src.engine.mapper import is_empty_value
from src.engine.validator import (
    EMAIL_REGEX,
    ContactValidator,
    standardize_phone_number,
)

logger = logging.getLogger(__name__)


class NotePayload(BaseModel):
    """Validated GoHighLevel API v2 Contact Note payload."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    email: Optional[str] = None
    phone: Optional[str] = None
    body: str = Field(..., min_length=1)
    created_at: Optional[str] = None
    user_id: Optional[str] = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        normalized = value.strip().lower()
        if not EMAIL_REGEX.fullmatch(normalized):
            raise ValueError(f"Invalid contact email format on note: '{value}'.")
        return normalized

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        return standardize_phone_number(value, default_country_code="+1")

    @model_validator(mode="after")
    def verify_contact_identifier(self) -> NotePayload:
        if not (self.email or self.phone):
            raise ValueError(
                "Note must specify at least a contact email or phone number."
            )
        return self

    def formatted_body(self) -> str:
        """Return the note body enriched with its legacy creation timestamp if present."""
        if self.created_at:
            return f"{self.body}\n\n[Legacy Created At: {self.created_at}]"
        return self.body


class NoteErrorRecord(BaseModel):
    """Audit record for a rejected Contact Note row."""

    row_number: int
    failure_stage: Literal[
        "VALIDATION_ERROR", "UNMATCHED_CONTACT", "API_ERROR"
    ] = "VALIDATION_ERROR"
    status_code: Optional[int] = None
    error_reason: str
    api_response: Optional[str] = None
    source_email: Optional[str] = None
    source_phone: Optional[str] = None
    raw_row: dict[str, Any] = Field(default_factory=dict)


class NoteMigrationStats(BaseModel):
    """Execution metrics for Contact Note migration."""

    total_processed: int = 0
    created: int = 0
    failed: int = 0
    validation_failures: int = 0
    unmatched_contact_failures: int = 0
    api_failures: int = 0
    error_report_path: Optional[str] = None


class NoteMigrator:
    """
    Loads legacy CRM contact notes with pandas, validates via `NotePayload`,
    resolves `contactId` via shared cache or `client.find_contact()`, and attaches notes.
    """

    def __init__(
        self,
        ghl_client: GHLClient,
        contact_id_cache: Optional[dict[str, str]] = None,
        error_report_path: str | Path = "data/output/note_error_report.csv",
    ) -> None:
        self.ghl_client = ghl_client
        self.contact_id_cache = (
            contact_id_cache
            if contact_id_cache is not None
            else ghl_client.contact_id_cache
        )
        self.error_report_path = Path(error_report_path)

    def _resolve_contact_id(
        self, email: Optional[str], phone: Optional[str]
    ) -> Optional[str]:
        """Look up `contactId` in shared cache first, then fallback to `client.find_contact`."""
        if email:
            clean_email = email.strip().lower()
            if clean_email in self.contact_id_cache:
                return self.contact_id_cache[clean_email]

        if phone:
            clean_phone = phone.strip()
            digits = re.sub(r"\D", "", clean_phone)
            if clean_phone in self.contact_id_cache:
                return self.contact_id_cache[clean_phone]
            if digits and digits in self.contact_id_cache:
                return self.contact_id_cache[digits]

        resolved = self.ghl_client.find_contact(email=email, phone=phone)
        if resolved:
            if email:
                self.contact_id_cache[email.strip().lower()] = resolved
            if phone:
                self.contact_id_cache[phone.strip()] = resolved
                digits = re.sub(r"\D", "", phone)
                if digits:
                    self.contact_id_cache[digits] = resolved
        return resolved

    @staticmethod
    def _write_error_report(
        failed_records: list[NoteErrorRecord],
        output_path: Path,
        source_columns: list[str],
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        audit_cols = [
            "row_number",
            "failure_stage",
            "status_code",
            "error_reason",
            "api_response",
            "source_email",
            "source_phone",
        ]
        ordered_cols = audit_cols + [
            col for col in source_columns if col not in audit_cols
        ]
        rows: list[dict[str, Any]] = []
        for rec in failed_records:
            row_dict: dict[str, Any] = {
                "row_number": rec.row_number,
                "failure_stage": rec.failure_stage,
                "status_code": rec.status_code if rec.status_code is not None else "",
                "error_reason": rec.error_reason,
                "api_response": rec.api_response or "",
                "source_email": rec.source_email or "",
                "source_phone": rec.source_phone or "",
            }
            for col in source_columns:
                if col not in row_dict:
                    row_dict[col] = rec.raw_row.get(col, "")
            rows.append(row_dict)

        df = pd.DataFrame(rows, columns=ordered_cols)
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(
            "Saved %d failed note row(s) to error report: %s",
            len(rows),
            output_path,
        )

    def migrate_csv(self, csv_path: str | Path) -> NoteMigrationStats:
        """Execute historical Note migration for the provided CSV file."""
        df = ContactValidator.load_csv(csv_path)
        source_columns = list(df.columns)
        records: list[dict[str, Any]] = df.to_dict(orient="records")

        created_count = 0
        validation_failures = 0
        unmatched_failures = 0
        api_failures = 0
        failed_records: list[NoteErrorRecord] = []

        for idx, raw_row in enumerate(records, start=1):
            source_email = (
                str(raw_row.get("Contact_Email") or raw_row.get("email") or "").strip()
                or None
            )
            source_phone = (
                str(raw_row.get("Contact_Phone") or raw_row.get("phone") or "").strip()
                or None
            )
            raw_body = raw_row.get("Note_Body") or raw_row.get("body")
            raw_created_at = raw_row.get("Created_At") or raw_row.get("created_at")

            candidate: dict[str, Any] = {
                "email": None if is_empty_value(source_email) else source_email,
                "phone": None if is_empty_value(source_phone) else source_phone,
                "body": "" if is_empty_value(raw_body) else str(raw_body).strip(),
                "created_at": (
                    None
                    if is_empty_value(raw_created_at)
                    else str(raw_created_at).strip()
                ),
            }

            payload: Optional[NotePayload] = None
            try:
                payload = NotePayload.model_validate(candidate)
            except ValidationError as exc:
                validation_failures += 1
                errs: list[str] = []
                for err in exc.errors():
                    loc_str = ".".join(str(p) for p in err.get("loc", ()))
                    msg = err.get("msg", "Invalid value")
                    if msg.startswith("Value error, "):
                        msg = msg[len("Value error, ") :]
                    errs.append(f"[{loc_str}] {msg}" if loc_str else msg)
                reason = " | ".join(errs)
                logger.warning(
                    "Note row %d failed validation (%s): %s",
                    idx,
                    source_email or source_phone or "unknown-contact",
                    reason,
                )
                failed_records.append(
                    NoteErrorRecord(
                        row_number=idx,
                        failure_stage="VALIDATION_ERROR",
                        error_reason=reason,
                        source_email=source_email,
                        source_phone=source_phone,
                        raw_row=raw_row,
                    )
                )
                continue

            contact_id = self._resolve_contact_id(
                email=payload.email, phone=payload.phone
            )
            if not contact_id:
                unmatched_failures += 1
                reason = (
                    f"Could not resolve existing GHL contactId for email='{payload.email or ''}' "
                    f"or phone='{payload.phone or ''}'."
                )
                logger.warning(
                    "Note row %d skipped: %s",
                    idx,
                    reason,
                )
                failed_records.append(
                    NoteErrorRecord(
                        row_number=idx,
                        failure_stage="UNMATCHED_CONTACT",
                        error_reason=reason,
                        source_email=source_email,
                        source_phone=source_phone,
                        raw_row=raw_row,
                    )
                )
                continue

            try:
                self.ghl_client.create_contact_note(
                    contact_id=contact_id,
                    body=payload.formatted_body(),
                    user_id=payload.user_id,
                )
                created_count += 1
            except GHLAPIError as exc:
                api_failures += 1
                logger.error(
                    "Note row %d failed GHL API creation (%s): %s",
                    idx,
                    contact_id,
                    exc,
                )
                failed_records.append(
                    NoteErrorRecord(
                        row_number=idx,
                        failure_stage="API_ERROR",
                        status_code=exc.status_code,
                        error_reason=exc.message,
                        api_response=exc.response_body,
                        source_email=source_email,
                        source_phone=source_phone,
                        raw_row=raw_row,
                    )
                )

        self._write_error_report(
            failed_records=failed_records,
            output_path=self.error_report_path,
            source_columns=source_columns,
        )

        total_failed = validation_failures + unmatched_failures + api_failures
        return NoteMigrationStats(
            total_processed=len(records),
            created=created_count,
            failed=total_failed,
            validation_failures=validation_failures,
            unmatched_contact_failures=unmatched_failures,
            api_failures=api_failures,
            error_report_path=str(self.error_report_path),
        )
