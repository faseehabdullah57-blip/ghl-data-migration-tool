"""Opportunity (Deal) schema mapping, validation, and ingestion engine for GoHighLevel API v2."""

from __future__ import annotations

import json
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
from src.engine.mapper import CustomFieldDefinition, FieldMapper, is_empty_value
from src.engine.validator import (
    EMAIL_REGEX,
    ContactValidator,
    GHLCustomFieldValue,
     standardize_phone_number,
)

logger = logging.getLogger(__name__)

OpportunityStatus = Literal["open", "won", "lost", "abandoned"]

ALLOWED_OPPORTUNITY_STATUSES: frozenset[str] = frozenset(
    {"open", "won", "lost", "abandoned"}
)


class OpportunityMappingConfig(BaseModel):
    """Pydantic configuration model for `config/opportunity_mappings.json`."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    version: str = Field(default="2.0")
    description: str = Field(default="")
    pipelineId: str = Field(..., min_length=1)
    default_status: OpportunityStatus = Field(default="open")
    stage_mappings: dict[str, str] = Field(
        default_factory=dict,
        description="Map of legacy stage name -> GoHighLevel pipelineStageId.",
    )
    standard_fields: dict[str, str] = Field(
        default_factory=dict,
        description="Map of CSV column name -> Opportunity standard field key.",
    )
    custom_fields: dict[str, CustomFieldDefinition] = Field(
        default_factory=dict,
        description="Map of CSV column name -> Opportunity custom field definition.",
    )

    @model_validator(mode="after")
    def ensure_opportunity_model_on_custom_fields(self) -> OpportunityMappingConfig:
        for cf in self.custom_fields.values():
            cf.model = "opportunity"
        return self

    def resolve_stage_id(self, stage_name: str) -> str:
        """Resolve a legacy stage name (case-insensitively) to a GHL pipelineStageId."""
        cleaned = stage_name.strip()
        if cleaned in self.stage_mappings:
            return self.stage_mappings[cleaned]
        lower_lookup = {k.lower(): v for k, v in self.stage_mappings.items()}
        if cleaned.lower() in lower_lookup:
            return lower_lookup[cleaned.lower()]
        allowed = ", ".join(self.stage_mappings.keys())
        raise ValueError(
            f"Unknown opportunity stage '{stage_name}'. Allowed stages: [{allowed}]."
        )


class OpportunityPayload(BaseModel):
    """Validated GoHighLevel API v2 Opportunity upsert payload."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    pipelineId: str = Field(..., min_length=1)
    pipelineStageId: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1)
    status: OpportunityStatus = Field(default="open")
    monetaryValue: Optional[float] = Field(default=None, ge=0.0)
    locationId: Optional[str] = None
    contactId: Optional[str] = None
    assignedTo: Optional[str] = None
    source: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    customFields: list[GHLCustomFieldValue] = Field(default_factory=list)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        normalized = value.strip().lower()
        if not EMAIL_REGEX.fullmatch(normalized):
            raise ValueError(f"Invalid contact email format on opportunity: '{value}'.")
        return normalized

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        return standardize_phone_number(value, default_country_code="+1")

    @field_validator("status", mode="before")
    @classmethod
    def normalize_status(cls, value: Any) -> str:
        if is_empty_value(value):
            return "open"
        cleaned = str(value).strip().lower()
        status_aliases = {
            "closed won": "won",
            "closed_won": "won",
            "closed-won": "won",
            "closed lost": "lost",
            "closed_lost": "lost",
            "closed-lost": "lost",
            "active": "open",
        }
        cleaned = status_aliases.get(cleaned, cleaned)
        if cleaned not in ALLOWED_OPPORTUNITY_STATUSES:
            allowed = ", ".join(sorted(ALLOWED_OPPORTUNITY_STATUSES))
            raise ValueError(
                f"Invalid opportunity status '{value}'. Allowed: [{allowed}]."
            )
        return cleaned

    @field_validator("monetaryValue", mode="before")
    @classmethod
    def parse_monetary_value(cls, value: Any) -> Optional[float]:
        if is_empty_value(value):
            return None
        if isinstance(value, (int, float)):
            return round(float(value), 2)
        cleaned = re.sub(r"[$€£¥,\s]", "", str(value).strip())
        try:
            return round(float(cleaned), 2)
        except ValueError as exc:
            raise ValueError(
                f"Invalid monetaryValue '{value}': expected a numeric currency value."
            ) from exc

    @model_validator(mode="after")
    def verify_contact_reference(self) -> OpportunityPayload:
        if not (self.contactId or self.email or self.phone):
            raise ValueError(
                "Opportunity must specify at least a contact email, phone, or contactId."
            )
        return self

    def to_api_payload(
        self,
        location_id: Optional[str] = None,
        contact_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """Serialize to the GHL API v2 `POST /opportunities/upsert` request payload."""
        effective_loc_id = location_id or self.locationId
        effective_contact_id = contact_id or self.contactId
        if not effective_contact_id:
            raise ValueError("contactId is required to serialize Opportunity API payload.")

        body: dict[str, Any] = {
            "pipelineId": self.pipelineId,
            "pipelineStageId": self.pipelineStageId,
            "name": self.name,
            "status": self.status,
            "contactId": effective_contact_id,
        }
        if effective_loc_id:
            body["locationId"] = effective_loc_id
        if self.monetaryValue is not None:
            body["monetaryValue"] = self.monetaryValue
        if self.assignedTo:
            body["assignedTo"] = self.assignedTo
        if self.source:
            body["source"] = self.source
        if self.customFields:
            body["customFields"] = [cf.to_api_dict() for cf in self.customFields]
        return body


class OpportunityErrorRecord(BaseModel):
    """Audit record for a rejected Opportunity row."""

    row_number: int
    failure_stage: Literal[
        "VALIDATION_ERROR", "UNMATCHED_CONTACT", "API_ERROR"
    ] = "VALIDATION_ERROR"
    status_code: Optional[int] = None
    error_reason: str
    api_response: Optional[str] = None
    deal_name: Optional[str] = None
    source_email: Optional[str] = None
    source_phone: Optional[str] = None
    raw_row: dict[str, Any] = Field(default_factory=dict)


class OpportunityMigrationStats(BaseModel):
    """Execution metrics for Opportunity migration."""

    total_processed: int = 0
    upserted: int = 0
    failed: int = 0
    validation_failures: int = 0
    unmatched_contact_failures: int = 0
    api_failures: int = 0
    custom_fields_ensured: int = 0
    error_report_path: Optional[str] = None


class OpportunityMigrator:
    """
    Loads legacy CRM deal exports with pandas, validates against `OpportunityPayload`,
    resolves GHL `contactId` via shared cache or API lookup, and upserts Opportunities.
    """

    def __init__(
        self,
        config_path: str | Path,
        ghl_client: GHLClient,
        contact_id_cache: Optional[dict[str, str]] = None,
        error_report_path: str | Path = "data/output/opportunity_error_report.csv",
    ) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config(self.config_path)
        self.ghl_client = ghl_client
        self.contact_id_cache = (
            contact_id_cache
            if contact_id_cache is not None
            else ghl_client.contact_id_cache
        )
        self.error_report_path = Path(error_report_path)
        self._custom_field_ids: dict[str, str] = {}

    @staticmethod
    def _load_config(path: Path) -> OpportunityMappingConfig:
        if not path.exists():
            raise FileNotFoundError(
                f"Opportunity mapping configuration file not found: {path}"
            )
        with path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
        return OpportunityMappingConfig.model_validate(raw)

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

    def _map_row(
        self, raw_row: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        """Transform a raw CSV opportunity row into an `OpportunityPayload` candidate dict."""
        candidate: dict[str, Any] = {
            "pipelineId": self.config.pipelineId,
            "status": self.config.default_status,
        }
        errors: list[str] = []

        for source_col, target_key in self.config.standard_fields.items():
            raw_val = raw_row.get(source_col)
            if is_empty_value(raw_val):
                continue
            text_val = str(raw_val).strip()

            if target_key == "stage":
                try:
                    candidate["pipelineStageId"] = self.config.resolve_stage_id(
                        text_val
                    )
                except ValueError as exc:
                    errors.append(str(exc))
            else:
                candidate[target_key] = text_val

        if "pipelineStageId" not in candidate and not errors:
            errors.append("Missing required opportunity stage column.")

        custom_fields_list: list[dict[str, Any]] = []
        for source_col, definition in self.config.custom_fields.items():
            raw_val = raw_row.get(source_col)
            if is_empty_value(raw_val):
                if definition.required:
                    errors.append(
                        f"Required opportunity custom field '{source_col}' ({definition.ghl_key}) is missing."
                    )
                continue
            try:
                coerced = FieldMapper.coerce_custom_field_value(
                    raw_val, definition, source_column=source_col
                )
                cf_entry: dict[str, Any] = {
                    "key": definition.normalized_key,
                    "field_value": coerced,
                }
                resolved_cf_id = self._custom_field_ids.get(
                    definition.normalized_key
                )
                if resolved_cf_id:
                    cf_entry["id"] = resolved_cf_id
                custom_fields_list.append(cf_entry)
            except ValueError as exc:
                errors.append(str(exc))

        if custom_fields_list:
            candidate["customFields"] = custom_fields_list

        return candidate, errors

    @staticmethod
    def _write_error_report(
        failed_records: list[OpportunityErrorRecord],
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
            "deal_name",
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
                "deal_name": rec.deal_name or "",
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
            "Saved %d failed opportunity row(s) to error report: %s",
            len(rows),
            output_path,
        )

    def migrate_csv(self, csv_path: str | Path) -> OpportunityMigrationStats:
        """Execute Opportunity migration for the provided CSV file."""
        df = ContactValidator.load_csv(csv_path)
        source_columns = list(df.columns)
        records: list[dict[str, Any]] = df.to_dict(orient="records")

        # Provision opportunity custom fields dynamically
        custom_fields_ensured = 0
        if self.config.custom_fields:
            self._custom_field_ids = self.ghl_client.ensure_custom_fields(
                self.config.custom_fields,
                model="opportunity",
            )
            custom_fields_ensured = len(self._custom_field_ids)

        upserted_count = 0
        validation_failures = 0
        unmatched_failures = 0
        api_failures = 0
        failed_records: list[OpportunityErrorRecord] = []

        for idx, raw_row in enumerate(records, start=1):
            deal_name = str(raw_row.get("Deal_Name", "")).strip() or None
            source_email = str(raw_row.get("Contact_Email", "")).strip() or None
            source_phone = str(raw_row.get("Contact_Phone", "")).strip() or None

            candidate_dict, mapping_errors = self._map_row(raw_row)
            pydantic_errors: list[str] = []
            payload: Optional[OpportunityPayload] = None

            try:
                payload = OpportunityPayload.model_validate(candidate_dict)
            except ValidationError as exc:
                for err in exc.errors():
                    loc_str = ".".join(str(p) for p in err.get("loc", ()))
                    msg = err.get("msg", "Invalid value")
                    if msg.startswith("Value error, "):
                        msg = msg[len("Value error, ") :]
                    pydantic_errors.append(
                        f"[{loc_str}] {msg}" if loc_str else msg
                    )

            all_errors = mapping_errors + pydantic_errors
            if all_errors or payload is None:
                validation_failures += 1
                reason = " | ".join(all_errors)
                logger.warning(
                    "Opportunity row %d failed validation (%s): %s",
                    idx,
                    deal_name or "unnamed-deal",
                    reason,
                )
                failed_records.append(
                    OpportunityErrorRecord(
                        row_number=idx,
                        failure_stage="VALIDATION_ERROR",
                        error_reason=reason,
                        deal_name=deal_name,
                        source_email=source_email,
                        source_phone=source_phone,
                        raw_row=raw_row,
                    )
                )
                continue

            # Resolve contactId via cache or API
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
                    "Opportunity row %d skipped (%s): %s",
                    idx,
                    payload.name,
                    reason,
                )
                failed_records.append(
                    OpportunityErrorRecord(
                        row_number=idx,
                        failure_stage="UNMATCHED_CONTACT",
                        error_reason=reason,
                        deal_name=payload.name,
                        source_email=source_email,
                        source_phone=source_phone,
                        raw_row=raw_row,
                    )
                )
                continue

            try:
                api_body = payload.to_api_payload(
                    location_id=self.ghl_client.location_id,
                    contact_id=contact_id,
                )
                self.ghl_client.upsert_opportunity(api_body)
                upserted_count += 1
            except GHLAPIError as exc:
                api_failures += 1
                logger.error(
                    "Opportunity row %d failed GHL API upsert (%s): %s",
                    idx,
                    payload.name,
                    exc,
                )
                failed_records.append(
                    OpportunityErrorRecord(
                        row_number=idx,
                        failure_stage="API_ERROR",
                        status_code=exc.status_code,
                        error_reason=exc.message,
                        api_response=exc.response_body,
                        deal_name=payload.name,
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
        return OpportunityMigrationStats(
            total_processed=len(records),
            upserted=upserted_count,
            failed=total_failed,
            validation_failures=validation_failures,
            unmatched_contact_failures=unmatched_failures,
            api_failures=api_failures,
            custom_fields_ensured=custom_fields_ensured,
            error_report_path=str(self.error_report_path),
        )
