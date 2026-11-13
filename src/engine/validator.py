"""Pandas CSV ingestion and Pydantic schema validation for GoHighLevel contact migration."""

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
    ValidationInfo,
    field_validator,
    model_validator,
)

from src.engine.mapper import FieldMapper, is_empty_value

logger = logging.getLogger(__name__)

# Strict email pattern disallowing consecutive dots or leading/trailing dots
EMAIL_REGEX = re.compile(
    r"^[A-Za-z0-9]+(?:[._%+\-][A-Za-z0-9]+)*@"
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,24}$"
)

# Extension suffix matcher (e.g., "ext. 204", "ext 42", "x109")
PHONE_EXTENSION_REGEX = re.compile(
    r"\s*(?:ext\.?|extension|x)\s*\d+\s*$",
    re.IGNORECASE,
)

COUNTRY_ISO_MAP: dict[str, str] = {
    "united states": "US",
    "united states of america": "US",
    "usa": "US",
    "u.s.a.": "US",
    "u.s.": "US",
    "us": "US",
    "canada": "CA",
    "ca": "CA",
    "united kingdom": "GB",
    "great britain": "GB",
    "england": "GB",
    "uk": "GB",
    "gb": "GB",
    "australia": "AU",
    "au": "AU",
    "new zealand": "NZ",
    "nz": "NZ",
    "germany": "DE",
    "de": "DE",
    "france": "FR",
    "fr": "FR",
    "spain": "ES",
    "es": "ES",
    "italy": "IT",
    "it": "IT",
    "netherlands": "NL",
    "nl": "NL",
    "switzerland": "CH",
    "ch": "CH",
    "japan": "JP",
    "jp": "JP",
    "singapore": "SG",
    "sg": "SG",
    "india": "IN",
    "in": "IN",
    "brazil": "BR",
    "br": "BR",
    "mexico": "MX",
    "mx": "MX",
    "ireland": "IE",
    "ie": "IE",
    "united arab emirates": "AE",
    "uae": "AE",
    "ae": "AE",
}


def standardize_phone_number(raw_phone: str, default_country_code: str = "+1") -> str:
    """
    Standardize a legacy phone number string into ITU-T E.164 format (e.g., '+14155550142').

    Raises:
        ValueError: If the phone string contains invalid alphabetic characters or an
            invalid number of digits.
    """
    cleaned = PHONE_EXTENSION_REGEX.sub("", raw_phone.strip())

    # Reject alphabetic characters remaining after stripping any valid extension
    if re.search(r"[A-Za-z]", cleaned):
        raise ValueError(
            f"Invalid phone number '{raw_phone}': contains alphabetic characters."
        )

    # Check if international prefix is explicitly provided
    has_plus = cleaned.startswith("+")
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]
        has_plus = True

    digits_only = re.sub(r"\D", "", cleaned)
    if not digits_only:
        raise ValueError(f"Invalid phone number '{raw_phone}': no digits found.")

    if has_plus:
        if not (10 <= len(digits_only) <= 15):
            raise ValueError(
                f"Invalid international phone number '{raw_phone}': expected 10-15 digits, "
                f"got {len(digits_only)}."
            )
        return f"+{digits_only}"

    # Local or national format without '+'
    cc_digits = re.sub(r"\D", "", default_country_code) or "1"

    if len(digits_only) == 10:
        return f"+{cc_digits}{digits_only}"

    if len(digits_only) == 11 and digits_only.startswith(cc_digits):
        return f"+{digits_only}"

    if 11 <= len(digits_only) <= 15:
        return f"+{digits_only}"

    raise ValueError(
        f"Invalid phone number '{raw_phone}': expected at least 10 digits for E.164 "
        f"standardization, got {len(digits_only)}."
    )


class GHLCustomFieldValue(BaseModel):
    """Pydantic model for a single custom field entry in a GHL API v2 contact payload."""

    model_config = ConfigDict(extra="forbid")

    id: Optional[str] = Field(
        default=None,
        description="GHL Custom Field ID (resolved dynamically via GHL API v2).",
    )
    key: str = Field(
        ...,
        min_length=1,
        description="Normalized GHL custom field key (e.g., 'custom_field_123').",
    )
    field_value: Any = Field(
        ...,
        description="Typed value for the custom field.",
    )

    def to_api_dict(self) -> dict[str, Any]:
        """Serialize to the dictionary format expected by GHL API v2 `customFields`."""
        entry: dict[str, Any] = {
            "key": self.key,
            "field_value": self.field_value,
        }
        if self.id:
            entry["id"] = self.id
        return entry


class GHLContactPayload(BaseModel):
    """Validated GoHighLevel API v2 contact upsert payload."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    locationId: Optional[str] = None
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    companyName: Optional[str] = None
    website: Optional[str] = None
    address1: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postalCode: Optional[str] = None
    country: Optional[str] = None
    source: Optional[str] = None
    timezone: Optional[str] = None
    dateOfBirth: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    dnd: bool = Field(default=False)
    customFields: list[GHLCustomFieldValue] = Field(default_factory=list)

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        normalized = value.strip().lower()
        if not EMAIL_REGEX.fullmatch(normalized):
            raise ValueError(f"Invalid email format: '{value}'.")
        return normalized

    @field_validator("phone")
    @classmethod
    def validate_and_format_phone(
        cls, value: Optional[str], info: ValidationInfo
    ) -> Optional[str]:
        if value is None or value == "":
            return None
        default_cc = "+1"
        if info.context and isinstance(info.context, dict):
            default_cc = info.context.get("default_country_code", "+1")
        return standardize_phone_number(value, default_country_code=default_cc)

    @field_validator("website")
    @classmethod
    def validate_and_normalize_website(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        url = value.strip()
        if not re.match(r"^https?://", url, flags=re.IGNORECASE):
            url = f"https://{url}"
        if not re.match(
            r"^https?://[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/.*)?$",
            url,
        ):
            raise ValueError(f"Invalid website URL: '{value}'.")
        return url

    @field_validator("country")
    @classmethod
    def normalize_country_code(cls, value: Optional[str]) -> Optional[str]:
        if value is None or value == "":
            return None
        cleaned = value.strip()
        lookup = cleaned.lower()
        if lookup in COUNTRY_ISO_MAP:
            return COUNTRY_ISO_MAP[lookup]
        if len(cleaned) == 2 and cleaned.isalpha():
            return cleaned.upper()
        return cleaned

    @model_validator(mode="after")
    def verify_required_identifiers(self, info: ValidationInfo) -> GHLContactPayload:
        require_identifier = True
        if info.context and isinstance(info.context, dict):
            require_identifier = info.context.get("require_email_or_phone", True)

        if require_identifier and not (self.email or self.phone):
            raise ValueError(
                "Contact must have at least one valid identifier ('email' or 'phone')."
            )

        # Populate full name automatically if firstName/lastName provided without name
        if not self.name and (self.firstName or self.lastName):
            parts = [p for p in (self.firstName, self.lastName) if p]
            self.name = " ".join(parts)

        return self

    def attach_custom_field_ids(self, key_to_id_map: dict[str, str]) -> None:
        """Bind dynamically resolved GHL custom field IDs to each customField item."""
        for cf in self.customFields:
            clean_key = (
                cf.key[len("contact.") :] if cf.key.startswith("contact.") else cf.key
            )
            if clean_key in key_to_id_map:
                cf.id = key_to_id_map[clean_key]

    def to_api_payload(
        self,
        location_id: Optional[str] = None,
        include_location_id: bool = True,
    ) -> dict[str, Any]:
        """
        Convert the model into a clean dictionary suitable for GoHighLevel API v2 requests.

        Note:
            GHL API v2 requires `locationId` on `POST /contacts/` and `POST /contacts/upsert`,
            but rejects `locationId` in the body of `PUT /contacts/{contactId}`.
        """
        data: dict[str, Any] = {}
        effective_loc_id = location_id or self.locationId
        if include_location_id and effective_loc_id:
            data["locationId"] = effective_loc_id

        scalar_fields = (
            "firstName",
            "lastName",
            "name",
            "email",
            "phone",
            "companyName",
            "website",
            "address1",
            "city",
            "state",
            "postalCode",
            "country",
            "source",
            "timezone",
            "dateOfBirth",
        )
        for field_name in scalar_fields:
            val = getattr(self, field_name)
            if val is not None:
                data[field_name] = val

        data["dnd"] = self.dnd
        if self.tags:
            data["tags"] = list(self.tags)
        if self.customFields:
            data["customFields"] = [cf.to_api_dict() for cf in self.customFields]

        return data


class MappedContactRecord(BaseModel):
    """Container pairing a validated GHLContactPayload with its source CSV metadata."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    row_number: int = Field(
        ...,
        description="1-based data row index in the source CSV file.",
    )
    payload: GHLContactPayload
    raw_row: dict[str, Any]


class ValidationErrorRecord(BaseModel):
    """Audit record for a row rejected during validation or GHL API ingestion."""

    model_config = ConfigDict(extra="ignore")

    row_number: int = Field(
        ...,
        description="1-based data row index in the source CSV file.",
    )
    failure_stage: Literal["VALIDATION_ERROR", "API_ERROR"] = Field(
        default="VALIDATION_ERROR"
    )
    status_code: Optional[int] = Field(default=None)
    error_reason: str = Field(...)
    api_response: Optional[str] = Field(default=None)
    source_email: Optional[str] = Field(default=None)
    source_phone: Optional[str] = Field(default=None)
    raw_row: dict[str, Any] = Field(default_factory=dict)


class ValidationBatchResult(BaseModel):
    """Summary of batch CSV validation before API ingestion."""

    total_rows: int
    valid_records: list[MappedContactRecord] = Field(default_factory=list)
    failed_records: list[ValidationErrorRecord] = Field(default_factory=list)


class ContactValidator:
    """Loads legacy CSV exports with pandas and validates rows against GHL schemas."""

    def __init__(self, mapper: FieldMapper) -> None:
        self.mapper = mapper

    @staticmethod
    def load_csv(file_path: str | Path) -> pd.DataFrame:
        """
        Load a legacy CRM CSV file using pandas while preserving string representations
        (such as leading zeroes in postal codes and phone numbers).
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Input CSV file not found: {path}")

        df = pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8-sig",
        )

        # Clean column names
        df.columns = [str(col).strip() for col in df.columns]

        # Trim whitespace on all string cells and drop completely empty rows
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()

        non_empty_mask = df.apply(
            lambda row: any(not is_empty_value(val) for val in row),
            axis=1,
        )
        cleaned_df = df.loc[non_empty_mask].reset_index(drop=True)

        logger.info(
            "Loaded %d non-empty contact rows from '%s' (%d columns).",
            len(cleaned_df),
            path.name,
            len(cleaned_df.columns),
        )
        return cleaned_df

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationBatchResult:
        """
        Map and validate every row in a pandas DataFrame.

        Returns:
            ValidationBatchResult containing valid `MappedContactRecord` items and
            rejected `ValidationErrorRecord` items with exact failure reasons.
        """
        valid_records: list[MappedContactRecord] = []
        failed_records: list[ValidationErrorRecord] = []

        email_source_cols = [
            src
            for src, target in self.mapper.config.standard_fields.items()
            if target == "email"
        ]
        phone_source_cols = [
            src
            for src, target in self.mapper.config.standard_fields.items()
            if target == "phone"
        ]

        validation_context = {
            "default_country_code": self.mapper.settings.default_country_code,
            "require_email_or_phone": self.mapper.settings.require_email_or_phone,
        }

        records: list[dict[str, Any]] = df.to_dict(orient="records")

        for idx, raw_row in enumerate(records, start=1):
            source_email = next(
                (
                    str(raw_row.get(c, "")).strip()
                    for c in email_source_cols
                    if raw_row.get(c)
                ),
                None,
            )
            source_phone = next(
                (
                    str(raw_row.get(c, "")).strip()
                    for c in phone_source_cols
                    if raw_row.get(c)
                ),
                None,
            )

            mapped_dict, mapping_errors = self.mapper.map_row(raw_row)
            pydantic_errors: list[str] = []

            payload: Optional[GHLContactPayload] = None
            try:
                payload = GHLContactPayload.model_validate(
                    mapped_dict,
                    context=validation_context,
                )
            except ValidationError as exc:
                for err in exc.errors():
                    loc_str = ".".join(str(part) for part in err.get("loc", ()))
                    msg = err.get("msg", "Invalid value")
                    # Strip Pydantic's 'Value error, ' prefix for cleaner audit logs
                    if msg.startswith("Value error, "):
                        msg = msg[len("Value error, ") :]
                    if loc_str:
                        pydantic_errors.append(f"[{loc_str}] {msg}")
                    else:
                        pydantic_errors.append(msg)

            all_errors = mapping_errors + pydantic_errors
            if all_errors or payload is None:
                combined_reason = " | ".join(all_errors)
                logger.warning(
                    "Row %d failed validation (%s): %s",
                    idx,
                    source_email or source_phone or "unknown-contact",
                    combined_reason,
                )
                failed_records.append(
                    ValidationErrorRecord(
                        row_number=idx,
                        failure_stage="VALIDATION_ERROR",
                        status_code=None,
                        error_reason=combined_reason,
                        api_response=None,
                        source_email=source_email or None,
                        source_phone=source_phone or None,
                        raw_row=raw_row,
                    )
                )
            else:
                valid_records.append(
                    MappedContactRecord(
                        row_number=idx,
                        payload=payload,
                        raw_row=raw_row,
                    )
                )

        logger.info(
            "Validation complete: %d valid, %d failed out of %d rows.",
            len(valid_records),
            len(failed_records),
            len(records),
        )
        return ValidationBatchResult(
            total_rows=len(records),
            valid_records=valid_records,
            failed_records=failed_records,
        )
