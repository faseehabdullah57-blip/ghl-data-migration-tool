"""Schema mapping engine for translating legacy CRM columns to GoHighLevel API v2 fields."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

SUPPORTED_GHL_STANDARD_FIELDS: frozenset[str] = frozenset(
    {
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
        "tags",
        "dnd",
        "timezone",
        "dateOfBirth",
    }
)

GHLCustomFieldDataType = Literal[
    "TEXT",
    "LARGE_TEXT",
    "NUMERICAL",
    "MONETORY",
    "PHONE",
    "DATE",
    "SINGLE_OPTIONS",
    "MULTIPLE_OPTIONS",
    "CHECKBOX",
    "FILE_UPLOAD",
]

NULL_STRINGS: frozenset[str] = frozenset(
    {"", "null", "none", "n/a", "na", "nan", "undefined", "nil", "-"}
)


def is_empty_value(value: Any) -> bool:
    """Return True if a raw cell value represents null/empty data."""
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN check
        return True
    if isinstance(value, str) and value.strip().lower() in NULL_STRINGS:
        return True
    return False


class CustomFieldDefinition(BaseModel):
    """Configuration schema for a GoHighLevel custom field mapping."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ghl_key: str = Field(
        ...,
        min_length=1,
        description="GoHighLevel custom field key (e.g., 'contact.custom_field_123' or 'custom_field_123').",
    )
    name: str = Field(
        ...,
        min_length=1,
        description="Human-readable label used when creating the custom field in GHL.",
    )
    data_type: GHLCustomFieldDataType = Field(
        default="TEXT",
        description="GoHighLevel API v2 custom field dataType.",
    )
    placeholder: str = Field(
        default="",
        description="Optional placeholder text when creating the field in GHL.",
    )
    options: list[str] = Field(
        default_factory=list,
        description="Allowed option values for SINGLE_OPTIONS, MULTIPLE_OPTIONS, or CHECKBOX fields.",
    )
    model: Literal["contact", "opportunity"] = Field(
        default="contact",
        description="Target GoHighLevel object model ('contact' or 'opportunity').",
    )
    required: bool = Field(
        default=False,
        description="Whether a non-empty value is required for every migrated record.",
    )

    @property
    def normalized_key(self) -> str:
        """Return the custom field key stripped of any leading 'contact.' or 'opportunity.' namespace."""
        key = self.ghl_key.strip()
        for prefix in ("contact.", "opportunity."):
            if key.startswith(prefix):
                return key[len(prefix) :]
        return key

    @property
    def qualified_key(self) -> str:
        """Return the custom field key prefixed with its model namespace as used by GHL API v2."""
        return f"{self.model}.{self.normalized_key}"


class MigrationSettings(BaseModel):
    """Runtime settings controlling validation, normalization, and deduplication."""

    model_config = ConfigDict(extra="ignore")

    default_country_code: str = Field(
        default="+1",
        description="Default E.164 country calling code applied to 10-digit local numbers.",
    )
    require_email_or_phone: bool = Field(
        default=True,
        description="Require each contact to have at least a valid email or phone number.",
    )
    deduplicate_by: list[Literal["email", "phone"]] = Field(
        default_factory=lambda: ["email", "phone"],
        description="Ordered list of identifiers used to search for existing contacts.",
    )
    merge_tags: bool = Field(
        default=True,
        description="Preserve and merge existing contact tags in GHL during updates.",
    )
    default_tags: list[str] = Field(
        default_factory=lambda: ["legacy-crm-import"],
        description="Tags automatically attached to every migrated contact.",
    )
    tag_delimiter: str = Field(
        default=",",
        description="Primary delimiter used to split multi-tag strings in the source CSV.",
    )
    auto_create_custom_fields: bool = Field(
        default=True,
        description="Automatically create missing custom fields in GHL before upserting contacts.",
    )

    @field_validator("default_country_code")
    @classmethod
    def validate_country_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned.startswith("+"):
            cleaned = f"+{cleaned}"
        if not re.fullmatch(r"\+\d{1,4}", cleaned):
            raise ValueError(
                f"Invalid default_country_code '{value}'. Expected E.164 prefix like '+1' or '+44'."
            )
        return cleaned


class FieldMappingConfig(BaseModel):
    """Top-level Pydantic model for `config/field_mappings.json`."""

    model_config = ConfigDict(extra="ignore")

    version: str = Field(default="2.0")
    description: str = Field(default="")
    settings: MigrationSettings = Field(default_factory=MigrationSettings)
    standard_fields: dict[str, str] = Field(
        default_factory=dict,
        description="Map of legacy CSV column name -> GHL standard contact field key.",
    )
    custom_fields: dict[str, CustomFieldDefinition] = Field(
        default_factory=dict,
        description="Map of legacy CSV column name -> GHL custom field definition.",
    )

    @model_validator(mode="before")
    @classmethod
    def support_flat_or_structured_config(cls, data: Any) -> Any:
        """Allow both structured configs and simple flat {'Legacy_Col': 'ghlKey'} mappings."""
        if not isinstance(data, dict):
            return data

        if "standard_fields" in data or "custom_fields" in data:
            # Normalize any string shorthand entries inside custom_fields
            raw_custom = data.get("custom_fields", {})
            if isinstance(raw_custom, dict):
                normalized_custom: dict[str, Any] = {}
                for col_name, spec in raw_custom.items():
                    if isinstance(spec, str):
                        normalized_custom[col_name] = {
                            "ghl_key": spec,
                            "name": col_name.replace("_", " ").strip(),
                            "data_type": "TEXT",
                        }
                    else:
                        normalized_custom[col_name] = spec
                data = {**data, "custom_fields": normalized_custom}
            return data

        # Flat dictionary format: {"First_Name": "firstName", "Custom_Field_123": "custom_field_123"}
        standard_fields: dict[str, str] = {}
        custom_fields: dict[str, dict[str, Any]] = {}
        reserved_keys = {"version", "description", "settings"}

        for source_col, target in data.items():
            if source_col in reserved_keys:
                continue
            if isinstance(target, str):
                if target in SUPPORTED_GHL_STANDARD_FIELDS:
                    standard_fields[source_col] = target
                else:
                    custom_fields[source_col] = {
                        "ghl_key": target,
                        "name": source_col.replace("_", " ").strip(),
                        "data_type": "TEXT",
                    }
            elif isinstance(target, dict):
                custom_fields[source_col] = target

        return {
            "version": data.get("version", "2.0"),
            "description": data.get("description", "Flat field mapping configuration"),
            "settings": data.get("settings", {}),
            "standard_fields": standard_fields,
            "custom_fields": custom_fields,
        }

    @field_validator("standard_fields")
    @classmethod
    def validate_standard_field_targets(cls, mapping: dict[str, str]) -> dict[str, str]:
        invalid_targets = {
            src: target
            for src, target in mapping.items()
            if target not in SUPPORTED_GHL_STANDARD_FIELDS
        }
        if invalid_targets:
            allowed = ", ".join(sorted(SUPPORTED_GHL_STANDARD_FIELDS))
            raise ValueError(
                f"Unsupported GHL standard field target(s): {invalid_targets}. "
                f"Allowed standard fields: {allowed}"
            )
        return mapping


class FieldMapper:
    """Transforms legacy CRM CSV rows into GoHighLevel API v2 payload structures."""

    def __init__(self, config: FieldMappingConfig) -> None:
        self.config = config
        self.settings = config.settings
        # Maps resolved GHL custom field key (normalized) -> GHL custom field ID
        self._custom_field_ids: dict[str, str] = {}

    @classmethod
    def from_json_file(cls, config_path: str | Path) -> FieldMapper:
        """Load and validate a `field_mappings.json` file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Field mapping configuration file not found: {path}")

        with path.open("r", encoding="utf-8") as fh:
            raw_data = json.load(fh)

        config = FieldMappingConfig.model_validate(raw_data)
        logger.info(
            "Loaded field mapping config '%s' (%d standard fields, %d custom fields).",
            path.name,
            len(config.standard_fields),
            len(config.custom_fields),
        )
        return cls(config=config)

    def register_custom_field_ids(self, key_to_id_map: dict[str, str]) -> None:
        """Store resolved GHL custom field IDs keyed by normalized custom field key."""
        for key, field_id in key_to_id_map.items():
            clean_key = key[len("contact.") :] if key.startswith("contact.") else key
            self._custom_field_ids[clean_key] = field_id

    def inspect_columns(self, csv_columns: list[str]) -> dict[str, list[str]]:
        """Compare CSV headers against configured standard and custom field mappings."""
        csv_col_set = set(csv_columns)
        std_cols = set(self.config.standard_fields.keys())
        cust_cols = set(self.config.custom_fields.keys())
        configured_cols = std_cols | cust_cols

        return {
            "mapped_standard": sorted(csv_col_set & std_cols),
            "mapped_custom": sorted(csv_col_set & cust_cols),
            "unmapped_source_columns": sorted(csv_col_set - configured_cols),
            "missing_configured_columns": sorted(configured_cols - csv_col_set),
        }

    def parse_tags(self, raw_tags: Any) -> list[str]:
        """Parse delimited legacy tags and merge with default migration tags."""
        collected: list[str] = []
        seen_lower: set[str] = set()

        def _add_tag(tag_str: str) -> None:
            cleaned = tag_str.strip()
            if cleaned and cleaned.lower() not in NULL_STRINGS:
                lower_key = cleaned.lower()
                if lower_key not in seen_lower:
                    seen_lower.add(lower_key)
                    collected.append(cleaned)

        if not is_empty_value(raw_tags):
            if isinstance(raw_tags, list):
                for item in raw_tags:
                    _add_tag(str(item))
            else:
                # Split on configured delimiter as well as semicolons and pipes
                delimiters_pattern = r"[,;|]"
                if self.settings.tag_delimiter and self.settings.tag_delimiter not in ",;|":
                    delimiters_pattern = (
                        f"[{re.escape(self.settings.tag_delimiter)},;|]"
                    )
                for fragment in re.split(delimiters_pattern, str(raw_tags)):
                    _add_tag(fragment)

        for default_tag in self.settings.default_tags:
            _add_tag(default_tag)

        return collected

    @staticmethod
    def parse_boolean(raw_value: Any) -> bool:
        """Convert common legacy CRM boolean/opt-out representations to bool."""
        if isinstance(raw_value, bool):
            return raw_value
        if is_empty_value(raw_value):
            return False
        normalized = str(raw_value).strip().lower()
        if normalized in {"true", "1", "yes", "y", "t", "dnd", "opted_out", "unsubscribed"}:
            return True
        if normalized in {"false", "0", "no", "n", "f", "subscribed", "active"}:
            return False
        raise ValueError(f"Cannot parse boolean value from '{raw_value}'.")

    @staticmethod
    def coerce_custom_field_value(
        raw_value: Any,
        definition: CustomFieldDefinition,
        source_column: str,
    ) -> Any:
        """Validate and coerce a raw CSV value to the target GHL custom field dataType."""
        text_val = str(raw_value).strip()
        dtype = definition.data_type

        if dtype in {"NUMERICAL", "MONETORY"}:
            # Strip currency symbols, spaces, and thousands separators
            cleaned_num = re.sub(r"[$€£¥,\s]", "", text_val)
            try:
                numeric_val = float(cleaned_num)
            except ValueError as exc:
                raise ValueError(
                    f"Custom field '{source_column}' ({definition.ghl_key}) expects {dtype}, "
                    f"got invalid number '{text_val}'."
                ) from exc
            # Return int if whole number for NUMERICAL, or rounded float for MONETORY
            if dtype == "MONETORY":
                return round(numeric_val, 2)
            return int(numeric_val) if numeric_val.is_integer() else numeric_val

        if dtype == "DATE":
            for fmt in (
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y/%m/%d",
                "%m-%d-%Y",
                "%d-%m-%Y",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    parsed_dt = datetime.strptime(text_val, fmt)
                    return parsed_dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            raise ValueError(
                f"Custom field '{source_column}' ({definition.ghl_key}) expects DATE in "
                f"YYYY-MM-DD or MM/DD/YYYY format, got '{text_val}'."
            )

        if dtype == "SINGLE_OPTIONS":
            if definition.options:
                option_lookup = {opt.lower(): opt for opt in definition.options}
                if text_val.lower() not in option_lookup:
                    allowed = ", ".join(definition.options)
                    raise ValueError(
                        f"Custom field '{source_column}' ({definition.ghl_key}) value '{text_val}' "
                        f"is not in allowed options: [{allowed}]."
                    )
                return option_lookup[text_val.lower()]
            return text_val

        if dtype in {"MULTIPLE_OPTIONS", "CHECKBOX"}:
            parts = [p.strip() for p in re.split(r"[,;|]", text_val) if p.strip()]
            if definition.options:
                option_lookup = {opt.lower(): opt for opt in definition.options}
                validated_parts: list[str] = []
                for part in parts:
                    if part.lower() not in option_lookup:
                        allowed = ", ".join(definition.options)
                        raise ValueError(
                            f"Custom field '{source_column}' ({definition.ghl_key}) option '{part}' "
                            f"is not in allowed options: [{allowed}]."
                        )
                    validated_parts.append(option_lookup[part.lower()])
                return validated_parts
            return parts

        return text_val

    def map_row(self, raw_row: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        """
        Transform a raw CSV row dict into a candidate GHLContactPayload dict.

        Returns:
            A tuple of (mapped_payload_dict, list_of_mapping_errors).
        """
        payload: dict[str, Any] = {}
        errors: list[str] = []
        raw_tags_value: Any = None

        # 1. Map standard fields
        for source_col, ghl_field in self.config.standard_fields.items():
            raw_val = raw_row.get(source_col)
            if ghl_field == "tags":
                raw_tags_value = raw_val
                continue

            if is_empty_value(raw_val):
                continue

            if ghl_field == "dnd":
                try:
                    payload["dnd"] = self.parse_boolean(raw_val)
                except ValueError as exc:
                    errors.append(str(exc))
            else:
                payload[ghl_field] = str(raw_val).strip()

        # Always assign parsed tags (including default_tags)
        payload["tags"] = self.parse_tags(raw_tags_value)

        # 2. Map custom fields
        custom_fields_list: list[dict[str, Any]] = []
        for source_col, definition in self.config.custom_fields.items():
            raw_val = raw_row.get(source_col)
            if is_empty_value(raw_val):
                if definition.required:
                    errors.append(
                        f"Required custom field '{source_col}' ({definition.ghl_key}) is missing."
                    )
                continue

            try:
                coerced_value = self.coerce_custom_field_value(
                    raw_val, definition, source_column=source_col
                )
                cf_entry: dict[str, Any] = {
                    "key": definition.normalized_key,
                    "field_value": coerced_value,
                }
                resolved_id = self._custom_field_ids.get(definition.normalized_key)
                if resolved_id:
                    cf_entry["id"] = resolved_id
                custom_fields_list.append(cf_entry)
            except ValueError as exc:
                errors.append(str(exc))

        if custom_fields_list:
            payload["customFields"] = custom_fields_list

        return payload, errors
