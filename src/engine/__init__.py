"""Schema mapping and data validation engine for GoHighLevel contact, opportunity, and note migration."""

from __future__ import annotations

from typing import Any

from src.engine.mapper import (
    CustomFieldDefinition,
    FieldMapper,
    FieldMappingConfig,
    MigrationSettings,
)
from src.engine.validator import (
    ContactValidator,
    GHLContactPayload,
    GHLCustomFieldValue,
    MappedContactRecord,
    ValidationBatchResult,
    ValidationErrorRecord,
)

__all__ = [
    "ContactValidator",
    "CustomFieldDefinition",
    "FieldMapper",
    "FieldMappingConfig",
    "GHLContactPayload",
    "GHLCustomFieldValue",
    "MappedContactRecord",
    "MigrationSettings",
    "NoteErrorRecord",
    "NoteMigrationStats",
    "NoteMigrator",
    "NotePayload",
    "OpportunityErrorRecord",
    "OpportunityMappingConfig",
    "OpportunityMigrationStats",
    "OpportunityMigrator",
    "OpportunityPayload",
    "ValidationBatchResult",
    "ValidationErrorRecord",
]


def __getattr__(name: str) -> Any:
    if name in {
        "NoteErrorRecord",
        "NoteMigrationStats",
        "NoteMigrator",
        "NotePayload",
    }:
        from src.engine import note_migrator

        return getattr(note_migrator, name)
    if name in {
        "OpportunityErrorRecord",
        "OpportunityMappingConfig",
        "OpportunityMigrationStats",
        "OpportunityMigrator",
        "OpportunityPayload",
    }:
        from src.engine import opportunity_migrator

        return getattr(opportunity_migrator, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
