"""End-to-end and unit tests for the GoHighLevel Data Migration Tool."""

from __future__ import annotations

import functools
import json
from pathlib import Path
from typing import Any, Callable
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

try:
    import pytest
except ImportError:  # pragma: no cover - lightweight offline fallback

    class _PytestMark:
        @staticmethod
        def parametrize(argnames: Any, argvalues: list[Any]) -> Callable[..., Any]:
            def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                func._parametrize = (argnames, argvalues)  # type: ignore[attr-defined]
                return func

            return _decorator

    class _RaisesContext:
        def __init__(
            self, exc_type: type[BaseException], match: str | None = None
        ) -> None:
            self.exc_type = exc_type
            self.match = match

        def __enter__(self) -> _RaisesContext:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, _tb: Any) -> bool:
            if exc_type is None:
                raise AssertionError(f"Did not raise {self.exc_type.__name__}")
            if not issubclass(exc_type, self.exc_type):
                return False
            if self.match and self.match not in str(exc_val):
                raise AssertionError(
                    f"Exception {exc_val!r} did not match {self.match!r}"
                )
            return True

    class _PytestStub:
        mark = _PytestMark()

        @staticmethod
        def raises(
            exc_type: type[BaseException], match: str | None = None
        ) -> _RaisesContext:
            return _RaisesContext(exc_type, match=match)

    pytest = _PytestStub()  # type: ignore[assignment]

try:
    import responses
except ImportError:  # pragma: no cover - lightweight offline HTTP mock fallback

    class _ResponsesStub:
        GET = "GET"
        POST = "POST"
        PUT = "PUT"

        def __init__(self) -> None:
            self._Queue: list[dict[str, Any]] = []

        def reset(self) -> None:
            self._Queue.clear()

        def add(
            self,
            method: str,
            url: str,
            json: Any = None,
            status: int = 200,
            headers: dict[str, str] | None = None,
        ) -> None:
            self._Queue.append(
                {
                    "method": method.upper(),
                    "url": url,
                    "json": json,
                    "status": status,
                    "headers": headers or {},
                }
            )

        def activate(self, func: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(func)
            def _wrapper(*args: Any, **kwargs: Any) -> Any:
                self.reset()

                def _fake_request(
                    _session_self: Any,
                    method: str,
                    url: str,
                    **_req_kwargs: Any,
                ) -> Any:
                    base_url = url.split("?", 1)[0]
                    for idx, item in enumerate(self._Queue):
                        if (
                            item["method"] == method.upper()
                            and item["url"].split("?", 1)[0] == base_url
                        ):
                            matched = self._Queue.pop(idx)
                            resp = MagicMock()
                            resp.status_code = matched["status"]
                            resp.headers = matched["headers"]
                            resp.text = (
                                json.dumps(matched["json"])
                                if matched["json"] is not None
                                else ""
                            )
                            resp.json.return_value = matched["json"]
                            return resp
                    raise AssertionError(f"Unmocked HTTP call: {method} {url}")

                with patch.object(requests.Session, "request", _fake_request):
                    return func(*args, **kwargs)

            return _wrapper

    responses = _ResponsesStub()  # type: ignore[assignment]

from src.api.ghl_client import GHLClient
from src.engine.mapper import CustomFieldDefinition, FieldMapper, FieldMappingConfig
from src.engine.note_migrator import NoteMigrator, NotePayload
from src.engine.opportunity_migrator import (
    OpportunityMigrator,
    OpportunityPayload,
)
from src.engine.validator import (
    GHLContactPayload,
    standardize_phone_number,
)
from src.main import MigrationOrchestrator


class TestPhoneAndEmailValidation:
    """Verify phone standardization and email format validation."""

    @pytest.mark.parametrize(
        ("raw_phone", "expected_e164"),
        [
            ("(415) 555-0142", "+14155550142"),
            ("212-555-0198", "+12125550198"),
            ("1-512-555-0173 ext. 204", "+15125550173"),
            ("+44 20 7946 0958", "+442079460958"),
            ("0081 3 5555 0144", "+81355550144"),
            ("3055550164", "+13055550164"),
        ],
    )
    def test_valid_phone_standardization(
        self, raw_phone: str, expected_e164: str
    ) -> None:
        assert standardize_phone_number(raw_phone) == expected_e164

    @pytest.mark.parametrize(
        "invalid_phone",
        [
            "555-ABCD",
            "12345",
            "ext 123",
            "+1-234",
        ],
    )
    def test_invalid_phone_raises_value_error(self, invalid_phone: str) -> None:
        with pytest.raises(ValueError):
            standardize_phone_number(invalid_phone)

    def test_contact_payload_requires_email_or_phone(self) -> None:
        with pytest.raises(ValueError, match="at least one valid identifier"):
            GHLContactPayload.model_validate(
                {"firstName": "Ghost", "lastName": "User"}
            )


class TestFieldMapper:
    """Verify schema mapping for both structured and flat JSON configurations."""

    def test_flat_json_mapping_support(self) -> None:
        flat_cfg = FieldMappingConfig.model_validate(
            {
                "First_Name": "firstName",
                "Email_Address": "email",
                "Custom_Field_123": "contact.custom_field_123",
            }
        )
        mapper = FieldMapper(config=flat_cfg)
        mapped, errors = mapper.map_row(
            {
                "First_Name": "Alice",
                "Email_Address": "alice@example.com",
                "Custom_Field_123": "AC-999",
            }
        )
        assert not errors
        assert mapped["firstName"] == "Alice"
        assert mapped["email"] == "alice@example.com"
        assert mapped["customFields"] == [
            {"key": "custom_field_123", "field_value": "AC-999"}
        ]

    def test_custom_field_coercion_types(self) -> None:
        monetary_def = CustomFieldDefinition(
            ghl_key="contact.ltv",
            name="Lifetime Value",
            data_type="MONETORY",
        )
        assert (
            FieldMapper.coerce_custom_field_value("$12,450.75", monetary_def, "LTV")
            == 12450.75
        )

        date_def = CustomFieldDefinition(
            ghl_key="contact.onboard_date",
            name="Onboard Date",
            data_type="DATE",
        )
        assert (
            FieldMapper.coerce_custom_field_value("04/18/2024", date_def, "Onboard")
            == "2024-04-18"
        )


class TestGHLClientRateLimitsAndUpsert:
    """Verify HTTP 429 retry logic, custom field creation, and contact deduplication."""

    def test_retry_on_http_429_rate_limit(self) -> None:
        mock_session = MagicMock()

        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.headers = {"Retry-After": "0.1"}
        resp_429.text = '{"message": "Too Many Requests"}'

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.headers = {"X-RateLimit-Remaining": "85"}
        resp_200.text = '{"contacts": [{"id": "c_123", "email": "test@example.com"}]}'
        resp_200.json.return_value = {
            "contacts": [{"id": "c_123", "email": "test@example.com"}]
        }

        mock_session.request.side_effect = [resp_429, resp_200]

        client = GHLClient(
            api_key="test-key",
            location_id="loc_1",
            session=mock_session,
        )
        found = client.search_contact_by_email("test@example.com")
        assert found is not None
        assert found["id"] == "c_123"
        assert mock_session.request.call_count == 2

    def test_dry_run_end_to_end_pipeline(self, tmp_path: Path) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        input_csv = repo_root / "data" / "input" / "sample_legacy_contacts.csv"
        config_json = repo_root / "config" / "field_mappings.json"
        error_csv = tmp_path / "error_report.csv"

        client = GHLClient(
            api_key="dry-run-key",
            location_id="loc_dryrun",
            dry_run=True,
        )
        orchestrator = MigrationOrchestrator(
            input_csv_path=input_csv,
            config_path=config_json,
            error_report_path=error_csv,
            ghl_client=client,
        )
        summary = orchestrator.run()

        assert summary.total_processed == 15
        assert summary.created == 8
        assert summary.updated == 2
        assert summary.failed == 5
        assert summary.validation_failures == 5
        assert summary.api_failures == 0
        assert error_csv.exists()


class TestOpportunitiesAndNotesMigration:
    """Verify Opportunity upserts, Contact Note creation, HTTP mocking via `responses`, and cache hits."""

    @responses.activate
    def test_opportunity_custom_fields_and_upsert_http_endpoints(self) -> None:
        responses.add(
            responses.GET,
            "https://services.leadconnectorhq.com/locations/loc_live_1/customFields",
            json={
                "customFields": [
                    {
                        "id": "cf_opp_existing_1",
                        "name": "Legacy CRM Deal ID",
                        "fieldKey": "opportunity.legacy_deal_id",
                        "dataType": "TEXT",
                    }
                ]
            },
            status=200,
        )
        responses.add(
            responses.POST,
            "https://services.leadconnectorhq.com/locations/loc_live_1/customFields",
            json={
                "customField": {
                    "id": "cf_opp_created_2",
                    "name": "Deal Contract Term",
                    "fieldKey": "opportunity.deal_contract_term",
                    "dataType": "SINGLE_OPTIONS",
                }
            },
            status=201,
        )
        responses.add(
            responses.POST,
            "https://services.leadconnectorhq.com/opportunities/upsert",
            json={
                "opportunity": {
                    "id": "opp_live_900",
                    "name": "Apex Q1 Retainer",
                    "contactId": "contact_101",
                    "pipelineId": "pipe_agency_sales_001",
                    "pipelineStageId": "stage_negotiation_004",
                    "monetaryValue": 24500.0,
                },
                "new": True,
            },
            status=200,
        )

        client = GHLClient(api_key="pit-live-key", location_id="loc_live_1")
        ensured = client.ensure_custom_fields(
            {
                "Legacy_Deal_ID": CustomFieldDefinition(
                    ghl_key="opportunity.legacy_deal_id",
                    name="Legacy CRM Deal ID",
                    data_type="TEXT",
                    model="opportunity",
                ),
                "Deal_Contract_Term": CustomFieldDefinition(
                    ghl_key="opportunity.deal_contract_term",
                    name="Deal Contract Term",
                    data_type="SINGLE_OPTIONS",
                    options=["12 Months", "24 Months"],
                    model="opportunity",
                ),
            },
            model="opportunity",
        )
        assert ensured["legacy_deal_id"] == "cf_opp_existing_1"
        assert ensured["deal_contract_term"] == "cf_opp_created_2"

        opp_payload = OpportunityPayload.model_validate(
            {
                "pipelineId": "pipe_agency_sales_001",
                "pipelineStageId": "stage_negotiation_004",
                "name": "Apex Q1 Retainer",
                "status": "open",
                "monetaryValue": "$24,500.00",
                "email": "sophia.martinez@apexdigital.io",
            }
        )
        result = client.upsert_opportunity(
            opp_payload.to_api_payload(
                location_id="loc_live_1", contact_id="contact_101"
            )
        )
        assert result["opportunity"]["id"] == "opp_live_900"

    @responses.activate
    def test_create_contact_note_http_endpoint(self) -> None:
        responses.add(
            responses.POST,
            "https://services.leadconnectorhq.com/contacts/contact_101/notes",
            json={
                "note": {
                    "id": "note_live_555",
                    "contactId": "contact_101",
                    "body": "Onboarding call completed.",
                }
            },
            status=201,
        )

        client = GHLClient(api_key="pit-live-key", location_id="loc_live_1")
        note = NotePayload.model_validate(
            {
                "email": "sophia.martinez@apexdigital.io",
                "body": "Onboarding call completed.",
                "created_at": "2024-11-10T14:30:00Z",
            }
        )
        res = client.create_contact_note(
            contact_id="contact_101",
            body=note.formatted_body(),
        )
        assert res["note"]["id"] == "note_live_555"

    def test_multi_object_dry_run_end_to_end(self, tmp_path: Path) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        contacts_csv = repo_root / "data" / "input" / "sample_legacy_contacts.csv"
        opportunities_csv = repo_root / "data" / "input" / "sample_opportunities.csv"
        notes_csv = repo_root / "data" / "input" / "sample_notes.csv"

        contact_cfg = repo_root / "config" / "field_mappings.json"
        opp_cfg = repo_root / "config" / "opportunity_mappings.json"

        contact_err_csv = tmp_path / "error_report.csv"
        opp_err_csv = tmp_path / "opportunity_error_report.csv"
        note_err_csv = tmp_path / "note_error_report.csv"

        client = GHLClient(
            api_key="dry-run-key",
            location_id="loc_dryrun",
            dry_run=True,
        )
        orchestrator = MigrationOrchestrator(
            input_csv_path=contacts_csv,
            config_path=contact_cfg,
            error_report_path=contact_err_csv,
            ghl_client=client,
            opportunities_csv_path=opportunities_csv,
            opportunity_config_path=opp_cfg,
            opportunity_error_report_path=opp_err_csv,
            notes_csv_path=notes_csv,
            note_error_report_path=note_err_csv,
        )
        summary = orchestrator.run()

        # 1. Contacts assertions
        assert summary.total_processed == 15
        assert summary.created == 8
        assert summary.updated == 2
        assert summary.failed == 5
        assert contact_err_csv.exists()

        # 2. Opportunities assertions (2 matched via contact_id_cache, 1 unmatched contact)
        assert summary.opportunities_stats is not None
        assert summary.opportunities_stats.total_processed == 3
        assert summary.opportunities_stats.upserted == 2
        assert summary.opportunities_stats.failed == 1
        assert summary.opportunities_stats.unmatched_contact_failures == 1
        assert opp_err_csv.exists()
        opp_err_df = pd.read_csv(opp_err_csv)
        assert len(opp_err_df) == 1
        assert opp_err_df.iloc[0]["failure_stage"] == "UNMATCHED_CONTACT"

        # 3. Notes assertions (2 matched via contact_id_cache, 1 unmatched contact)
        assert summary.notes_stats is not None
        assert summary.notes_stats.total_processed == 3
        assert summary.notes_stats.created == 2
        assert summary.notes_stats.failed == 1
        assert summary.notes_stats.unmatched_contact_failures == 1
        assert note_err_csv.exists()
        note_err_df = pd.read_csv(note_err_csv)
        assert len(note_err_df) == 1
        assert note_err_df.iloc[0]["failure_stage"] == "UNMATCHED_CONTACT"


if __name__ == "__main__":
    import inspect
    import tempfile

    test_classes = [
        TestPhoneAndEmailValidation,
        TestFieldMapper,
        TestGHLClientRateLimitsAndUpsert,
        TestOpportunitiesAndNotesMigration,
    ]
    passed = 0
    for cls in test_classes:
        instance = cls()
        for name, method in inspect.getmembers(instance, predicate=inspect.ismethod):
            if not name.startswith("test_"):
                continue
            param_spec = getattr(method, "_parametrize", None)
            if param_spec:
                _, argvalues = param_spec
                for args in argvalues:
                    call_args = args if isinstance(args, tuple) else (args,)
                    method(*call_args)
                    print(f"PASSED {cls.__name__}::{name}{call_args}")
                    passed += 1
            else:
                sig = inspect.signature(method)
                if "tmp_path" in sig.parameters:
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        method(Path(tmp_dir))
                else:
                    method()
                print(f"PASSED {cls.__name__}::{name}")
                passed += 1

    print(f"\n=== {passed} passed in test_migration_pipeline.py ===")
