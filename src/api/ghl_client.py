"""GoHighLevel API v2 client with rate-limit throttling, tenacity retries, and deduplication."""

from __future__ import annotations

import collections
import json
import logging
import re
import time
import uuid
import functools
import random
from typing import Any, Callable, Literal, Optional

import requests
from pydantic import BaseModel, Field

try:
    from tenacity import (
        RetryCallState,
        before_sleep_log,
        retry,
        retry_if_exception_type,
        stop_after_attempt,
        wait_exponential_jitter,
    )
except ImportError:  # pragma: no cover - lightweight offline fallback

    class _Outcome:
        def __init__(self, exc: BaseException) -> None:
            self._exc = exc
            self.failed = True

        def exception(self) -> BaseException:
            return self._exc

    class RetryCallState:  # type: ignore[no-redef]
        def __init__(self, attempt_number: int, exc: BaseException) -> None:
            self.attempt_number = attempt_number
            self.outcome = _Outcome(exc)

    def retry_if_exception_type(exc_types: tuple[type[BaseException], ...]) -> Any:
        return exc_types

    def stop_after_attempt(max_attempts: int) -> int:
        return max_attempts

    def wait_exponential_jitter(initial: float = 1.0, max: float = 30.0) -> Callable[[Any], float]:
        def _wait(state: Any) -> float:
            attempt = getattr(state, "attempt_number", 1)
            delay = min(max, initial * (2 ** max(0, attempt - 1)))
            return delay + random.uniform(0.0, 0.25)

        return _wait

    def before_sleep_log(log: logging.Logger, level: int) -> Callable[[Any], None]:
        def _log(state: Any) -> None:
            log.log(level, "Retrying failed GHL request (attempt %d)...", state.attempt_number)

        return _log

    def retry(
        retry: Any,
        wait: Callable[[Any], float],
        stop: int,
        before_sleep: Optional[Callable[[Any], None]] = None,
        reraise: bool = True,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def _decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(func)
            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                attempt = 1
                while True:
                    try:
                        return func(*args, **kwargs)
                    except retry as exc:
                        if attempt >= stop:
                            raise
                        state = RetryCallState(attempt_number=attempt, exc=exc)
                        sleep_s = wait(state)
                        if before_sleep:
                            before_sleep(state)
                        time.sleep(sleep_s)
                        attempt += 1

            return _wrapped

        return _decorator

from src.engine.mapper import CustomFieldDefinition
from src.engine.validator import GHLContactPayload

logger = logging.getLogger(__name__)

DEFAULT_GHL_BASE_URL = "https://services.leadconnectorhq.com"
DEFAULT_GHL_API_VERSION = "2021-07-28"


class GHLAPIError(Exception):
    """Base exception raised when a GoHighLevel API v2 request fails."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[str] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        self.endpoint = endpoint
        self.method = method

    def __str__(self) -> str:
        prefix = f"HTTP {self.status_code}" if self.status_code else "API Error"
        return f"{prefix} ({self.method or ''} {self.endpoint or ''}): {self.message}"


class GHLRateLimitError(GHLAPIError):
    """Raised when GoHighLevel API returns HTTP 429 Too Many Requests."""

    def __init__(
        self,
        message: str,
        retry_after: float = 2.0,
        status_code: int = 429,
        response_body: Optional[str] = None,
        endpoint: Optional[str] = None,
        method: Optional[str] = None,
    ) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            response_body=response_body,
            endpoint=endpoint,
            method=method,
        )
        self.retry_after = max(0.5, retry_after)


class GHLTransientServerError(GHLAPIError):
    """Raised on retryable 5xx server responses from GoHighLevel."""


class UpsertOperationResult(BaseModel):
    """Structured result of a single contact create/update operation."""

    action: Literal["CREATED", "UPDATED"]
    contact_id: str
    matched_by: Optional[Literal["email", "phone"]] = None
    merged_tags: list[str] = Field(default_factory=list)
    raw_response: dict[str, Any] = Field(default_factory=dict)


class SlidingWindowThrottle:
    """
    Proactive rate-limiter enforcing GoHighLevel API v2 burst limits
    (100 requests per 10 seconds per location) and daily quota headers.
    """

    def __init__(
        self,
        burst_limit: int = 90,
        window_seconds: float = 10.0,
        min_remaining_buffer: int = 5,
    ) -> None:
        self.burst_limit = burst_limit
        self.window_seconds = window_seconds
        self.min_remaining_buffer = min_remaining_buffer
        self._timestamps: collections.deque[float] = collections.deque()
        self.daily_remaining: Optional[int] = None

    def acquire(self) -> None:
        """Sleep if necessary to stay within the sliding burst window."""
        now = time.monotonic()
        while self._timestamps and (now - self._timestamps[0]) >= self.window_seconds:
            self._timestamps.popleft()

        if len(self._timestamps) >= self.burst_limit:
            earliest = self._timestamps[0]
            sleep_duration = self.window_seconds - (now - earliest) + 0.05
            if sleep_duration > 0:
                logger.debug(
                    "Burst rate limit threshold (%d/%ds) reached; throttling for %.2fs.",
                    self.burst_limit,
                    int(self.window_seconds),
                    sleep_duration,
                )
                time.sleep(sleep_duration)
            now = time.monotonic()
            while (
                self._timestamps and (now - self._timestamps[0]) >= self.window_seconds
            ):
                self._timestamps.popleft()

        self._timestamps.append(time.monotonic())

    def inspect_response_headers(self, headers: dict[str, Any]) -> None:
        """Read GHL rate-limit response headers and pause proactively if near exhaustion."""
        # Case-insensitive header lookup
        lower_headers = {k.lower(): v for k, v in headers.items()}

        daily_raw = lower_headers.get("x-ratelimit-daily-remaining")
        if daily_raw is not None:
            try:
                self.daily_remaining = int(daily_raw)
                if self.daily_remaining <= 100:
                    logger.warning(
                        "GHL daily API quota critically low: %d requests remaining.",
                        self.daily_remaining,
                    )
            except ValueError:
                pass

        burst_remaining_raw = lower_headers.get("x-ratelimit-remaining")
        reset_raw = lower_headers.get("x-ratelimit-reset")
        if burst_remaining_raw is not None:
            try:
                remaining = int(burst_remaining_raw)
                if remaining <= self.min_remaining_buffer:
                    wait_seconds = 1.5
                    if reset_raw is not None:
                        try:
                            reset_val = float(reset_raw)
                            # Handle either epoch timestamp or seconds-until-reset
                            if reset_val > 1_000_000_000:
                                wait_seconds = max(0.5, reset_val - time.time())
                            else:
                                wait_seconds = max(0.5, min(reset_val, 15.0))
                        except ValueError:
                            pass
                    logger.info(
                        "GHL X-RateLimit-Remaining=%d; sleeping %.2fs until window reset.",
                        remaining,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
            except ValueError:
                pass


def _wait_for_rate_limit_or_backoff(retry_state: RetryCallState) -> float:
    """
    Custom tenacity wait strategy that honors GHL's `Retry-After` / `X-RateLimit-Reset`
    delay on HTTP 429 responses, or falls back to exponential backoff with jitter.
    """
    outcome = retry_state.outcome
    if outcome is not None and outcome.failed:
        exc = outcome.exception()
        if isinstance(exc, GHLRateLimitError) and exc.retry_after > 0:
            return exc.retry_after
    fallback_wait = wait_exponential_jitter(initial=1.0, max=30.0)
    return fallback_wait(retry_state)


class GHLClient:
    """
    Enterprise GoHighLevel API v2 Client supporting contact search, dynamic custom field
    creation, contact deduplication/merging, and automatic HTTP 429 retry handling.
    """

    def __init__(
        self,
        api_key: str,
        location_id: str,
        base_url: str = DEFAULT_GHL_BASE_URL,
        api_version: str = DEFAULT_GHL_API_VERSION,
        burst_limit: int = 90,
        burst_window_seconds: float = 10.0,
        timeout_seconds: float = 30.0,
        dry_run: bool = False,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.location_id = location_id.strip()
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.timeout_seconds = timeout_seconds
        self.dry_run = dry_run

        self.throttle = SlidingWindowThrottle(
            burst_limit=burst_limit,
            window_seconds=burst_window_seconds,
        )
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Version": self.api_version,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "GHL-Data-Migration-Tool/2.0",
            }
        )

        # In-memory caches to minimize redundant API calls and prevent intra-batch duplicates
        self._custom_fields_cache: Optional[dict[str, dict[str, Any]]] = None
        self._custom_fields_by_model: dict[str, dict[str, dict[str, Any]]] = {}
        self._email_contact_cache: dict[str, dict[str, Any]] = {}
        self._phone_contact_cache: dict[str, dict[str, Any]] = {}
        self.contact_id_cache: dict[str, str] = {}

        if self.dry_run:
            self._seed_dry_run_state()

    def _seed_dry_run_state(self) -> None:
        """Initialize realistic simulated GHL sub-account state for `--dry-run` mode."""
        self._custom_fields_cache = {
            "legacy_crm_id": {
                "id": "cf_existing_001",
                "name": "Legacy CRM Contact ID",
                "fieldKey": "contact.legacy_crm_id",
                "dataType": "TEXT",
            },
            "subscription_tier": {
                "id": "cf_existing_002",
                "name": "Subscription Tier",
                "fieldKey": "contact.subscription_tier",
                "dataType": "SINGLE_OPTIONS",
            },
        }
        self._custom_fields_by_model["contact"] = dict(self._custom_fields_cache)
        self._custom_fields_by_model["opportunity"] = {
            "legacy_deal_id": {
                "id": "cf_opp_existing_001",
                "name": "Legacy CRM Deal ID",
                "fieldKey": "opportunity.legacy_deal_id",
                "dataType": "TEXT",
            }
        }
        # Pre-populate two existing GHL contacts to demonstrate email & phone deduplication
        existing_sophia = {
            "id": "ghl_contact_existing_101",
            "locationId": self.location_id,
            "firstName": "Sophia",
            "lastName": "Martinez",
            "email": "sophia.martinez@apexdigital.io",
            "phone": "+14155550142",
            "tags": ["existing-ghl-lead", "2023-client"],
        }
        existing_ethan = {
            "id": "ghl_contact_existing_102",
            "locationId": self.location_id,
            "firstName": "Ethan",
            "lastName": "Brooks",
            "email": "old.email@summitlogistics.com",
            "phone": "+13055550164",
            "tags": ["inbound-call-lead"],
        }
        self._cache_contact(existing_sophia)
        self._cache_contact(existing_ethan)

    @staticmethod
    def _normalize_phone_digits(phone: Optional[str]) -> str:
        if not phone:
            return ""
        return re.sub(r"\D", "", phone)

    def _cache_contact(self, contact: dict[str, Any]) -> None:
        """Store a contact record in local email, phone, and contact_id lookup caches."""
        contact_id = str(contact.get("id") or "").strip()

        email = contact.get("email")
        if isinstance(email, str) and email.strip():
            clean_email = email.strip().lower()
            self._email_contact_cache[clean_email] = contact
            if contact_id:
                self.contact_id_cache[clean_email] = contact_id

        phone = contact.get("phone")
        if isinstance(phone, str) and phone.strip():
            clean_phone = phone.strip()
            digits = self._normalize_phone_digits(clean_phone)
            if digits:
                self._phone_contact_cache[digits] = contact
                if contact_id:
                    self.contact_id_cache[clean_phone] = contact_id
                    self.contact_id_cache[digits] = contact_id
                    self.contact_id_cache[f"+{digits}"] = contact_id

    @staticmethod
    def _parse_retry_after(headers: dict[str, Any]) -> float:
        """Extract retry delay in seconds from `Retry-After` or `X-RateLimit-Reset`."""
        lower_headers = {k.lower(): v for k, v in headers.items()}
        retry_after_raw = lower_headers.get("retry-after")
        if retry_after_raw is not None:
            try:
                return max(0.5, float(retry_after_raw))
            except ValueError:
                pass

        reset_raw = lower_headers.get("x-ratelimit-reset")
        if reset_raw is not None:
            try:
                reset_val = float(reset_raw)
                if reset_val > 1_000_000_000:
                    return max(0.5, reset_val - time.time())
                return max(0.5, min(reset_val, 30.0))
            except ValueError:
                pass

        return 2.5

    @retry(
        retry=retry_if_exception_type(
            (
                GHLRateLimitError,
                GHLTransientServerError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            )
        ),
        wait=_wait_for_rate_limit_or_backoff,
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Execute an HTTP request against GoHighLevel API v2 with proactive rate-limit
        throttling and automatic tenacity retries on HTTP 429 / 5xx errors.
        """
        self.throttle.acquire()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        response = self.session.request(
            method=method.upper(),
            url=url,
            params=params,
            json=json_payload,
            timeout=self.timeout_seconds,
        )

        self.throttle.inspect_response_headers(dict(response.headers))

        if response.status_code == 429:
            retry_after = self._parse_retry_after(dict(response.headers))
            raise GHLRateLimitError(
                message=f"GHL API rate limit exceeded (HTTP 429). Retrying in {retry_after:.2f}s.",
                retry_after=retry_after,
                status_code=429,
                response_body=response.text,
                endpoint=endpoint,
                method=method.upper(),
            )

        if 500 <= response.status_code < 600:
            raise GHLTransientServerError(
                message=f"GHL transient server error ({response.status_code}): {response.text[:300]}",
                status_code=response.status_code,
                response_body=response.text,
                endpoint=endpoint,
                method=method.upper(),
            )

        if not (200 <= response.status_code < 300):
            error_detail = response.text
            try:
                parsed_err = response.json()
                if isinstance(parsed_err, dict):
                    msg = parsed_err.get("message") or parsed_err.get("error")
                    if isinstance(msg, list):
                        error_detail = "; ".join(str(m) for m in msg)
                    elif msg:
                        error_detail = str(msg)
            except ValueError:
                pass

            raise GHLAPIError(
                message=error_detail or f"Unexpected status {response.status_code}",
                status_code=response.status_code,
                response_body=response.text,
                endpoint=endpoint,
                method=method.upper(),
            )

        if not response.text.strip():
            return {}

        try:
            return response.json()
        except ValueError as exc:
            raise GHLAPIError(
                message=f"Invalid JSON response from GHL: {response.text[:200]}",
                status_code=response.status_code,
                response_body=response.text,
                endpoint=endpoint,
                method=method.upper(),
            ) from exc

    # -------------------------------------------------------------------------
    # Custom Field Management (Contact & Opportunity Models)
    # -------------------------------------------------------------------------

    def get_custom_fields(
        self,
        force_refresh: bool = False,
        model: Literal["contact", "opportunity"] = "contact",
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch all custom fields for the configured GHL Location and object model
        (`contact` or `opportunity`).

        Returns:
            Dictionary mapping normalized field key (without 'contact.' or 'opportunity.'
            prefix) to its GHL custom field metadata object.
        """
        if model == "contact" and self._custom_fields_cache is not None and not force_refresh:
            return self._custom_fields_cache

        if model in self._custom_fields_by_model and not force_refresh:
            return self._custom_fields_by_model[model]

        if self.dry_run:
            cached = self._custom_fields_by_model.setdefault(model, {})
            if model == "contact":
                self._custom_fields_cache = cached
            return cached

        data = self._request(
            method="GET",
            endpoint=f"/locations/{self.location_id}/customFields",
            params={"model": model},
        )
        raw_fields: list[dict[str, Any]] = data.get("customFields", [])
        indexed: dict[str, dict[str, Any]] = {}

        for cf in raw_fields:
            field_key = str(cf.get("fieldKey", "")).strip()
            for prefix in ("contact.", "opportunity."):
                if field_key.startswith(prefix):
                    field_key = field_key[len(prefix) :]
            if field_key:
                indexed[field_key] = cf

        self._custom_fields_by_model[model] = indexed
        if model == "contact":
            self._custom_fields_cache = indexed

        logger.info(
            "Fetched %d existing %s custom fields from GHL location '%s'.",
            len(indexed),
            model,
            self.location_id,
        )
        return indexed

    def create_custom_field(
        self,
        definition: CustomFieldDefinition,
        model: Optional[Literal["contact", "opportunity"]] = None,
    ) -> dict[str, Any]:
        """
        Create a new custom field dynamically in the GHL sub-account via
        `POST /locations/{locationId}/customFields`.
        """
        target_model: Literal["contact", "opportunity"] = (
            model or definition.model or "contact"
        )

        if self.dry_run:
            created = {
                "id": f"cf_dryrun_{target_model}_{definition.normalized_key}",
                "name": definition.name,
                "fieldKey": f"{target_model}.{definition.normalized_key}",
                "dataType": definition.data_type,
                "model": target_model,
            }
            model_cache = self._custom_fields_by_model.setdefault(target_model, {})
            model_cache[definition.normalized_key] = created
            if target_model == "contact":
                if self._custom_fields_cache is None:
                    self._custom_fields_cache = model_cache
                else:
                    self._custom_fields_cache[definition.normalized_key] = created
            logger.info(
                "[DRY-RUN] Dynamically created GHL %s custom field '%s' (%s, type=%s) -> id=%s",
                target_model,
                definition.name,
                created["fieldKey"],
                definition.data_type,
                created["id"],
            )
            return created

        body: dict[str, Any] = {
            "name": definition.name,
            "dataType": definition.data_type,
            "model": target_model,
        }
        if definition.placeholder:
            body["placeholder"] = definition.placeholder
        if definition.options and definition.data_type in {
            "SINGLE_OPTIONS",
            "MULTIPLE_OPTIONS",
            "CHECKBOX",
        }:
            body["options"] = definition.options

        response = self._request(
            method="POST",
            endpoint=f"/locations/{self.location_id}/customFields",
            json_payload=body,
        )
        created_cf: dict[str, Any] = response.get("customField", response)
        model_cache = self._custom_fields_by_model.setdefault(target_model, {})
        model_cache[definition.normalized_key] = created_cf
        if target_model == "contact" and self._custom_fields_cache is not None:
            self._custom_fields_cache[definition.normalized_key] = created_cf

        logger.info(
            "Created GHL %s custom field '%s' (%s) with id='%s'.",
            target_model,
            definition.name,
            definition.qualified_key,
            created_cf.get("id"),
        )
        return created_cf

    def ensure_custom_fields(
        self,
        configured_fields: dict[str, CustomFieldDefinition],
        model: Optional[Literal["contact", "opportunity"]] = None,
    ) -> dict[str, str]:
        """
        Verify that all mapped custom fields exist in GoHighLevel, creating any missing
        fields dynamically.

        Returns:
            Dictionary mapping normalized `ghl_key` -> GHL custom field `id`.
        """
        resolved_ids: dict[str, str] = {}

        for source_col, definition in configured_fields.items():
            target_model: Literal["contact", "opportunity"] = (
                model or definition.model or "contact"
            )
            existing_by_key = self.get_custom_fields(model=target_model)
            existing_by_name = {
                str(cf.get("name", "")).strip().lower(): cf
                for cf in existing_by_key.values()
                if cf.get("name")
            }

            norm_key = definition.normalized_key
            matched_cf = existing_by_key.get(norm_key) or existing_by_name.get(
                definition.name.strip().lower()
            )

            if matched_cf and matched_cf.get("id"):
                resolved_ids[norm_key] = str(matched_cf["id"])
                logger.debug(
                    "Custom field '%s' (%s) matched existing GHL %s field id='%s'.",
                    source_col,
                    definition.qualified_key,
                    target_model,
                    resolved_ids[norm_key],
                )
            else:
                created_cf = self.create_custom_field(
                    definition, model=target_model
                )
                cf_id = str(created_cf.get("id", f"cf_{norm_key}"))
                resolved_ids[norm_key] = cf_id

        return resolved_ids

    # -------------------------------------------------------------------------
    # Contact Search & De-duplication
    # -------------------------------------------------------------------------

    def search_contact_by_email(self, email: str) -> Optional[dict[str, Any]]:
        """Search for an existing contact in GoHighLevel by exact email address."""
        clean_email = email.strip().lower()
        if not clean_email:
            return None

        if clean_email in self._email_contact_cache:
            return self._email_contact_cache[clean_email]

        if self.dry_run:
            return None

        response = self._request(
            method="GET",
            endpoint="/contacts/",
            params={
                "locationId": self.location_id,
                "query": clean_email,
                "limit": 20,
            },
        )
        contacts: list[dict[str, Any]] = response.get("contacts", [])
        for contact in contacts:
            candidate_email = str(contact.get("email") or "").strip().lower()
            if candidate_email == clean_email:
                self._cache_contact(contact)
                return contact

        return None

    def search_contact_by_phone(self, phone: str) -> Optional[dict[str, Any]]:
        """Search for an existing contact in GoHighLevel by standardized E.164 phone number."""
        target_digits = self._normalize_phone_digits(phone)
        if not target_digits:
            return None

        if target_digits in self._phone_contact_cache:
            return self._phone_contact_cache[target_digits]

        if self.dry_run:
            return None

        response = self._request(
            method="GET",
            endpoint="/contacts/",
            params={
                "locationId": self.location_id,
                "query": phone.strip(),
                "limit": 20,
            },
        )
        contacts: list[dict[str, Any]] = response.get("contacts", [])
        for contact in contacts:
            candidate_digits = self._normalize_phone_digits(contact.get("phone"))
            if candidate_digits and (
                candidate_digits == target_digits
                or (
                    len(candidate_digits) >= 10
                    and len(target_digits) >= 10
                    and candidate_digits[-10:] == target_digits[-10:]
                )
            ):
                self._cache_contact(contact)
                return contact

        return None

    def find_existing_contact(
        self,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        deduplicate_by: Optional[list[Literal["email", "phone"]]] = None,
    ) -> tuple[Optional[dict[str, Any]], Optional[Literal["email", "phone"]]]:
        """
        Look up an existing contact by email and/or phone in priority order.

        Returns:
            Tuple of (contact_dict_or_None, matched_by_field_or_None).
        """
        search_order: list[Literal["email", "phone"]] = (
            deduplicate_by if deduplicate_by is not None else ["email", "phone"]
        )

        for field_name in search_order:
            if field_name == "email" and email:
                found = self.search_contact_by_email(email)
                if found:
                    return found, "email"
            elif field_name == "phone" and phone:
                found = self.search_contact_by_phone(phone)
                if found:
                    return found, "phone"

        return None, None

    # -------------------------------------------------------------------------
    # Contact Create, Update & Upsert Operations
    # -------------------------------------------------------------------------

    @staticmethod
    def merge_tag_lists(
        existing_tags: list[str], incoming_tags: list[str]
    ) -> list[str]:
        """Merge existing GHL contact tags with incoming legacy CRM tags without duplicates."""
        merged: list[str] = []
        seen: set[str] = set()
        for tag in [*existing_tags, *incoming_tags]:
            cleaned = str(tag).strip()
            if cleaned and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                merged.append(cleaned)
        return merged

    def create_contact(self, payload: GHLContactPayload) -> dict[str, Any]:
        """Create a new contact in GoHighLevel via `POST /contacts/`."""
        body = payload.to_api_payload(
            location_id=self.location_id,
            include_location_id=True,
        )

        if self.dry_run:
            created_contact = {
                "id": f"ghl_dryrun_{uuid.uuid4().hex[:10]}",
                **body,
            }
            self._cache_contact(created_contact)
            return created_contact

        response = self._request(
            method="POST",
            endpoint="/contacts/",
            json_payload=body,
        )
        contact: dict[str, Any] = response.get("contact", response)
        self._cache_contact(contact)
        return contact

    def update_contact(
        self, contact_id: str, payload: GHLContactPayload
    ) -> dict[str, Any]:
        """Update an existing contact in GoHighLevel via `PUT /contacts/{contactId}`."""
        body = payload.to_api_payload(
            location_id=self.location_id,
            include_location_id=False,
        )

        if self.dry_run:
            updated_contact = {
                "id": contact_id,
                "locationId": self.location_id,
                **body,
            }
            self._cache_contact(updated_contact)
            return updated_contact

        response = self._request(
            method="PUT",
            endpoint=f"/contacts/{contact_id}",
            json_payload=body,
        )
        contact: dict[str, Any] = response.get("contact", response)
        self._cache_contact(contact)
        return contact

    def upsert_contact(
        self,
        payload: GHLContactPayload,
        merge_tags: bool = True,
        deduplicate_by: Optional[list[Literal["email", "phone"]]] = None,
    ) -> UpsertOperationResult:
        """
        De-duplicate by email/phone and either update the existing GHL contact
        (merging historical tags) or create a new contact.
        """
        existing_contact, matched_by = self.find_existing_contact(
            email=payload.email,
            phone=payload.phone,
            deduplicate_by=deduplicate_by,
        )

        if existing_contact and existing_contact.get("id"):
            contact_id = str(existing_contact["id"])
            if merge_tags:
                existing_tags = existing_contact.get("tags") or []
                if isinstance(existing_tags, list):
                    payload.tags = self.merge_tag_lists(existing_tags, payload.tags)

            updated = self.update_contact(contact_id=contact_id, payload=payload)
            logger.info(
                "Updated existing GHL contact id='%s' (matched by %s: %s).",
                contact_id,
                matched_by,
                payload.email or payload.phone,
            )
            return UpsertOperationResult(
                action="UPDATED",
                contact_id=contact_id,
                matched_by=matched_by,
                merged_tags=list(payload.tags),
                raw_response=updated,
            )

        created = self.create_contact(payload=payload)
        contact_id = str(created.get("id", "unknown_id"))
        logger.info(
            "Created new GHL contact id='%s' (%s).",
            contact_id,
            payload.email or payload.phone,
        )
        return UpsertOperationResult(
            action="CREATED",
            contact_id=contact_id,
            matched_by=None,
            merged_tags=list(payload.tags),
            raw_response=created,
        )

    def find_contact(
        self,
        email: Optional[str] = None,
        phone: Optional[str] = None,
    ) -> Optional[str]:
        """
        Resolve a GHL `contactId` by checking the in-memory `contact_id_cache` first,
        then falling back to `find_existing_contact(email, phone)`.
        """
        if email:
            clean_email = email.strip().lower()
            if clean_email in self.contact_id_cache:
                return self.contact_id_cache[clean_email]

        if phone:
            clean_phone = phone.strip()
            digits = self._normalize_phone_digits(clean_phone)
            if clean_phone in self.contact_id_cache:
                return self.contact_id_cache[clean_phone]
            if digits and digits in self.contact_id_cache:
                return self.contact_id_cache[digits]

        found_contact, _ = self.find_existing_contact(email=email, phone=phone)
        if found_contact and found_contact.get("id"):
            contact_id = str(found_contact["id"])
            self._cache_contact(found_contact)
            return contact_id

        return None

    # -------------------------------------------------------------------------
    # Opportunity (Deal) & Historical Note Operations
    # -------------------------------------------------------------------------

    def upsert_opportunity(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Upsert an Opportunity (Deal) into GoHighLevel via `POST /opportunities/upsert`.
        """
        body = dict(payload)
        if "locationId" not in body and self.location_id:
            body["locationId"] = self.location_id

        if self.dry_run:
            opp_id = f"opp_dryrun_{uuid.uuid4().hex[:10]}"
            simulated = {
                "opportunity": {
                    "id": opp_id,
                    **body,
                },
                "new": True,
            }
            logger.info(
                "[DRY-RUN] Upserted opportunity '%s' -> id='%s' (contactId='%s').",
                body.get("name"),
                opp_id,
                body.get("contactId"),
            )
            return simulated

        response = self._request(
            method="POST",
            endpoint="/opportunities/upsert",
            json_payload=body,
        )
        opp_obj = response.get("opportunity", response)
        logger.info(
            "Upserted GHL opportunity '%s' -> id='%s' (contactId='%s').",
            body.get("name"),
            opp_obj.get("id"),
            body.get("contactId"),
        )
        return response

    def create_contact_note(
        self,
        contact_id: str,
        body: str,
        user_id: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Attach a historical note to a GoHighLevel contact via
        `POST /contacts/{contact_id}/notes`.
        """
        clean_contact_id = contact_id.strip()
        if not clean_contact_id:
            raise ValueError("contact_id is required to create a contact note.")

        note_payload: dict[str, Any] = {"body": body}
        if user_id:
            note_payload["userId"] = user_id

        if self.dry_run:
            note_id = f"note_dryrun_{uuid.uuid4().hex[:10]}"
            simulated = {
                "note": {
                    "id": note_id,
                    "contactId": clean_contact_id,
                    **note_payload,
                }
            }
            logger.info(
                "[DRY-RUN] Created note id='%s' on contactId='%s'.",
                note_id,
                clean_contact_id,
            )
            return simulated

        response = self._request(
            method="POST",
            endpoint=f"/contacts/{clean_contact_id}/notes",
            json_payload=note_payload,
        )
        note_obj = response.get("note", response)
        logger.info(
            "Created GHL note id='%s' on contactId='%s'.",
            note_obj.get("id"),
            clean_contact_id,
        )
        return response
