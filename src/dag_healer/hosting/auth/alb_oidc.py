"""The ``alb_oidc`` backend: the users an AWS Application Load Balancer authenticated.

For a listener rule with the ``authenticate-oidc`` (or Cognito) action. The ALB signs the user
claims of the identity provider into the ``x-amzn-oidc-data`` header, an ES256 JWT whose header
carries ``kid``, ``signer`` (the ARN of the load balancer) and ``exp``. A request is accepted
when, in this order:

1. it carries exactly one ``x-amzn-oidc-data`` header, of at most :data:`MAX_TOKEN_LENGTH`
   characters;
2. the JWT header, read before anything is verified, has ``alg`` ``ES256``, a ``kid`` made of
   letters, digits and dashes (it becomes part of the key URL), ``signer`` equal to
   ``serve.auth.alb_arn`` and an ``exp`` in the future. Nothing is fetched before these pass;
3. the public key of that kid verifies the signature, and PyJWT finds the payload valid (its
   ``exp`` included). The key comes from :func:`alb_key_url` (the regional endpoint
   ``https://public-keys.auth.elb.{region}.amazonaws.com/{kid}``, an S3 bucket in GovCloud),
   fetched once per kid and cached. The ALB adds base64 padding to the segments of the JWT
   (RFC 7515 has none), so the padding-stripped token (:func:`strip_jwt_padding`) is verified
   first and, only when that fails and the token had padding, the token as received. Which form
   the ALB signs is an inference; trying both is safe, because the signature must verify.

The user is the ``user_claim`` claim (``email`` by default), else ``sub``. An ``email`` or a
``phone_number`` counts only when the identity provider marks it verified (``email_verified`` /
``phone_number_verified`` true, or the string ``"true"`` Cognito sends): many providers let users
set their own address, and an unverified one could name somebody else. ``sub`` is assigned by the
provider, so ``user:`` rules are safest on ``sub`` or on groups the provider manages. The groups
come from ``groups_claim`` when it is set: a list of strings or a comma-separated string (names
that are not usable strings are left out, which only removes permissions). The verified claims
are kept in ``Principal.claims`` and never logged. ``x-amzn-oidc-identity`` and
``x-amzn-oidc-accesstoken`` are not signed and are never read. The targets must accept traffic
from the load balancer's security group only.

Failures answer 401 ``Not authenticated`` without saying which check failed; a kid the key
endpoint refuses (4xx) is a 401 too. A key endpoint that cannot be reached, fails (5xx),
throttles (408, 429) or answers something that is not a P-256 public key gives 503
:data:`KEY_UNAVAILABLE_DETAIL`. A kid whose key could not be had is not asked for again during
:data:`FAILURE_TTL` seconds. One key is fetched at a time, and at most :data:`MAX_KEY_FETCHES`
within :data:`KEY_FETCH_WINDOW` seconds: past that, a request with another new kid answers 503
without a fetch. PyJWT (``pip install 'dag-healer[serve]'``), cryptography and httpx are imported
only when used.
"""

from __future__ import annotations

import logging
import math
import re
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping, Sequence
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException, Request, status

from dag_healer.api.auth import Principal
from dag_healer.hosting.auth.base import (
    AuthConfigError,
    StandaloneAuthBackend,
    clean_name,
    not_authenticated,
)

if TYPE_CHECKING:
    from dag_healer.api.auth import Authorizer

log = logging.getLogger(__name__)

DATA_HEADER = "x-amzn-oidc-data"
ALGORITHM = "ES256"
KEY_UNAVAILABLE_DETAIL = "The load balancer signing key cannot be fetched"
PYJWT_HINT = "the alb_oidc backend needs PyJWT with its crypto extra: pip install 'dag-healer[serve]'"
#: The longest ``x-amzn-oidc-data`` header accepted (ALB user claims are limited to about 11 KB).
MAX_TOKEN_LENGTH = 16 * 1024
#: The largest key document read from the key endpoint (a P-256 PEM key is about 180 bytes).
MAX_KEY_BYTES = 16 * 1024
#: Seconds allowed for each step of a key fetch (connect, read, write), and for its whole body.
KEY_FETCH_TIMEOUT = 5.0
#: How many public keys (kids) are kept, least recently used first out.
KEY_CACHE_SIZE = 32
#: Seconds during which a kid whose key could not be had is not asked for again.
FAILURE_TTL = 60.0
#: How many such kids are remembered.
FAILURE_CACHE_SIZE = 256
#: The most key fetches a backend starts within :data:`KEY_FETCH_WINDOW` seconds; past that, a
#: request with a new kid answers 503 without a fetch.
MAX_KEY_FETCHES = 10
KEY_FETCH_WINDOW = 60.0
#: The ARN partitions whose key endpoints :func:`alb_key_url` knows.
SUPPORTED_PARTITIONS = ("aws", "aws-us-gov")
#: Answers of the key endpoint that mean "try again later" (timeout, throttling), not "unknown kid".
_RETRY_LATER = frozenset({408, 429})
#: Claims many identity providers let users set themselves, and the claim saying it was verified.
_VERIFIED_BY: Mapping[str, str] = MappingProxyType(
    {"email": "email_verified", "phone_number": "phone_number_verified"}
)

_KID = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,127}")
_REGION_PATTERN = r"[a-z]{2}(?:-[a-z]+)+-\d{1,2}"
_REGION = re.compile(_REGION_PATTERN)
_ALB_ARN = re.compile(
    rf"arn:(?P<partition>aws[a-z-]*):elasticloadbalancing:(?P<region>{_REGION_PATTERN}):\d{{12}}"
    r":loadbalancer/app/[^/\s]+/[^/\s]+"
)
_PEM_BEGIN = "-----BEGIN PUBLIC KEY-----"
_PEM_END = "-----END PUBLIC KEY-----"
#: The token is bound to this deployment by its signed header (``signer``); the audience of the
#: identity provider's user info is not ours to check, so PyJWT must not refuse a token that has one.
_DECODE_OPTIONS: dict[str, Any] = {"verify_aud": False}


class SigningKeyError(Exception):
    """The public key of a kid could not be had."""


class SigningKeyNotFound(SigningKeyError):
    """The key endpoint refused the kid (4xx): the token cannot be verified (401)."""


class SigningKeyUnavailable(SigningKeyError):
    """The key endpoint is unreachable or failing, or gave no P-256 public key (503)."""


def alb_key_url(region: str, kid: str) -> str:
    """The URL of the public key ``kid`` of the load balancers of ``region``.

    ``https://public-keys.auth.elb.{region}.amazonaws.com/{kid}``, and in GovCloud (``us-gov-*``)
    ``https://s3-{region}.amazonaws.com/aws-elb-public-keys-prod-{region}/{kid}``. ``ValueError``
    for a region or a kid of an unexpected shape, which could change what the URL points at.
    """
    if not isinstance(region, str) or not _REGION.fullmatch(region):
        raise ValueError(f"invalid AWS region {region!r}")
    if not isinstance(kid, str) or not _KID.fullmatch(kid):
        raise ValueError("invalid key id")
    if region.startswith("us-gov-"):
        return f"https://s3-{region}.amazonaws.com/aws-elb-public-keys-prod-{region}/{kid}"
    return f"https://public-keys.auth.elb.{region}.amazonaws.com/{kid}"


def strip_jwt_padding(token: str) -> str:
    """``token`` without the trailing ``=`` of its base64 segments (``a==.b=.c`` -> ``a.b.c``)."""
    return ".".join(segment.rstrip("=") for segment in token.split("."))


def fetch_public_key(url: str, *, timeout: float = KEY_FETCH_TIMEOUT) -> str:
    """GET the PEM public key at ``url``: TLS verified, no redirect followed, at most 16 KB.

    :class:`SigningKeyNotFound` for a 4xx answer but 408 and 429. :class:`SigningKeyUnavailable`
    for those two (the endpoint asks to try later), a network error or a timeout, any other
    status but 200, a body larger than :data:`MAX_KEY_BYTES` or slower than ``timeout``, and a
    body that is not a PEM public key.
    """
    import httpx

    deadline = time.monotonic() + timeout
    body = bytearray()
    try:
        with (
            httpx.Client(follow_redirects=False, verify=True, timeout=timeout) as client,
            client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response,
        ):
            if response.status_code in _RETRY_LATER:
                raise SigningKeyUnavailable(f"HTTP {response.status_code}")
            if 400 <= response.status_code < 500:
                raise SigningKeyNotFound(f"HTTP {response.status_code}")
            if response.status_code != 200:
                raise SigningKeyUnavailable(f"HTTP {response.status_code}")
            for chunk in response.iter_raw():
                body += chunk
                if len(body) > MAX_KEY_BYTES:
                    raise SigningKeyUnavailable("the key document is too large")
                if time.monotonic() > deadline:
                    raise SigningKeyUnavailable("the key endpoint is too slow")
    except httpx.HTTPError as exc:
        raise SigningKeyUnavailable(type(exc).__name__) from None
    try:
        text = bytes(body).decode("ascii").strip()
    except UnicodeDecodeError:
        raise SigningKeyUnavailable("the key document is not a PEM public key") from None
    if not (text.startswith(_PEM_BEGIN) and text.endswith(_PEM_END)):
        raise SigningKeyUnavailable("the key document is not a PEM public key")
    return text


def load_public_key(pem: Any) -> Any:
    """The P-256 public key of the PEM document ``pem``; :class:`SigningKeyUnavailable` otherwise."""
    from cryptography.exceptions import UnsupportedAlgorithm
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.serialization import load_pem_public_key

    if not isinstance(pem, str) or _PEM_BEGIN not in pem:
        raise SigningKeyUnavailable("the key document is not a PEM public key")
    try:
        key = load_pem_public_key(pem.strip().encode("ascii"))
    except (ValueError, TypeError, UnicodeError, UnsupportedAlgorithm):
        raise SigningKeyUnavailable("the key document is not a PEM public key") from None
    if not isinstance(key, ec.EllipticCurvePublicKey) or not isinstance(key.curve, ec.SECP256R1):
        raise SigningKeyUnavailable("the key is not a P-256 (ES256) public key")
    return key


class _SigningKeys:
    """The load balancer's public keys by kid: a bounded LRU, a short memory of failed kids and a
    bound on fetches.

    Thread-safe. One key is fetched at a time, outside the lock. A request for the kid being
    fetched waits for that fetch (at most :data:`KEY_FETCH_TIMEOUT` seconds); a request for another
    new kid meanwhile, or once :data:`MAX_KEY_FETCHES` fetches started within
    :data:`KEY_FETCH_WINDOW` seconds, gets :class:`SigningKeyUnavailable` at once. Those refusals
    are not remembered as failures of the kid. A client that reaches the targets directly with
    made-up kids therefore gets neither a fetch per request nor the threadpool of the sync routes.
    """

    def __init__(self, region: str, fetcher: Callable[[str], str], clock: Callable[[], float]) -> None:
        self._region = region
        self._fetcher = fetcher
        self._clock = clock
        self._lock = threading.Lock()
        self._fetch_done = threading.Condition(self._lock)
        self._keys: OrderedDict[str, Any] = OrderedDict()
        self._failures: OrderedDict[str, tuple[float, bool]] = OrderedDict()  # kid -> (until, unavailable)
        self._fetching: str | None = None  # the kid whose key is being fetched
        self._fetch_starts: deque[float] = deque()  # clock times of the recent fetches
        self._quiet_until = 0.0  # no new "too many fetches" warning before this clock time

    def get(self, kid: str) -> Any:
        """The key of ``kid``: :class:`SigningKeyNotFound` (401) or :class:`SigningKeyUnavailable` (503)."""
        with self._lock:
            key = self._known(kid)
            if key is not None:
                return key
            self._start_fetch(kid)
        try:
            return self._fetch(kid)
        finally:
            with self._lock:
                self._fetching = None
                self._fetch_done.notify_all()

    def _known(self, kid: str) -> Any:
        """The cached key of ``kid``, else ``None`` once no other fetch runs (lock held).

        Raises the remembered failure of ``kid``. Waits for a fetch of ``kid`` that is running;
        :class:`SigningKeyUnavailable` while another kid is fetched or when the wait times out.
        """
        deadline = time.monotonic() + KEY_FETCH_TIMEOUT
        while True:
            key = self._keys.get(kid)
            if key is not None:
                self._keys.move_to_end(kid)
                return key
            failure = self._failures.get(kid)
            if failure is not None:
                if self._clock() < failure[0]:
                    if failure[1]:
                        raise SigningKeyUnavailable("the key could not be fetched a moment ago")
                    raise SigningKeyNotFound("the key endpoint refused this key id a moment ago")
                del self._failures[kid]
            if self._fetching is None:
                return None
            if self._fetching != kid:
                raise SigningKeyUnavailable("another signing key is being fetched")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SigningKeyUnavailable("the signing key is still being fetched")
            self._fetch_done.wait(remaining)

    def _start_fetch(self, kid: str) -> None:
        """Claim the one fetch for ``kid`` within the fetch budget (lock held)."""
        now = self._clock()
        while self._fetch_starts and self._fetch_starts[0] <= now - KEY_FETCH_WINDOW:
            self._fetch_starts.popleft()
        if len(self._fetch_starts) >= MAX_KEY_FETCHES:
            if now >= self._quiet_until:
                self._quiet_until = now + KEY_FETCH_WINDOW
                log.warning(
                    "dag-healer: %d load balancer signing keys were fetched (%s) within %d s: requests "
                    "with another new key id answer 503 for now",
                    MAX_KEY_FETCHES,
                    self._region,
                    int(KEY_FETCH_WINDOW),
                )
            raise SigningKeyUnavailable("too many signing key fetches")
        self._fetch_starts.append(now)
        self._fetching = kid

    def _fetch(self, kid: str) -> Any:
        """Fetch, check and cache the key of ``kid``, remembering a failure (lock not held)."""
        try:
            key = load_public_key(self._fetcher(alb_key_url(self._region, kid)))
        except SigningKeyNotFound:
            log.debug("dag-healer: the load balancer key endpoint (%s) refused a key id", self._region)
            self._failed(kid, unavailable=False)
            raise
        except Exception as exc:
            reason = str(exc) if isinstance(exc, SigningKeyUnavailable) else type(exc).__name__
            log.warning("dag-healer: cannot fetch a load balancer signing key (%s): %s", self._region, reason)
            self._failed(kid, unavailable=True)
            if isinstance(exc, SigningKeyUnavailable):
                raise
            raise SigningKeyUnavailable(reason) from None
        with self._lock:
            self._failures.pop(kid, None)
            self._keys[kid] = key
            self._keys.move_to_end(kid)
            while len(self._keys) > KEY_CACHE_SIZE:
                self._keys.popitem(last=False)
        return key

    def _failed(self, kid: str, *, unavailable: bool) -> None:
        with self._lock:
            self._failures[kid] = (self._clock() + FAILURE_TTL, unavailable)
            self._failures.move_to_end(kid)
            while len(self._failures) > FAILURE_CACHE_SIZE:
                self._failures.popitem(last=False)


def _require_pyjwt() -> None:
    """:class:`AuthConfigError` with the pip hint unless PyJWT and its crypto backend import."""
    try:
        from jwt.algorithms import has_crypto
    except ImportError:
        raise AuthConfigError(PYJWT_HINT) from None
    if not has_crypto:
        raise AuthConfigError(PYJWT_HINT)


def _rejected(reason: str) -> HTTPException:
    log.debug("dag-healer: x-amzn-oidc-data rejected: %s", reason)
    return not_authenticated()


def _in_future(exp: Any, now: float) -> bool:
    """True when ``exp`` (a JWT ``exp``: seconds since the epoch) is a finite number after ``now``."""
    if isinstance(exp, bool) or not isinstance(exp, (int, float)):
        return False
    if isinstance(exp, float) and not math.isfinite(exp):
        return False
    return exp > now


def _unverified_header(token: str) -> dict[str, Any] | None:
    import jwt

    try:
        header = jwt.get_unverified_header(token)
    except (jwt.PyJWTError, ValueError, TypeError):
        return None
    return header if isinstance(header, dict) else None


def _verified_claims(token: str, unpadded: str, key: Any, header: Mapping[str, Any]) -> dict[str, Any] | None:
    """The payload of ``token`` once its signature verifies with ``key``; ``None`` otherwise.

    The padding-stripped form first; the form as received only when the token had padding and
    the stripped form's signature does not verify. The verified header must be the one checked.
    """
    import jwt
    from jwt import api_jwt

    for form in (unpadded,) if unpadded == token else (unpadded, token):
        try:
            decoded = api_jwt.decode_complete(form, key, algorithms=[ALGORITHM], options=_DECODE_OPTIONS)
        except jwt.InvalidSignatureError:
            continue
        except (jwt.PyJWTError, ValueError, TypeError):
            return None
        payload = decoded.get("payload")
        if decoded.get("header") != header or not isinstance(payload, dict):
            return None
        return payload
    return None


def _verified(claims: Mapping[str, Any], name: str) -> bool:
    """False when the claim ``name`` needs a verification flag (:data:`_VERIFIED_BY`) that is not true.

    True is the JSON ``true`` or the string ``"true"`` in any case (Cognito sends strings).
    """
    flag = _VERIFIED_BY.get(name)
    if flag is None:
        return True
    value = claims.get(flag)
    return value is True or (isinstance(value, str) and value.lower() == "true")


def _claim_user(claims: Mapping[str, Any], user_claim: str) -> str | None:
    """``claims[user_claim]``, else ``claims["sub"]``, when it is a usable name.

    An ``email`` or ``phone_number`` the identity provider has not verified is skipped: a user
    may have set it to somebody else's.
    """
    for name in (user_claim, "sub"):
        user = clean_name(claims.get(name))
        if user is None:
            continue
        if not _verified(claims, name):
            log.debug("dag-healer: the %s claim is not verified by the identity provider; trying sub", name)
            continue
        return user
    return None


def _claim_groups(value: Any) -> frozenset[str]:
    """The groups of a groups claim: a list of strings or a comma-separated string."""
    if isinstance(value, str):
        items: list[Any] = value.split(",")
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        return frozenset()
    return frozenset(group for group in map(clean_name, items) if group is not None)


class AlbOidcAuthBackend(StandaloneAuthBackend):
    """``serve.auth.backend: alb_oidc``: the ALB-signed ``x-amzn-oidc-data`` JWT.

    ``key_fetcher`` (URL -> PEM text, raising :class:`SigningKeyNotFound` or
    :class:`SigningKeyUnavailable`) replaces :func:`fetch_public_key` and ``clock`` the time
    source of the header ``exp`` check, of the failure memory and of the fetch budget (tests).
    :class:`~dag_healer.hosting.auth.base.AuthConfigError` without a valid ``alb_arn`` and
    ``region`` (the region of the ARN), for an ARN outside :data:`SUPPORTED_PARTITIONS` (whose key
    endpoint is unknown, so every request would answer 503), with an empty ``user_claim`` or
    without PyJWT[crypto].
    """

    name = "alb_oidc"

    def __init__(
        self,
        *,
        alb_arn: str,
        region: str,
        user_claim: str = "email",
        groups_claim: str | None = None,
        authorizer: Authorizer,
        origins: Sequence[str] = (),
        key_fetcher: Callable[[str], str] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        super().__init__(authorizer=authorizer, origins=origins)
        match = _ALB_ARN.fullmatch(alb_arn) if isinstance(alb_arn, str) else None
        if match is None:
            raise AuthConfigError(
                "the alb_oidc backend requires serve.auth.alb_arn, the ARN of the Application Load "
                "Balancer that signs the users' JWT "
                "(arn:aws:elasticloadbalancing:<region>:<account>:loadbalancer/app/<name>/<id>)"
            )
        if not isinstance(region, str) or not _REGION.fullmatch(region):
            raise AuthConfigError(
                "the alb_oidc backend requires serve.auth.region, an AWS region such as eu-west-1"
            )
        if region != match.group("region"):
            raise AuthConfigError(
                f"serve.auth.region {region!r} is not the region of serve.auth.alb_arn "
                f"({match.group('region')!r})"
            )
        partition = match.group("partition")
        if partition not in SUPPORTED_PARTITIONS:
            raise AuthConfigError(
                f"serve.auth.alb_arn is in the {partition!r} partition: the alb_oidc backend knows the "
                f"signing key endpoints of the {' and '.join(SUPPORTED_PARTITIONS)} partitions only"
            )
        if (partition == "aws-us-gov") != region.startswith("us-gov-"):
            raise AuthConfigError(f"serve.auth.alb_arn: the {partition!r} partition has no region {region!r}")
        if not isinstance(user_claim, str) or not user_claim.strip():
            raise AuthConfigError("serve.auth.user_claim must name a claim, such as email")
        if groups_claim is not None and (not isinstance(groups_claim, str) or not groups_claim.strip()):
            raise AuthConfigError("serve.auth.groups_claim must name a claim, or be left out")
        _require_pyjwt()
        self._alb_arn = alb_arn
        self._region = region
        self._user_claim = user_claim.strip()
        self._groups_claim = groups_claim.strip() if groups_claim is not None else None
        self._clock = clock
        self._keys = _SigningKeys(region, key_fetcher or fetch_public_key, clock)

    @property
    def alb_arn(self) -> str:
        """The load balancer whose signature is accepted."""
        return self._alb_arn

    @property
    def region(self) -> str:
        """The region of the load balancer (and of its key endpoint)."""
        return self._region

    def user_dependency(self) -> Callable[..., Any]:
        """A plain dependency (FastAPI runs it in the threadpool: a key fetch blocks)."""
        return self._current_user

    def _current_user(self, request: Request) -> Principal:
        values = request.headers.getlist(DATA_HEADER)
        token = values[0].strip() if len(values) == 1 else ""
        if not token or len(token) > MAX_TOKEN_LENGTH:
            raise _rejected("not exactly one x-amzn-oidc-data header of a usable size")
        unpadded = strip_jwt_padding(token)
        header = _unverified_header(unpadded)
        if header is None:
            raise _rejected("the JWT is malformed")
        problem = self._header_problem(header)
        if problem is not None:
            raise _rejected(problem)
        try:
            key = self._keys.get(header["kid"])
        except SigningKeyNotFound:
            raise _rejected("the key endpoint does not know the key id") from None
        except SigningKeyUnavailable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=KEY_UNAVAILABLE_DETAIL
            ) from None
        claims = _verified_claims(token, unpadded, key, header)
        if claims is None:
            raise _rejected("the signature or the claims do not verify")
        user = _claim_user(claims, self._user_claim)
        if user is None:
            raise _rejected("neither the user claim (verified) nor sub is a usable name")
        groups = _claim_groups(claims.get(self._groups_claim)) if self._groups_claim else frozenset()
        return Principal(id=user, name=user, groups=groups, claims=MappingProxyType(dict(claims)))

    def _header_problem(self, header: Mapping[str, Any]) -> str | None:
        """Why the unverified JWT header is refused before any key is fetched; ``None`` if it is not."""
        if header.get("alg") != ALGORITHM:
            return "the JWT algorithm is not ES256"
        kid = header.get("kid")
        if not isinstance(kid, str) or not _KID.fullmatch(kid):
            return "the JWT key id has an unexpected shape"
        if header.get("signer") != self._alb_arn:
            return "the JWT signer is not serve.auth.alb_arn"
        if not _in_future(header.get("exp"), self._clock()):
            return "the JWT header exp is missing or past"
        return None


__all__ = [
    "ALGORITHM",
    "DATA_HEADER",
    "FAILURE_TTL",
    "KEY_FETCH_WINDOW",
    "KEY_UNAVAILABLE_DETAIL",
    "MAX_KEY_FETCHES",
    "PYJWT_HINT",
    "SUPPORTED_PARTITIONS",
    "AlbOidcAuthBackend",
    "SigningKeyError",
    "SigningKeyNotFound",
    "SigningKeyUnavailable",
    "alb_key_url",
    "fetch_public_key",
    "load_public_key",
    "strip_jwt_padding",
]
