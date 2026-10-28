"""Claude on Amazon Bedrock: the ``bedrock`` LLM provider (``llm.provider: bedrock``).

:class:`BedrockProvider` is the :class:`~dag_healer.agent.llm.AnthropicProvider` on the SDK's
``anthropic.AnthropicBedrock`` client: the same prompts, tools, token budget, refusal handling
and repair loop (``client.beta.messages.tool_runner``, which resends the whole conversation every
turn on Bedrock), sent to Claude on Amazon Bedrock in the operator's AWS account.

Endpoint. The provider always calls the ``bedrock-runtime`` endpoint: InvokeModel
(``POST /model/<llm.model>/invoke``), signed with SigV4 for the service ``bedrock``, so the IAM
action is ``bedrock:InvokeModel``. AWS recommends this endpoint for new applications, and only
there are the cross-Region (geographic and global) inference profiles, model invocation logging
and Guardrails. Everything dag-healer sends is supported on it: structured outputs
(``output_config.format``), ``output_config.effort``, explicit ``cache_control`` breakpoints,
client tools and non-streaming requests. ``bedrock-mantle`` (``AnthropicBedrockMantle``) is not
used: it needs another IAM action (``bedrock-mantle:CreateInference``) and has no cross-Region
inference. Nothing is streamed, so ``bedrock:InvokeModelWithResponseStream`` is not needed.

Settings. The AWS credentials come from the default AWS chain (the environment, a shared
profile, a container or EC2 instance role) or a Bedrock API key in ``AWS_BEARER_TOKEN_BEDROCK``,
never from ``healer.yaml``. The region is ``llm.aws_region``, else ``AWS_REGION`` /
``AWS_DEFAULT_REGION``, else the AWS profile's (``llm.aws_profile`` or the default chain's);
``llm.endpoint_url`` (else ``ANTHROPIC_BEDROCK_BASE_URL``) replaces the endpoint, for a VPC
endpoint or a gateway. The region names the host that receives the signed requests, so wherever
it comes from it must be an AWS Region code (:data:`dag_healer.config.AWS_REGION_CODE`), and
``ANTHROPIC_BEDROCK_BASE_URL`` follows the rules of ``llm.endpoint_url``
(:func:`dag_healer.config.valid_endpoint_url`: https, http only for a loopback host, no
credentials, query or fragment). The provider hands the region it found (``llm.aws_region`` or
a variable) and the endpoint to the client itself; only without both does the SDK read the
profile's region, which the provider then checks. Empty ``AWS_BEARER_TOKEN_BEDROCK``,
``AWS_REGION`` and ``AWS_DEFAULT_REGION`` values count as unset (botocore still reads an empty
``AWS_DEFAULT_REGION`` as the region, which hides the profile's). A value of spaces only in
``AWS_REGION`` or ``AWS_DEFAULT_REGION`` while no region is set otherwise, and an empty
``ANTHROPIC_BEDROCK_BASE_URL``, are refused: the SDK would use them as they are. The whitespace
around ``AWS_BEARER_TOKEN_BEDROCK`` and around the AWS keys of the environment
(``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and the session token, ``AWS_SECURITY_TOKEN``
else ``AWS_SESSION_TOKEN``, in botocore's order) falls away: the SDK and botocore would send it
in a header, which the HTTP library refuses, and the provider hands the stripped values to the
client itself. Whitespace, a control character or a non-ASCII character within the value of one
of those five variables is refused without the value (:data:`CREDENTIAL_NOT_PRINTABLE`).
``llm.model`` must be a Bedrock model ID, inference profile ID or ARN: a Claude API model ID
such as the default ``claude-opus-5-5`` is refused before any request
(:func:`model_alias_problem`), since choosing between an in-Region, geographic or global ID
decides where the data is processed. :func:`settings_problems` holds these rules for the
provider and for ``dag-healer doctor``.

Redirects and proxies. The client that the provider builds never follows a redirect: a 3xx
answer ends the call after that one request (:data:`REDIRECTED`), so neither the request, with
its signature and the session token of temporary credentials or with the Bedrock API key, nor
the prompt ever reaches another host. It follows the proxy variables of the environment, except
for an ``http`` endpoint (a loopback host), whose cleartext requests never go through a proxy.

Credentials in messages. No message of this module quotes a credential
(:func:`hide_credentials`). The values of the credentials that the client authenticates with
(its Bedrock API key; the access key, secret key and session token of the SDK's boto3 session,
which may come from a profile or a role and sit in no variable) and of the environment's
credential variables (:data:`_SECRET_ENVS`), 8 characters or more, are hidden as they are,
URL-encoded or JSON-escaped, and so is any run of 16 or more of their characters; a message in
which one can still be read once its spaces are taken out is hidden whole. So are access key
IDs, the value after a credential's name (a header, a JSON or XML field, a setting of the shared
AWS files, a parameter) and long random-looking strings. AWS's own message in an error answer is
shown only when it talks about none of the credentials (:data:`_CREDENTIAL_TALK`: a token, a
signature, a secret, an API key, a password, a bearer token, authorization, an ``x-amz-``
header); otherwise :data:`MESSAGE_LEFT_OUT` stands in for it, after the AWS error type of the
answer when there is one. An answer without a JSON message is never quoted. The error type
(``x-amzn-ErrorType``, else ``__type``; the name alone, before ``:`` and after ``#``) is shown
only when it is shaped like AWS's names of errors (``AccessDeniedException``: capitalised words
of letters, 3 to 64 in all) and holds no credential; :data:`UNKNOWN_ERROR_TYPE` stands in for
any other value. A refusal's message, and the log line of a refusal in a repair session, name
its category only when it is shaped like one of Claude's (``cyber``: lower-case letters and
``_``, 32 at most) and holds no credential (the check that both providers share). The log line
at the end of a repair session names its stop reason only when it is one of Claude's
(``other`` stands in for any other value), and no server-side fallback is ever logged: the SDK
builds an answer without checking these fields, and Bedrock has no fallbacks.

What degrades on Bedrock:

* no server-side refusal fallbacks (the Claude API only): no beta and no ``fallbacks`` field is
  ever sent; ``llm.fallbacks: true`` written in the file logs :data:`FALLBACKS_IGNORED` once;
* when the endpoint rejects structured outputs (a 400 about them) or answers without applying
  them, the classification asks for a JSON answer instead (:data:`JSON_ANSWER_INSTRUCTION`, one
  append-only retry with :data:`JSON_RETRY_INSTRUCTION`) and validates it with the same pydantic
  model, for the rest of the provider's life. The tokens of every billed answer count: when the
  classification fails after some, the :class:`LLMUnavailable` or :class:`LLMRefusal` it raises
  carries them in ``tokens_used``.

boto3 and botocore (the ``aws`` extra) are reached only through the anthropic SDK: this module
checks that they are installed with :func:`importlib.util.find_spec` and recognises botocore's
errors by the module of their class, so importing it needs neither of them (nor the SDK).
botocore logs the signed request and the canonical request of its signature, with the session
token of temporary credentials, at DEBUG: the provider installs
:func:`dag_healer.storage.s3.quiet_botocore_payloads`, which drops those records, before any
client is used.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import re
from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Any
from urllib.parse import unquote, urlsplit

import pydantic

from dag_healer.agent.budget import usage_tokens
from dag_healer.agent.llm import (
    _MAX_TOKENS_TOO_LARGE,
    AnthropicProvider,
    Classification,
    LLMRefusal,
    LLMUnavailable,
    _known_stop_reason,
    _message_text,
    excerpt,
)
from dag_healer.config import AWS_REGION_CODE, valid_endpoint_url
from dag_healer.masking import hide_secrets, holds_secret, known_secrets
from dag_healer.storage.s3 import quiet_botocore_payloads

logger = logging.getLogger(__name__)

API_KEY_ENV = "AWS_BEARER_TOKEN_BEDROCK"
BASE_URL_ENV = "ANTHROPIC_BEDROCK_BASE_URL"
#: The variables of the region, in the order the provider reads them (after ``llm.aws_region``).
REGION_ENVS: tuple[str, ...] = ("AWS_REGION", "AWS_DEFAULT_REGION")
AWS_EXTRA_INSTALL = 'pip install "dag-healer[aws]"'
AWS_SDK_MISSING = f"the bedrock provider needs boto3 and botocore: {AWS_EXTRA_INSTALL}"
NO_REGION = (
    "no AWS region for the bedrock provider: set llm.aws_region, AWS_REGION or AWS_DEFAULT_REGION, "
    "or a region in the AWS profile"
)
#: Where the region came from when none of llm.aws_region, AWS_REGION and AWS_DEFAULT_REGION is set.
PROFILE_REGION = "the region of the AWS profile"
#: A region that is no AWS Region code (``{source}``: llm.aws_region, a variable or the profile's).
REGION_INVALID = "{source} is not an AWS region such as eu-west-1"
#: The SDK reads a value of spaces only as the region when nothing else sets one.
REGION_BLANK = (
    "{name} is set but blank, and the AWS SDK would take it as the region: unset it, or set llm.aws_region"
)
NO_CREDENTIALS = (
    "no AWS credentials for the bedrock provider: attach an instance role to the worker's host "
    "(in a container on EC2, set the IMDSv2 hop limit to 2), or set AWS_PROFILE or "
    "AWS_BEARER_TOKEN_BEDROCK (docs/bedrock.md)"
)
PROFILE_AND_API_KEY = "llm.aws_profile and AWS_BEARER_TOKEN_BEDROCK are both set: unset one of them"
#: The SDK refuses a profile next to any Bedrock API key value, even an empty one; the profile is
#: then handed to the client after it is built, which needs the region from elsewhere.
EMPTY_API_KEY_WITH_PROFILE = (
    "llm.aws_profile is set and AWS_BEARER_TOKEN_BEDROCK is set but empty: unset "
    "AWS_BEARER_TOKEN_BEDROCK, or set llm.aws_region or AWS_REGION"
)
BASE_URL_EMPTY = "ANTHROPIC_BEDROCK_BASE_URL is set but empty: unset it, or set llm.endpoint_url"
BASE_URL_INVALID = (
    "ANTHROPIC_BEDROCK_BASE_URL is not an https URL (http only for a loopback host) without "
    "credentials, query or fragment: fix or unset it, or set llm.endpoint_url"
)
ENDPOINT_URL_INVALID = (
    "llm.endpoint_url is not an https URL (http only for a loopback host) without credentials, "
    "query or fragment"
)
MODEL_NOT_PRINTABLE = (
    "llm.model holds whitespace or a control character; with llm.provider bedrock it must be a "
    "Bedrock model ID, inference profile ID or ARN, such as eu.anthropic.claude-opus-5-5 "
    "(docs/bedrock.md)"
)
#: A credential variable whose value, once the whitespace around it falls away, still holds a
#: character that no credential has (``{name}``: the variable; its value is never quoted).
CREDENTIAL_NOT_PRINTABLE = (
    "{name} holds whitespace, a control character or a non-ASCII character within its value: fix or unset it"
)
REDIRECTED = (
    "Amazon Bedrock answered {action} with a redirect (HTTP {status}), which the bedrock provider "
    "never follows: check llm.endpoint_url, ANTHROPIC_BEDROCK_BASE_URL and the proxy settings"
)
#: Stands for AWS's message in an error when that message talks about the credentials.
MESSAGE_LEFT_OUT = "AWS's message is not shown: it is about the credentials"
#: Stands for an AWS error type that is not shown: one not shaped like AWS's names of errors, or
#: one that holds a credential.
UNKNOWN_ERROR_TYPE = "an unknown error type"
FALLBACKS_IGNORED = (
    "llm.fallbacks is true, but Amazon Bedrock has no server-side refusal fallbacks: the bedrock "
    "provider sends none (set llm.fallbacks: false to silence this warning)"
)
JSON_ANSWER_INSTRUCTION = (
    "Answer with exactly one JSON object and nothing else: no Markdown, no code fence, no text before "
    "or after it. The object must validate against this JSON schema:\n{schema}"
)
JSON_RETRY_INSTRUCTION = (
    "Your answer was not a JSON object that validates against the schema ({problem}). Answer again "
    "with the JSON object only."
)
JSON_ANSWER_INVALID = "Claude's classification on Amazon Bedrock was not a JSON object of the expected schema"
NO_JSON_OBJECT = "no JSON object in the answer"

_DETAIL_CHARS = 300
#: What a 400 that rejects ``output_config.format`` says: ``output_config.format: ...``, Bedrock's
#: ``#/output_config: extraneous key [format] is not permitted`` (the word ``format``, never
#: ``reformat``), ``json_schema`` or structured outputs.
_STRUCTURED_OUTPUT_REJECTION = re.compile(r"\bformat\b|output_format|json_schema|structured output", re.I)
#: The SDK's own error when the AWS credential chain finds nothing.
_SDK_NO_CREDENTIALS = "could not resolve credentials from session"
#: Environment variables whose values never appear in a message of this module; each must be
#: printable ASCII without whitespace once the whitespace around it falls away
#: (:data:`CREDENTIAL_NOT_PRINTABLE`).
_SECRET_ENVS = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    API_KEY_ENV,
)
#: Where botocore's environment provider reads the session token: the first one that is set
#: (``AWS_SECURITY_TOKEN`` is its legacy name).
_SESSION_TOKEN_ENVS = ("AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN")
_HIDDEN = "[hidden]"
#: Access key IDs and the other AWS unique IDs of credentials.
_KEY_ID = re.compile(r"\b(?:AKIA|ASIA|ABIA|ACCA|AIDA|AROA)[A-Z0-9]{12,}\b")
#: AWS's answer to a signature that does not match can quote the canonical request, whose
#: ``x-amz-security-token`` line holds the session token of temporary credentials.
_SIGNING_DETAILS = re.compile(
    r"\s*The (?:Canonical String for this request|String-to-Sign) should have been\b.*", re.I | re.S
)
#: The names of credentials, and the generic names that hold one only in a field or a parameter.
_CREDENTIAL_NAMES = (
    r"x-amz-security-token|x-amz-credential|x-amz-signature|authorization|"
    r"(?:aws_)?(?:session|security)[ _-]?token|(?:aws_)?secret[ _-]?(?:access[ _-]?)?key|"
    r"access[ _-]?key[ _-]?id|api[ _-]?key"
)
_GENERIC_NAMES = r"token|signature|credential"
#: The value after the name of a credential: a header (``x-amz-security-token: v``,
#: ``Authorization: Bearer v``), a JSON or XML field (``"SessionToken": "v"``, ``"Token": "v"``,
#: ``<SecretAccessKey>v``), a setting of the shared AWS files (``aws_session_token = v``) or a
#: parameter (``X-Amz-Security-Token=v``, ``Credential=v``, ``Signature=v``). A generic name
#: followed by a colon in a sentence (``SSO Token: the token has expired``) is no field.
_NAMED_VALUE = re.compile(
    rf"((?:\b(?:{_CREDENTIAL_NAMES})['\"]?\s*[:=>]|['\"](?:{_GENERIC_NAMES})['\"]\s*:|"
    rf"\b(?:{_GENERIC_NAMES})\s*=|\b(?:{_GENERIC_NAMES})>)\s*['\"]?(?:bearer\s+)?)"
    r"([^\s'\",;<>&]+)",
    re.I,
)
#: A long string of base64 characters; hidden when it mixes lower- and upper-case letters and
#: digits, as keys and tokens do (model IDs, ARNs and request IDs are lower-case).
_RANDOM_RUN = re.compile(r"[A-Za-z0-9+/=]{16,}")
_UNICODE_ESCAPE = re.compile(r"\\u([0-9a-fA-F]{4})")
#: An AWS message that talks about these is never shown (the words, not the values, decide).
_CREDENTIAL_TALK = re.compile(
    r"\btoken\b|security[ _-]?token|session[ _-]?token|signature|authori[sz]ation|bearer|secret|"
    r"api[ _-]?key|password|x-amz-",
    re.I,
)
#: AWS's names of errors (``AccessDeniedException``, ``InternalFailure``): capitalised words of
#: letters, 3 to 64 characters in all (:func:`_error_type`). A run cut out of a key or a token
#: holds digits or mixes the capitals at random, so it is no such name.
_ERROR_TYPE = re.compile(r"(?=[A-Za-z]{3,64}\Z)(?:[A-Z][a-z]+)+")


# --------------------------------------------------------------------------- settings


def _importable(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def missing_aws_sdk() -> bool:
    """True when boto3 or botocore (the ``aws`` extra) is not installed; imports neither."""
    return not (_importable("boto3") and _importable("botocore"))


def is_aws_region(value: Any) -> bool:
    """True for an AWS Region code (:data:`dag_healer.config.AWS_REGION_CODE`)."""
    return isinstance(value, str) and AWS_REGION_CODE.match(value) is not None


def region_setting(llm: Any, environ: Mapping[str, str] | None = None) -> tuple[str | None, str | None]:
    """``(region, where it was found)``: ``llm.aws_region``, else ``AWS_REGION``, else
    ``AWS_DEFAULT_REGION`` (stripped; blank values are unset). ``(None, None)`` leaves the region
    to the AWS profile. The region is not checked here (:func:`settings_problems` does).
    """
    env = os.environ if environ is None else environ
    candidates = [("llm.aws_region", getattr(llm, "aws_region", None))]
    candidates += [(name, env.get(name)) for name in REGION_ENVS]
    for source, value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip(), source
    return None, None


def configured_region(llm: Any, environ: Mapping[str, str] | None = None) -> str | None:
    """The region of :func:`region_setting` (``None``: the AWS profile's, read by the SDK)."""
    return region_setting(llm, environ)[0]


def region_problem(region: str | None, source: str) -> str | None:
    """:data:`REGION_INVALID` for a region that is no AWS Region code (``None`` is no problem)."""
    if region is None or is_aws_region(region):
        return None
    return REGION_INVALID.format(source=source)


def model_alias_problem(model: str | None) -> str | None:
    """Why ``model`` cannot be sent to Bedrock: whitespace or a control character in it
    (:data:`MODEL_NOT_PRINTABLE`, which never quotes it), a Claude API model ID (``claude-...``)
    or nothing.

    ``None`` for anything else: Bedrock model IDs (``anthropic.claude-opus-5-5``), inference
    profile IDs (``eu.anthropic.claude-opus-5-5``, ``global.anthropic...``) and ARNs.
    """
    text = (model or "").strip()
    if any(char.isspace() or not char.isprintable() for char in text):
        return MODEL_NOT_PRINTABLE
    if text and not text.lower().startswith("claude-"):
        return None
    return (
        f"llm.model {text or '(empty)'} is a Claude API model ID; with llm.provider bedrock it must be a "
        "Bedrock model ID, inference profile ID or ARN, such as eu.anthropic.claude-opus-5-5 or "
        "global.anthropic.claude-opus-5-5 (docs/bedrock.md)"
    )


def _setting(llm: Any, name: str) -> str | None:
    value = getattr(llm, name, None)
    return (value.strip() or None) if isinstance(value, str) else None


def blank_region_variable(environ: Mapping[str, str] | None = None) -> str | None:
    """The first of ``AWS_REGION`` and ``AWS_DEFAULT_REGION`` set to spaces only (the SDK reads
    such a value as the region when nothing else sets one), or ``None``."""
    env = os.environ if environ is None else environ
    for name in REGION_ENVS:
        value = env.get(name)
        if value and not value.strip():
            return name
    return None


def _credential_problems(environ: Mapping[str, str]) -> list[str]:
    """:data:`CREDENTIAL_NOT_PRINTABLE` for each credential variable whose value, once the
    whitespace around it falls away, holds anything but printable ASCII without spaces (the
    characters of keys, tokens and Bedrock API keys, and of a valid HTTP header). The SDK would
    send it as it is: an invalid header, which the HTTP library refuses and quotes whole."""
    problems = []
    for name in _SECRET_ENVS:
        value = environ.get(name)
        if value is not None and not all("!" <= char <= "~" for char in value.strip()):
            problems.append(CREDENTIAL_NOT_PRINTABLE.format(name=name))
    return problems


def settings_problems(
    llm: Any, environ: Mapping[str, str] | None = None, *, model: str | None = None
) -> list[str]:
    """Every problem of the bedrock settings that the provider refuses before building a client.

    In this order: the model (:func:`model_alias_problem` of ``model``, else ``llm.model``), a
    profile next to a Bedrock API key (or next to an empty one, without a region outside the
    profile), a credential variable that holds whitespace, a control character or a non-ASCII
    character within its value (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
    ``AWS_SESSION_TOKEN``, ``AWS_SECURITY_TOKEN``, ``AWS_BEARER_TOKEN_BEDROCK``; the whitespace
    around a value falls away), the endpoint (``llm.endpoint_url``, else
    ``ANTHROPIC_BEDROCK_BASE_URL``: empty or not a valid endpoint URL), the region (no AWS Region
    code, or a blank variable the SDK would take as the region). The provider raises the first
    one; ``dag-healer doctor`` shows them all, then whether a region and credentials resolve. No
    value is quoted.
    """
    env = os.environ if environ is None else environ
    problems: list[str] = []
    alias = model_alias_problem(model if model is not None else getattr(llm, "model", None))
    if alias is not None:
        problems.append(alias)
    region, source = region_setting(llm, env)
    api_key = env.get(API_KEY_ENV)
    if _setting(llm, "aws_profile") is not None and api_key is not None:
        if api_key.strip():
            problems.append(PROFILE_AND_API_KEY)
        elif region is None:
            problems.append(EMPTY_API_KEY_WITH_PROFILE)
    problems += _credential_problems(env)
    endpoint = _setting(llm, "endpoint_url")
    base_url = env.get(BASE_URL_ENV)
    if endpoint is not None:
        if valid_endpoint_url(endpoint) is None:
            problems.append(ENDPOINT_URL_INVALID)
    elif base_url is not None and not base_url.strip():
        problems.append(BASE_URL_EMPTY)
    elif base_url is not None and valid_endpoint_url(base_url) is None:
        problems.append(BASE_URL_INVALID)
    if region is not None:
        problem = region_problem(region, source or PROFILE_REGION)
        if problem is not None:
            problems.append(problem)
    else:
        blank = blank_region_variable(env)
        if blank is not None:
            problems.append(REGION_BLANK.format(name=blank))
    return problems


def _base_url_variable(environ: Mapping[str, str] | None = None) -> str | None:
    """``ANTHROPIC_BEDROCK_BASE_URL`` as the provider hands it to the client (``None``: unset,
    empty or invalid; :func:`settings_problems` refuses the latter two first)."""
    value = (os.environ if environ is None else environ).get(BASE_URL_ENV)
    return valid_endpoint_url(value) if value is not None else None


def _api_key(environ: Mapping[str, str] | None = None) -> str | None:
    """``AWS_BEARER_TOKEN_BEDROCK`` without the whitespace around it (``None``: unset or blank).

    The SDK would send the value as it is, and a newline at its end makes an invalid header:
    the provider hands the stripped key to the client itself."""
    value = ((os.environ if environ is None else environ).get(API_KEY_ENV) or "").strip()
    return value or None


def _environment_keys(profile: str | None, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """The environment's AWS keys without the whitespace around them, as the client's own
    (``aws_access_key``, ``aws_secret_key``, ``aws_session_token``), when the default chain
    would sign with them as they are; ``{}`` otherwise.

    botocore reads ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and the session token
    (``AWS_SECURITY_TOKEN``, else ``AWS_SESSION_TOKEN``) first in its chain when no profile is
    configured (``profile``: ``llm.aws_profile``), and uses them as they are: a newline at the
    end of the token or of the key ID makes an invalid header, one at the end of the secret a
    wrong signature. Only a value with whitespace around it hands the keys to the client, so
    that every other case (an expiry in ``AWS_CREDENTIAL_EXPIRATION`` among them) stays the
    chain's.
    """
    env = os.environ if environ is None else environ
    access, secret = env.get("AWS_ACCESS_KEY_ID") or "", env.get("AWS_SECRET_ACCESS_KEY") or ""
    token = next((env[name] for name in _SESSION_TOKEN_ENVS if env.get(name)), "")
    if profile is not None or not access.strip() or not secret.strip():
        return {}
    if all(value == value.strip() for value in (access, secret, token)):
        return {}
    keys = {"aws_access_key": access.strip(), "aws_secret_key": secret.strip()}
    if token.strip():
        keys["aws_session_token"] = token.strip()
    return keys


def _proxy_mounts(endpoint: str | None) -> dict[str, None]:
    """The ``mounts`` of the provider's HTTP client: ``{"http://": None}`` (no proxy) for an
    ``http`` endpoint, which :func:`dag_healer.config.valid_endpoint_url` allows on a loopback
    host only, else none. A proxy of the environment (``HTTP_PROXY``, ``ALL_PROXY``) would
    receive the signed requests and the prompts of that endpoint in cleartext; through an
    ``https`` endpoint's proxy, TLS runs end to end."""
    return {"http://": None} if endpoint is not None and urlsplit(endpoint).scheme == "http" else {}


def fallbacks_written(llm: Any) -> bool:
    """True when the configuration file itself sets ``llm.fallbacks: true``."""
    written = getattr(llm, "model_fields_set", None) or ()
    return getattr(llm, "fallbacks", False) is True and "fallbacks" in written


# --------------------------------------------------------------------------- secrets


def _decoded(text: str) -> str:
    """``text`` with its JSON escapes (``\\uXXXX``, ``\\/``) and URL escapes (``%XX``) decoded,
    control characters made spaces, on one line."""

    def char(match: re.Match[str]) -> str:
        code = int(match.group(1), 16)
        return match.group(0) if 0xD800 <= code <= 0xDFFF else chr(code)  # never a lone surrogate

    text = unquote(_UNICODE_ESCAPE.sub(char, text).replace("\\/", "/"))
    return " ".join("".join(c if c.isprintable() else " " for c in text).split())


def _known_secrets(secrets: Iterable[str | None]) -> list[str]:
    """The credential values to look for: ``secrets`` and the environment's AWS secrets
    (:func:`dag_healer.masking.known_secrets`: 8 characters or more, longest first)."""
    return known_secrets([*secrets, *(os.environ.get(name) for name in _SECRET_ENVS)])


def _random_like(run: str) -> bool:
    return any(c.islower() for c in run) and any(c.isupper() for c in run) and any(c.isdigit() for c in run)


def hide_credentials(text: Any, secrets: Iterable[str | None] = (), *, mask: str = _HIDDEN) -> str:
    """``text``, decoded and on one line, without any AWS credential or Bedrock API key.

    ``text`` is decoded first (JSON and URL escapes), so an encoded credential is found too. Then
    AWS's quote of a signed request is cut off, and ``mask`` replaces: the value after a
    credential's name (``x-amz-security-token``, ``Authorization``, ``SessionToken``,
    ``SecretAccessKey``, ``aws_session_token``, ``Signature``, ``Credential``...); every value of
    ``secrets`` and of the environment's AWS credential variables (8 characters or more), and
    every fragment of 16 or more characters of them; access key IDs; and runs of 16 or more
    base64 characters that mix lower- and upper-case letters and digits. When a known value can
    still be read once the spaces are taken out, nothing of ``text`` is kept but ``mask``. Shared
    by the bedrock provider and ``dag-healer doctor``.
    """
    result = _SIGNING_DETAILS.sub(" [signing details hidden]", _decoded(str(text)))
    result = _NAMED_VALUE.sub(lambda match: match.group(1) + mask, result)
    known = _known_secrets(secrets)
    result = hide_secrets(result, known, mask)
    result = _KEY_ID.sub(mask, result)
    result = _RANDOM_RUN.sub(lambda match: mask if _random_like(match.group()) else match.group(), result)
    if holds_secret("".join(result.split()), known):
        return mask
    return result.strip()


def _masked(text: Any, secrets: Iterable[str | None] = ()) -> str:
    """``text`` without any credential (:func:`hide_credentials`), cut to about 300 characters
    and kept on one line: the cut's marker (``...[truncated]...``) joins the head and the tail."""
    return " ".join(excerpt(hide_credentials(text, secrets), _DETAIL_CHARS).split())


def _shown_name(value: Any, shape: re.Pattern[str], secrets: Iterable[str]) -> str | None:
    """``value`` when it is a string of ``shape`` (the whole of it) that holds no credential
    (:func:`hide_credentials` leaves it as it is), else ``None``."""
    if isinstance(value, str) and shape.fullmatch(value) and hide_credentials(value, secrets) == value:
        return value
    return None


def _session_credentials(client: Any) -> list[str]:
    """The access key, secret key and session token of the boto3 session the SDK signed with.

    The SDK caches that session (``anthropic.lib.bedrock._auth._get_session``, an
    ``lru_cache``) and the session its credentials. ``lru_cache`` keys keywords in the order of
    the call, so the lookup passes them in the order of the SDK's own call
    (``get_auth_headers``): in any other order it would find no session, make a new one and run
    the credential chain again (a credential process, another AssumeRole call and MFA prompt)
    for values that did not sign. A session whose credentials are not resolved yet did not sign
    either (an SDK whose call differs): it gives ``[]`` without resolving them. Refreshable
    credentials (a role's, a credential process's) give the values of their last refresh as
    botocore keeps them: read through their properties or ``get_frozen_credentials()``, they
    would refresh near their expiry, and the chain would run again; deferred ones that never
    refreshed did not sign and give ``[]``. Another SDK layout, or credentials that cannot be
    read, give ``[]`` as well: the patterns of :func:`hide_credentials` remain, the long
    random-looking strings that real tokens are among them.
    """
    try:
        from anthropic.lib.bedrock._auth import _get_session

        # The keywords in the order of get_auth_headers' call: the key of the session that signed.
        session = _get_session(
            profile=getattr(client, "aws_profile", None),
            region=getattr(client, "aws_region", None),
            aws_access_key=getattr(client, "aws_access_key", None),
            aws_secret_key=getattr(client, "aws_secret_key", None),
            aws_session_token=getattr(client, "aws_session_token", None),
        )
        # botocore's session keeps the credentials it resolved; get_credentials() would resolve them.
        credentials = getattr(getattr(session, "_session", None), "_credentials", None)
        if credentials is None:
            return []
        if hasattr(credentials, "refresh_needed"):  # refreshable: the values of the last refresh
            frozen = getattr(credentials, "_frozen_credentials", None)
        else:  # static: read as they are, nothing to refresh
            frozen = credentials.get_frozen_credentials()
    except Exception:  # never let the hiding itself fail an error message
        return []
    values = (getattr(frozen, name, None) for name in ("access_key", "secret_key", "token"))
    return [value for value in values if isinstance(value, str)]


def _credentials_in_use(client: Any, *, signed: bool) -> list[str]:
    """The credential values that ``client`` authenticates with: its Bedrock API key and static
    keys, and (``signed``: a request was signed, so the session's credentials are cached) those
    of the SDK's boto3 session."""
    if client is None:
        return []
    names = ("api_key", "aws_access_key", "aws_secret_key", "aws_session_token")
    values = [getattr(client, name, None) for name in names]
    if signed and getattr(client, "api_key", None) is None:
        values += _session_credentials(client)
    return [value for value in values if isinstance(value, str)]


def _is_aws_sdk_error(exc: BaseException) -> bool:
    """True for an exception of boto3 or botocore (by the module of its classes; nothing imported)."""
    return any(str(klass.__module__).split(".")[0] in ("boto3", "botocore") for klass in type(exc).__mro__)


# --------------------------------------------------------------------------- errors


class _StructuredOutputRejected(LLMUnavailable):
    """Bedrock answered 400 to ``output_config.format``: the classification asks for JSON instead."""


class _Spent:
    """The tokens of every answer billed for one classification."""

    def __init__(self) -> None:
        self.total = 0

    def add(self, message: Any) -> None:
        input_tokens, output_tokens = usage_tokens(getattr(message, "usage", None))
        self.total += input_tokens + output_tokens


def _aws_message(exc: Any) -> str:
    """AWS's ``{"message": ...}`` of an error answer; ``""`` when the body has none (the SDK's
    own text quotes the whole body, so it is never used)."""
    body = getattr(exc, "body", None)
    if not isinstance(body, Mapping):
        return ""
    text = body.get("message") or body.get("Message")
    error = body.get("error")
    if not text and isinstance(error, Mapping):
        text = error.get("message")
    return text if isinstance(text, str) else ""


def _error_type(exc: Any, secrets: Iterable[str] = ()) -> str:
    """The AWS error type of an error answer (``x-amzn-ErrorType``, else ``__type``) as an error
    may show it: the name alone (before ``:``, after ``#``) when it is shaped like AWS's names of
    errors (:data:`_ERROR_TYPE`) and holds no credential, :data:`UNKNOWN_ERROR_TYPE` for any other
    value, ``""`` without one."""
    headers = getattr(getattr(exc, "response", None), "headers", None)
    body = getattr(exc, "body", None)
    values = (
        headers.get("x-amzn-errortype") if isinstance(headers, Mapping) else None,
        body.get("__type") if isinstance(body, Mapping) else None,
    )
    value = next((item for item in values if isinstance(item, str) and item.strip()), None)
    if value is None:
        return ""
    name = value.split(":", 1)[0].rsplit("#", 1)[-1].strip()
    return _shown_name(name, _ERROR_TYPE, secrets) or UNKNOWN_ERROR_TYPE


def _detail(exc: Any, secrets: Iterable[str]) -> tuple[str, str]:
    """``(AWS's message, the detail an error may show)`` of a status error.

    The message decides the hints only. The detail is that message without any credential
    (:func:`hide_credentials`); when the message talks about the credentials, the error type
    (:func:`_error_type`) and :data:`MESSAGE_LEFT_OUT` instead; without a message, the error
    type alone.
    """
    known = list(secrets)
    message = _aws_message(exc)
    kind = _error_type(exc, known)
    if message and _CREDENTIAL_TALK.search(_decoded(message)):
        return message, f"{kind} ({MESSAGE_LEFT_OUT})" if kind else MESSAGE_LEFT_OUT
    if message:
        return message, _masked(message, known)
    return message, kind


def _hint(message: str, *, model: str, region: str) -> str:
    lowered = message.lower()
    if "inference profile" in lowered or "on-demand throughput" in lowered:
        return (
            " (use an inference profile ID in llm.model, such as eu.anthropic.claude-opus-5-5: "
            "docs/bedrock.md)"
        )
    if "anthropic_version" in lowered:
        return (
            " (the model does not accept the API version of the installed anthropic SDK: use Claude "
            "Opus 4.7 or later, docs/bedrock.md)"
        )
    if "model identifier" in lowered:
        return (
            f" (check that llm.model ({model}) is a Bedrock model or inference profile ID available in "
            f"{region}: docs/bedrock.md)"
        )
    return ""


def _status_error(
    exc: Any,
    action: str,
    *,
    model: str,
    region: str | None,
    structured_output: bool,
    secrets: Iterable[str],
) -> LLMUnavailable:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and 300 <= status < 400:
        return LLMUnavailable(REDIRECTED.format(action=action, status=status))
    message, detail = _detail(exc, secrets)
    where = region or "unset"
    if structured_output and status == 400 and _STRUCTURED_OUTPUT_REJECTION.search(message):
        return _StructuredOutputRejected(
            f"Amazon Bedrock rejected structured outputs during {action}: {detail}"
        )
    if status == 401:
        text = (
            f"Amazon Bedrock rejected the AWS credentials during {action} (401): check the worker's "
            "AWS credentials"
        )
        return LLMUnavailable(f"{text}; {detail}" if detail else text)
    shown = f": {detail}" if detail else ""
    if status == 403:
        return LLMUnavailable(
            f"Amazon Bedrock refused {action} (403){shown}; check the worker's AWS credentials, its "
            f"permission bedrock:InvokeModel on {model} and the model access (docs/bedrock.md)"
        )
    if status == 404:
        text = (
            f"Amazon Bedrock returned 404 during {action}: check llm.model ({model}) and the region ({where})"
        )
        return LLMUnavailable(f"{text}; {detail}" if detail else text)
    if status == 429:
        return LLMUnavailable(
            f"Amazon Bedrock throttled {action} (429); retry later or raise the quota of {model} in {where}"
        )
    return LLMUnavailable(
        f"Amazon Bedrock error {status} during {action}{shown}{_hint(message, model=model, region=where)}"
    )


def _translated(exc: Exception, action: str, secrets: Iterable[str]) -> LLMUnavailable | None:
    """The :class:`LLMUnavailable` for an error raised before the request is sent, or ``None``."""
    name = type(exc).__name__
    if _is_aws_sdk_error(exc):
        if name == "NoCredentialsError":
            return LLMUnavailable(NO_CREDENTIALS)
        return LLMUnavailable(
            f"the AWS credentials for the bedrock provider cannot be resolved during {action}: "
            f"{name}: {_masked(exc, secrets)}"
        )
    if isinstance(exc, ModuleNotFoundError) and (exc.name or "").split(".")[0] in ("boto3", "botocore"):
        return LLMUnavailable(AWS_SDK_MISSING)
    if isinstance(exc, RuntimeError) and str(exc).strip().lower() == _SDK_NO_CREDENTIALS:
        return LLMUnavailable(NO_CREDENTIALS)  # the SDK's own error: the AWS chain found nothing
    if isinstance(exc, RuntimeError) and "credentials" in str(exc).lower():
        # botocore's, such as refreshed credentials that are still expired (a past
        # AWS_CREDENTIAL_EXPIRATION, a credential process that returns expired ones).
        return LLMUnavailable(
            f"the AWS credentials for the bedrock provider cannot be resolved during {action}: "
            f"{name}: {_masked(exc, secrets)}"
        )
    if isinstance(exc, ValueError) and "Streaming is required" in str(exc):
        return LLMUnavailable(_MAX_TOKENS_TOO_LARGE.format(action=action))
    return None


@contextmanager
def bedrock_errors(
    action: str, *, model: str, region: str | None, structured_output: bool = False, client: Any = None
) -> Iterator[None]:
    """Translate what a Bedrock request can raise into :class:`LLMUnavailable` that says what to fix.

    Status errors name the setting or permission to check (``structured_output``: a 400 about the
    structured output becomes the private ``_StructuredOutputRejected``; a redirect is
    :data:`REDIRECTED`); connection errors name the host; credential problems, which the SDK
    raises before sending anything (not retried), become :data:`NO_CREDENTIALS` or name the
    error. Other errors propagate. No message holds a credential (:func:`hide_credentials`, with
    the credentials ``client`` authenticates with).
    """
    import anthropic

    try:
        yield
    except (LLMUnavailable, LLMRefusal):
        raise
    except anthropic.APIStatusError as exc:
        secrets = _credentials_in_use(client, signed=True)
        raise _status_error(
            exc, action, model=model, region=region, structured_output=structured_output, secrets=secrets
        ) from exc
    except anthropic.APIConnectionError as exc:
        url = getattr(getattr(exc, "request", None), "url", None)
        host = getattr(url, "host", None) or "unknown host"
        raise LLMUnavailable(
            f"Cannot reach Amazon Bedrock ({host}) during {action}: {type(exc).__name__}"
        ) from exc
    except Exception as exc:
        translated = _translated(exc, action, _credentials_in_use(client, signed=False))
        if translated is None:
            raise
        raise translated from exc


def _not_built(exc: BaseException) -> LLMUnavailable:
    return LLMUnavailable(f"the Amazon Bedrock client cannot be built: {type(exc).__name__}: {_masked(exc)}")


# --------------------------------------------------------------------------- answers


def _json_object_text(text: str) -> str:
    """The text from the first ``{`` to the last ``}`` (a code fence around it falls away)."""
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ValueError(NO_JSON_OBJECT)
    return text[start : end + 1]


def _validated(message: Any) -> Classification:
    """The :class:`Classification` in the text of ``message``; :class:`ValueError` otherwise."""
    return Classification.model_validate_json(_json_object_text(_message_text(message)))


def _problem(exc: ValueError) -> str:
    """What was wrong with an answer, for the retry (never the answer itself)."""
    if not isinstance(exc, pydantic.ValidationError):
        return str(exc) or NO_JSON_OBJECT
    parts = []
    for error in exc.errors()[:5]:  # where and why only: the "input" of each error is never used
        where = ".".join(str(part) for part in error.get("loc") or ())
        parts.append(f"{where}: {error.get('msg')}" if where else str(error.get("msg")))
    return "; ".join(parts) or "the answer did not validate"


def _retry_turns(message: Any, problem: str) -> list[dict[str, Any]]:
    """The messages appended for the one retry: the answer as it came (thinking blocks included),
    then the instruction. Nothing earlier changes, so the conversation stays append-only."""
    turns: list[dict[str, Any]] = []
    content = getattr(message, "content", None)
    if content:
        turns.append({"role": "assistant", "content": content})
    turns.append({"role": "user", "content": JSON_RETRY_INSTRUCTION.format(problem=problem)})
    return turns


def _schema_text() -> str:
    return json.dumps(Classification.model_json_schema(), sort_keys=True)


# --------------------------------------------------------------------------- provider


class BedrockProvider(AnthropicProvider):
    """Claude on Amazon Bedrock through ``anthropic.AnthropicBedrock`` (see the module docstring).

    Construction does no I/O and imports nothing from AWS; the client is built on the first call.
    Every problem the operator must fix (:func:`settings_problems`, no region, no credentials, a
    region of the profile that is no AWS Region code, the ``aws`` extra missing) raises
    :class:`LLMUnavailable` from that call. ``client`` injects a pre-built
    ``anthropic.AnthropicBedrock``; ``http_client`` is handed to the client the provider builds
    (tests pass one on an ``httpx2.MockTransport``). Without it, the provider builds the SDK's
    default HTTP client with ``follow_redirects=False``, and without the environment's proxies
    for an ``http`` endpoint (:func:`_proxy_mounts`).
    """

    name = "bedrock"

    def __init__(
        self,
        cfg: Any = None,
        *,
        client: Any = None,
        model: str | None = None,
        effort: str | None = None,
        max_tokens: int | None = None,
        http_client: Any = None,
    ) -> None:
        super().__init__(
            cfg, client=client, model=model, effort=effort, max_tokens=max_tokens, fallbacks=False
        )
        llm = getattr(cfg, "llm", None)
        self._llm = llm
        self.model = str(self.model).strip()
        self.region: str | None = configured_region(llm)
        self.profile: str | None = _setting(llm, "aws_profile")
        self.endpoint_url: str | None = _setting(llm, "endpoint_url")
        self.structured_outputs = True
        self._http_client = http_client
        if fallbacks_written(llm):
            logger.warning(FALLBACKS_IGNORED)
        problem = model_alias_problem(self.model)
        if problem is not None:
            logger.warning("%s", problem)

    # ------------------------------------------------------------------ plumbing
    def _get_client(self) -> Any:
        problem = model_alias_problem(self.model)
        if problem is not None:
            raise LLMUnavailable(problem)
        quiet_botocore_payloads()  # before anything is signed, with an injected client too
        if self._client is not None:
            return self._client
        if missing_aws_sdk():
            raise LLMUnavailable(AWS_SDK_MISSING)
        problems = settings_problems(self._llm, model=self.model)
        if problems:
            raise LLMUnavailable(problems[0])
        self.region = configured_region(self._llm)
        api_key = _api_key()
        empty_api_key = api_key is None and os.environ.get(API_KEY_ENV) is not None
        keys = {} if api_key is not None else _environment_keys(self.profile)
        endpoint = self.endpoint_url or _base_url_variable()
        import anthropic

        http_client = self._http_client
        if http_client is None:
            # The SDK's defaults (timeouts, limits, proxies), but no redirect, and no proxy for an
            # http endpoint.
            http_client = anthropic.DefaultHttpxClient(follow_redirects=False, mounts=_proxy_mounts(endpoint))
        try:
            client = anthropic.AnthropicBedrock(
                aws_region=self.region,
                aws_profile=None if empty_api_key else self.profile,
                api_key=api_key,
                base_url=endpoint,
                http_client=http_client,
                **({} if empty_api_key else keys),
            )
        except Exception as exc:
            if self._http_client is None:
                http_client.close()
            # No region anywhere: the SDK's ValueError; an unknown profile: botocore's ProfileNotFound.
            if _is_aws_sdk_error(exc):
                raise _not_built(exc) from exc
            if not isinstance(exc, ValueError):
                raise
            if "region" in str(exc).lower():
                raise LLMUnavailable(NO_REGION) from exc
            raise _not_built(exc) from exc
        if not is_aws_region(client.aws_region):  # the profile's region, which the SDK read
            if self._http_client is None:
                client.close()
            raise LLMUnavailable(REGION_INVALID.format(source=PROFILE_REGION))
        if empty_api_key:
            # An empty AWS_BEARER_TOKEN_BEDROCK counts as unset: sign with SigV4 instead.
            client.api_key = None
            client.aws_profile = self.profile
            for name, value in keys.items():
                setattr(client, name, value)
        self._client = client
        return client

    def _errors(self, action: str, *, structured_output: bool = False) -> AbstractContextManager[None]:
        region = getattr(self._client, "aws_region", None) or self.region
        return bedrock_errors(
            action, model=self.model, region=region, structured_output=structured_output, client=self._client
        )

    def _request_kwargs(self, system: str, prompt: str) -> dict[str, Any]:
        kwargs = super()._request_kwargs(system, prompt)
        kwargs.pop("betas", None)  # never a beta or the fallbacks on Bedrock (fallbacks is False)
        kwargs.pop("fallbacks", None)
        return kwargs

    def _use_json_answers(self, reason: str) -> None:
        self.structured_outputs = False
        logger.warning("%s; asking %s for a JSON answer from now on", reason, self.model)

    def _without_credentials(self, text: str) -> str:
        """``text`` without any credential (:func:`hide_credentials`, with the credentials that the
        client signs with): for a refusal's category, checked as the Claude API provider checks
        it, and its explanation."""
        return hide_credentials(text, _credentials_in_use(self._client, signed=True))

    def _shown_stop_reason(self, stop_reason: Any) -> Any:
        """A stop reason of Claude's, else ``other`` (:func:`dag_healer.agent.llm._known_stop_reason`):
        the SDK builds the answer without checking its fields, so the endpoint could fill this one
        with anything, an echo of a credential included."""
        return _known_stop_reason(stop_reason)

    def _log_fallbacks(self, message: Any) -> None:
        """Nothing: Amazon Bedrock has no server-side fallbacks, so a ``fallback`` block or a
        ``fallback_message`` iteration in an answer can only come from a misbehaving endpoint,
        and the model names it gives are not logged."""

    # ------------------------------------------------------------------ classify
    def classify(self, *, system: str, prompt: str) -> tuple[Classification, int]:
        """One classification; returns it and the tokens of every request made for it.

        With structured outputs (the default) the request carries ``output_config.format``; the
        stop reason is checked before the answer is read (a refusal raises :class:`LLMRefusal`,
        a cut answer :class:`LLMUnavailable`, never a JSON retry). A 400 about the structured
        output, or an answer that does not validate, switches the provider to JSON answers
        (:meth:`_classify_json`) for the rest of its life. An error raised after billed answers
        carries their tokens in ``tokens_used``.
        """
        client = self._get_client()
        action = "the incident classification"
        spent = _Spent()
        try:
            parsed = self._classify(client, system, prompt, action, spent)
        except (LLMUnavailable, LLMRefusal) as exc:
            exc.tokens_used = spent.total
            raise
        return self._classified(parsed, spent.total)

    def _classify(self, client: Any, system: str, prompt: str, action: str, spent: _Spent) -> Classification:
        import anthropic

        if self.structured_outputs:
            kwargs = self._request_kwargs(system, prompt)
            schema = anthropic.transform_schema(Classification.model_json_schema())
            kwargs["output_config"] = {
                **kwargs["output_config"],
                "format": {"type": "json_schema", "schema": schema},
            }
            try:
                with self._errors(action, structured_output=True):
                    message = client.messages.create(**kwargs)
            except _StructuredOutputRejected as exc:
                self._use_json_answers(str(exc))
            else:
                spent.add(message)
                self._raise_for_stop(
                    getattr(message, "stop_reason", None), getattr(message, "stop_details", None), action
                )
                try:
                    return _validated(message)
                except ValueError:
                    self._use_json_answers(
                        "Claude's structured answer on Amazon Bedrock did not match the schema"
                    )
        return self._classify_json(client, system, prompt, action, spent)

    def _classify_json(
        self, client: Any, system: str, prompt: str, action: str, spent: _Spent
    ) -> Classification:
        """Ask for the JSON object in the answer; one append-only retry when it does not validate."""
        kwargs = self._request_kwargs(system, prompt)
        instruction = JSON_ANSWER_INSTRUCTION.format(schema=_schema_text())
        kwargs["system"] = [*kwargs["system"], {"type": "text", "text": instruction}]
        message = self._answer(client, kwargs, action, spent)
        try:
            return _validated(message)
        except ValueError as exc:
            problem = _problem(exc)
        kwargs["messages"] = [*kwargs["messages"], *_retry_turns(message, problem)]
        retry = self._answer(client, kwargs, action, spent)
        try:
            return _validated(retry)
        except ValueError as exc:
            raise LLMUnavailable(JSON_ANSWER_INVALID) from exc

    def _answer(self, client: Any, kwargs: dict[str, Any], action: str, spent: _Spent) -> Any:
        with self._errors(action):
            message = client.messages.create(**kwargs)
        spent.add(message)
        self._raise_for_stop(
            getattr(message, "stop_reason", None), getattr(message, "stop_details", None), action
        )
        return message

    @staticmethod
    def _classified(parsed: Classification, tokens: int) -> tuple[Classification, int]:
        logger.debug(
            "Claude classification on Amazon Bedrock: %s (confidence %.2f, %d tokens)",
            parsed.category,
            parsed.confidence,
            tokens,
        )
        return parsed, tokens

    # repair_session: AnthropicProvider's (client.beta.messages.tool_runner without threads, betas
    # or fallbacks; budget per message, tool records, refusal stop, partial results), with the
    # Bedrock errors of _errors() and the log lines of _shown_stop_reason() and _log_fallbacks().


__all__ = [
    "API_KEY_ENV",
    "AWS_EXTRA_INSTALL",
    "AWS_SDK_MISSING",
    "BASE_URL_EMPTY",
    "BASE_URL_ENV",
    "BASE_URL_INVALID",
    "CREDENTIAL_NOT_PRINTABLE",
    "EMPTY_API_KEY_WITH_PROFILE",
    "ENDPOINT_URL_INVALID",
    "FALLBACKS_IGNORED",
    "JSON_ANSWER_INSTRUCTION",
    "JSON_ANSWER_INVALID",
    "JSON_RETRY_INSTRUCTION",
    "MESSAGE_LEFT_OUT",
    "MODEL_NOT_PRINTABLE",
    "NO_CREDENTIALS",
    "NO_REGION",
    "PROFILE_AND_API_KEY",
    "PROFILE_REGION",
    "REDIRECTED",
    "REGION_BLANK",
    "REGION_ENVS",
    "REGION_INVALID",
    "UNKNOWN_ERROR_TYPE",
    "BedrockProvider",
    "bedrock_errors",
    "blank_region_variable",
    "configured_region",
    "fallbacks_written",
    "hide_credentials",
    "is_aws_region",
    "missing_aws_sdk",
    "model_alias_problem",
    "region_problem",
    "region_setting",
    "settings_problems",
]
