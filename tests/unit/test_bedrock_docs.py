"""The Amazon Bedrock docs (``docs/bedrock.md`` and the pages that point at it) against the code.

* the page's relative links resolve, and the README, ``docs/security.md``, ``docker/README.md``
  and the Step Functions example link the page;
* the IAM and endpoint policies are valid JSON and allow nothing but ``bedrock:InvokeModel``;
* the ``healer.yaml`` example validates, its commented lines included;
* the provider's messages are quoted verbatim, and the doctor lines are what ``check_llm``
  prints (fictional credentials, the instance metadata service off: nothing touches the network);
* what the page and ``docs/security.md`` say about redirects, proxies, credentials in messages,
  the session token variables, the tokens counted, the debug records of botocore and of the
  anthropic SDK, the reason phrase and the stop reason in the logs and the test sandbox matches
  the code's rules;
* ``docker/.env.example`` keeps every AWS line commented.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from dag_healer.agent import bedrock, llm
from dag_healer.cli import doctor_cmd
from dag_healer.cli import main as cli_main
from dag_healer.config import LlmConfig, config_from_mapping
from dag_healer.repo.pytest_runner import CLOUD_SANDBOX_ENV, CLOUD_SCRUB_EXACT
from dag_healer.storage.s3 import PAYLOAD_LOG_RECORDS

REPO_ROOT = Path(__file__).resolve().parents[2]
BEDROCK_DOC = REPO_ROOT / "docs" / "bedrock.md"
SECURITY_DOC = REPO_ROOT / "docs" / "security.md"
ENV_EXAMPLE = REPO_ROOT / "docker" / ".env.example"
MODEL = "eu.anthropic.claude-opus-5-5"
REGION = "eu-west-1"
ACCESS_KEY = "AKIDEXAMPLE"
SECRET_KEY = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
API_KEY = "bedrock-api-key-EXAMPLE"
_LINK = re.compile(r"\]\(([^)\s]+)\)")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def _blocks(text: str, lang: str) -> list[str]:
    """Fenced blocks of ``lang``, also inside list items (indented fences)."""
    return [
        "\n".join(line[len(indent) :] for line in body.splitlines())
        for indent, body in re.findall(rf"^( *)```{lang}\n(.*?)^ *```$", text, flags=re.MULTILINE | re.DOTALL)
    ]


def test_the_relative_links_of_the_page_resolve() -> None:
    broken = [
        target
        for target in _LINK.findall(_read(BEDROCK_DOC))
        if not target.startswith(("http://", "https://", "mailto:", "#"))
        and not (BEDROCK_DOC.parent / target.split("#", 1)[0]).exists()
    ]
    assert not broken


@pytest.mark.parametrize(
    "relative",
    ["README.md", "docs/security.md", "docker/README.md", "examples/stepfunctions_repo/README.md"],
    ids=["readme", "security", "docker", "sfn-example"],
)
def test_the_docs_link_the_bedrock_page(relative: str) -> None:
    path = REPO_ROOT / relative
    targets = [(path.parent / target.split("#", 1)[0]).resolve() for target in _LINK.findall(_read(path))]
    assert BEDROCK_DOC.resolve() in targets


def test_the_policies_allow_only_invoke_model() -> None:
    policies = [json.loads(block) for block in _blocks(_read(BEDROCK_DOC), "json")]
    assert len(policies) == 3
    statements = [s for p in policies for s in (p["Statement"] if "Statement" in p else [p])]
    allowed = {s["Action"] for s in statements if s["Effect"] == "Allow"}
    assert allowed == {"bedrock:InvokeModel"}
    denied = [s for s in statements if s["Effect"] == "Deny"]
    assert [s["Condition"] for s in denied] == [{"StringEquals": {"aws:RequestedRegion": "unspecified"}}]
    profile = f"arn:aws:bedrock:{REGION}:123456789012:inference-profile/{MODEL}"
    assert any(s.get("Resource") == profile for s in statements)
    assert any(
        s.get("Condition", {}).get("StringEquals", {}).get("bedrock:InferenceProfileArn") == profile
        for s in statements
    )


def test_the_healer_yaml_example_validates() -> None:
    (example,) = _blocks(_read(BEDROCK_DOC), "yaml")
    llm = LlmConfig.model_validate(yaml.safe_load(example)["llm"])
    assert (llm.provider, llm.model, llm.aws_region, llm.fallbacks) == ("bedrock", MODEL, REGION, False)
    assert bedrock.model_alias_problem(llm.model) is None
    uncommented = re.sub(r"^  # (aws_profile|endpoint_url):", r"  \1:", example, flags=re.MULTILINE)
    full = LlmConfig.model_validate(yaml.safe_load(uncommented)["llm"])
    assert full.aws_profile and full.endpoint_url and full.endpoint_url.startswith("https://vpce-")


def test_the_messages_are_quoted_verbatim() -> None:
    text = " ".join(_read(BEDROCK_DOC).split())
    for name in (
        "AWS_SDK_MISSING",
        "NO_REGION",
        "NO_CREDENTIALS",
        "PROFILE_AND_API_KEY",
        "BASE_URL_EMPTY",
        "FALLBACKS_IGNORED",
        "JSON_ANSWER_INVALID",
        "BASE_URL_INVALID",
        "MESSAGE_LEFT_OUT",
        "UNKNOWN_ERROR_TYPE",
    ):
        assert getattr(bedrock, name) in text, name
    assert bedrock.model_alias_problem("claude-opus-5-5") in text
    assert bedrock.REGION_INVALID.format(source="AWS_REGION") in text
    assert bedrock.REGION_BLANK.format(name="AWS_REGION") in text
    assert bedrock.REDIRECTED.format(action="<action>", status=307) in text
    assert bedrock.MODEL_NOT_PRINTABLE.split(";")[0] in text
    assert bedrock.CREDENTIAL_NOT_PRINTABLE.format(name="AWS_SESSION_TOKEN") in text


def _flat(path: Path) -> str:
    return " ".join(_read(path).split())


def test_the_pages_state_the_rules_of_the_code() -> None:
    """Redirects, credentials in messages, the session token variables, the tokens counted,
    botocore's debug records and the sandbox scrub."""
    page, security = _flat(BEDROCK_DOC), _flat(SECURITY_DOC)
    # no redirect is followed (tests/unit/test_bedrock.py and test_llm.py prove it on the wire)
    assert "The provider never follows a redirect." in page
    assert "never follow one: a 3xx answer ends the call after that one request" in security
    # the records botocore logs a request or an answer with, all dropped
    dropped = {prefix for prefixes in PAYLOAD_LOG_RECORDS.values() for prefix in prefixes}
    for record in ("Making request for", "Sending http request", "Response headers", "Response body"):
        assert record in dropped and f"`{record}`" in security, record
    assert "on every botocore logger" in security and "As a backstop" in security
    # credentials in messages: the forms they are hidden in, messages about them left out
    for phrase in ("URL-encoded", "JSON-escaped", "a fragment of 16 or more characters", "never quoted"):
        assert phrase in security, phrase
    assert "URL-encoded or JSON-escaped, and so is any run of 16 or more of their characters" in page
    assert "in any form" not in page and "in any form" not in security
    talk = (
        "talks about a token, a signature, a secret, an API key, a password, a bearer token, "
        "authorization or an `x-amz-` header"
    )
    assert talk in page and talk in security
    words = ("a token", "a signature", "a secret", "an API key", "a password", "a bearer", "authorization")
    for word in (*words, "Authorization", "x-amz-date"):
        assert bedrock._CREDENTIAL_TALK.search(word), word
    # the error type and the refusal category: shown only when shaped like AWS's and Claude's
    for name, found in (("bedrock.md", page), ("security.md", security)):
        assert "capitalised words of letters, 3 to 64 in all" in found, name
        assert "lower-case letters and `_`" in found, name
    assert "or that note alone when the answer has no error type" in page
    assert "follows the same rule, with both providers" in page and "one check for both providers" in security
    # the session token: botocore reads AWS_SECURITY_TOKEN first, and so does the provider
    assert bedrock._SESSION_TOKEN_ENVS == ("AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN")
    for found in (page, security):
        assert "`AWS_SECURITY_TOKEN`, its legacy name, which botocore reads first" in found
    # the tokens of the classification count in the incident, the limit applies to a repair run
    assert "count in the incident's `tokens_used`" in page
    assert "applies to each repair run, not to the classification" in page
    assert "the classification is not counted against them" in security
    # no proxy for a loopback http endpoint; no header of an answer in the debug logs
    assert "never goes through a proxy" in page and "never goes through a proxy" in security
    assert "`[headers not shown]` stands in for them" in page and "`[headers not shown]`" in security
    assert "quotes a header value that h11 refused" in security
    # the reason phrase of an answer: the standard one of its status code (the CLI's log filter)
    assert cli_main._standard_reason(403) == "Forbidden"
    assert "the standard one of the status code stands in for it (`HTTP/1.1 403 Forbidden`)" in page
    assert "the reason phrase of the status line only as the standard one of the status code" in security
    # the stop reason that the bedrock provider logs at the end of a repair: one of Claude's, else other
    assert llm._known_stop_reason("tool_use") == "tool_use" and llm._known_stop_reason("x") == "other"
    assert "`other` standing in for any other value" in page and "(`other` stands in for" in security
    # the sandbox: the scrubbed names, the emptied files, and what process still exposes
    for name in (
        "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_CREDENTIAL_FILE",
        "EC2_ACCESS_KEY",
        "EC2_SECRET_KEY",
    ):
        assert name in CLOUD_SCRUB_EXACT and f"`{name}`" in security and f"`{name}`" in page, name
    assert CLOUD_SANDBOX_ENV["BOTO_CONFIG"] == "" and "`BOTO_CONFIG` are set to empty values" in security
    assert "`BOTO_CONFIG` are set to empty values" in page
    for phrase in ("`/proc/<parent pid>/environ`", "`ps -E -p <parent pid>`", "`~/.aws` and `~/.boto`"):
        assert phrase in page and phrase in security, phrase
    assert "the JavaScript v3 and Go v2 SDKs" in page and "JavaScript v3 and Go v2 SDKs" in security
    assert "Under the container backend** (the default), the test sandbox never sees the AWS" in page


def _line(llm: dict[str, object], monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> str:
    with monkeypatch.context() as patch:
        for name, value in env.items():
            patch.setenv(name, value)
        result = doctor_cmd.check_llm(config_from_mapping({"llm": llm}))
    return f"{doctor_cmd._LABELS[result.status]} {result.name}  {result.detail}"


@pytest.mark.usefixtures("isolated_aws")
def test_the_doctor_lines_are_the_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``iam-role`` line stands for the ``env`` one: no test reaches an instance role."""
    pytest.importorskip("botocore")
    (block,) = [b for b in _blocks(_read(BEDROCK_DOC), "text") if b.startswith("[ OK ] llm")]
    good = {"provider": "bedrock", "model": MODEL, "fallbacks": False}
    keys = {"AWS_ACCESS_KEY_ID": ACCESS_KEY, "AWS_SECRET_ACCESS_KEY": SECRET_KEY, "AWS_REGION": REGION}
    with_keys = _line(good, monkeypatch, keys)
    with_api_key = _line(good, monkeypatch, {"AWS_BEARER_TOKEN_BEDROCK": API_KEY, "AWS_REGION": REGION})
    expected = [
        with_keys.replace("(env; values hidden)", "(iam-role; values hidden)"),
        with_api_key,
        _line({**good, "model": "claude-opus-5-5"}, monkeypatch, keys),
        _line(good, monkeypatch, {"AWS_REGION": REGION}),
    ]
    assert block.strip().splitlines() == expected
    for line in (with_keys, with_api_key):
        assert ACCESS_KEY not in line and SECRET_KEY not in line and API_KEY not in line


def test_the_env_example_keeps_the_aws_lines_commented() -> None:
    text = _read(ENV_EXAMPLE)
    active = set(re.findall(r"^([A-Z_][A-Z0-9_]*)=", text, flags=re.MULTILINE))
    assert not {name for name in active if name.startswith(("AWS_", "ANTHROPIC_BEDROCK"))}
    assert "HEALER_LLM_MODEL" not in active
    assert re.search(r"^# HEALER_LLM_MODEL=\S+$", text, flags=re.MULTILINE)
    assert "docs/bedrock.md" in text
