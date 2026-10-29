"""Unit tests for dag_healer.masking.find_secret: what looks like a credential in a text.

A context file (dag_healer.context) that holds one is refused: nothing of it reaches the model or
the store. Every fake credential below is put together from pieces at run time, so this file
holds no string that looks like a real one.
"""

from __future__ import annotations

import pytest

from dag_healer.masking import KNOWN_SECRET_KIND, SecretFinding, find_secret


def _lines(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def _in_a_file(fragment: str) -> str:
    """A small context file with ``fragment`` in a sentence on line 3."""
    return _lines("# Systems and owners", "", f"The export uses {fragment} for now.", "", "- ORG-1: A rule.")


def _key_header(algorithm: str) -> str:
    return "-----BEGIN " + algorithm + "PRIV" + "ATE KEY-----"


def _jwt() -> str:
    return ".".join(["eyJ" + "a1" * 6, "eyJ" + "b2" * 6, "c3" * 8])


PATTERNS = [
    pytest.param(_key_header("RSA "), "a private key", id="rsa-private-key"),
    pytest.param(_key_header(""), "a private key", id="private-key"),
    pytest.param(_key_header("OPENSSH "), "a private key", id="openssh-private-key"),
    pytest.param(_key_header("ENCRYPTED "), "a private key", id="encrypted-private-key"),
    pytest.param("-----BEGIN PGP " + "PRIV" + "ATE KEY BLOCK-----", "a private key", id="pgp-private-key"),
    pytest.param("AKIA" + "X" * 16, "an AWS access key ID", id="aws-access-key-id"),
    pytest.param("ASIA" + "Q7" * 8, "an AWS access key ID", id="aws-temporary-access-key-id"),
    pytest.param("aws_secret_access_key = " + "a1B2/c3D4+" * 4, "an AWS secret access key", id="aws-secret"),
    pytest.param(
        'AWS_SECRET_ACCESS_KEY: "' + "Zz9" * 13 + 'x"', "an AWS secret access key", id="aws-secret-yaml"
    ),
    pytest.param("gh" + "p_" + "a1" * 18, "a GitHub token", id="github-classic"),
    pytest.param("gh" + "s_" + "B2" * 18, "a GitHub token", id="github-app"),
    pytest.param(
        "github" + "_pat_" + "c3" * 11 + "_" + "d4" * 20, "a GitHub token", id="github-fine-grained"
    ),
    # an Anthropic key is also an sk- key: the more specific kind wins
    pytest.param("sk-" + "ant-" + "api03-" + "e5" * 20, "an Anthropic API key", id="anthropic"),
    pytest.param("sk-" + "f6" * 16, "an API key", id="sk-key"),
    pytest.param("sk-" + "proj-" + "g7" * 16, "an API key", id="sk-project-key"),
    pytest.param("sk" + "_live_" + "h8" * 10, "a Stripe key", id="stripe-secret-key"),
    pytest.param("rk" + "_live_" + "i9" * 10, "a Stripe key", id="stripe-restricted-key"),
    pytest.param("AIza" + "j0" * 17 + "k", "a Google API key", id="google-api-key"),
    pytest.param("xox" + "b-" + "1234567890" + "-" + "l1" * 6, "a Slack token", id="slack-token"),
    pytest.param(
        "https://hooks" + ".slack.com/services/" + "T0" * 4 + "/" + "B1" * 4 + "/" + "m2" * 12,
        "a Slack webhook URL",
        id="slack-webhook",
    ),
    pytest.param(_jwt(), "a JSON web token", id="json-web-token"),
    pytest.param(
        "postgresql://etl:" + "n3" * 5 + "@db.internal:5432/lab", "a password in a URL", id="url-password"
    ),
    pytest.param("password: " + "hunter" + "22a", "a credential with its value", id="password"),
    pytest.param('api_key = "' + "abc123" + 'def456"', "a credential with its value", id="api-key"),
    pytest.param("token=" + "abcd1234" + "efgh", "a credential with its value", id="token"),
    pytest.param(
        '{"client_secret": "' + "cs" + "9" * 10 + '"}', "a credential with its value", id="json-secret"
    ),
    pytest.param("passphrase: " + "correct7" + "horse", "a credential with its value", id="passphrase"),
    pytest.param("auth-token: " + "t0k3n" + "Value", "a credential with its value", id="auth-token"),
    pytest.param("Access_Token = " + "AT" + "12345678", "a credential with its value", id="access-token"),
    pytest.param("pwd: " + "Passw0rd" + "!", "a credential with its value", id="pwd"),
    pytest.param(
        "sftp-password: " + "Lab2026" + "Upload", "a credential with its value", id="hyphenated-name"
    ),
    # review fixes: names glued to a prefix, long values without digits, more token shapes
    pytest.param("PGPASSWORD=" + "s3cr3t" + "Passw0rd", "a credential with its value", id="pgpassword"),
    pytest.param("DBPASSWORD=" + "Pr0d" + "Pass2026", "a credential with its value", id="dbpassword"),
    pytest.param("APPSECRET: " + "Zq7Lm2" + "Pw9Xc4", "a credential with its value", id="glued-secret"),
    pytest.param(
        "password: " + "Correct" + "HorseBatteryStaple", "a credential with its value", id="long-passphrase"
    ),
    pytest.param(
        'curl -H "Authorization: Bearer ' + "8f14e45f" * 4 + '"', "an Authorization header", id="bearer"
    ),
    pytest.param("Authorization: Basic " + "ZXRsOlMz" + "Y3JldFB3", "an Authorization header", id="basic"),
    pytest.param("glpat-" + "xYz12AbCd" + "EfGhIjKlMnO", "a GitLab token", id="gitlab-token"),
    pytest.param(
        "DefaultEndpointsProtocol=https;AccountName=acme;AccountKey=" + "abc123def456" * 4 + "==",
        "an Azure storage account key",
        id="azure-account-key",
    ),
    pytest.param("dapi" + "0123456789abcdef" * 2, "a Databricks token", id="databricks-token"),
    pytest.param("hvs." + "CAESIJlU9JMY" + "EhOPYv4igdhm9PnZ", "a HashiCorp Vault token", id="vault-token"),
    pytest.param("npm_" + "abcdefghij" * 3 + "012345", "an npm token", id="npm-token"),
    pytest.param(
        "SG." + "abcdefghijk" * 2 + "." + "abcdefghijklmnopqrstuvwxyz" + "0123456789ABCDEFG",
        "a SendGrid API key",
        id="sendgrid-key",
    ),
    pytest.param("secret key: " + "wJalrXUtn/" * 4, "an AWS secret access key", id="aws-secret-key-label"),
]


@pytest.mark.parametrize(("fragment", "kind"), PATTERNS)
def test_find_secret_names_the_kind_and_the_line_never_the_value(fragment: str, kind: str) -> None:
    finding = find_secret(_in_a_file(fragment))
    assert finding == SecretFinding(kind, 3)
    assert finding.describe() == f"{kind} on line 3"
    assert fragment not in finding.describe() and fragment not in repr(finding)


@pytest.mark.parametrize(
    "fragment",
    [
        pytest.param("-----BEGIN PUBLIC KEY-----", id="public-key"),
        pytest.param("AKIA" + "X" * 17, id="aws-access-key-id-too-long"),
        pytest.param("akia" + "x" * 16, id="aws-access-key-id-lower-case"),
        pytest.param("gh" + "p_" + "a" * 35, id="github-too-short"),
        pytest.param("sk-" + "a" * 31, id="sk-key-too-short"),
        pytest.param("sk" + "_test_" + "a" * 24, id="stripe-test-key"),
        pytest.param("AIza" + "a" * 36, id="google-too-long"),
        pytest.param("xox" + "b-" + "short", id="slack-too-short"),
        pytest.param("eyJ" + "a" * 8 + ".eyJ" + "b" * 8, id="jwt-without-signature"),
        pytest.param("password: " + "hunter2", id="value-of-7-characters"),
        pytest.param("password: " + "correcthors", id="value-of-11-characters-without-digits"),
        pytest.param("db password=" + "supersecret" + "value", id="long-value-in-lower-case"),
        pytest.param("password: " + "Authorization", id="long-word-with-a-capital"),
        pytest.param("Authorization: Bearer " + "${TOKEN}", id="bearer-placeholder"),
        pytest.param("Authorization: Bearer " + "<token>", id="bearer-angle-placeholder"),
        pytest.param("glpat-" + "short", id="gitlab-too-short"),
        pytest.param("hvs." + "short", id="vault-too-short"),
        pytest.param("password: " + "12345678", id="value-without-letters"),
    ],
)
def test_near_misses_are_not_secrets(fragment: str) -> None:
    assert find_secret(_in_a_file(fragment)) is None


def test_find_secret_reports_the_first_line_that_holds_one() -> None:
    text = _lines(
        "# Systems",
        "",
        "- ORG-1: No secret here.",
        "",
        "- the token " + "gh" + "p_" + "b" * 36,
        "- the key " + "AKIA" + "Z" * 16,
    )
    assert find_secret(text) == SecretFinding("a GitHub token", 5)


def test_find_secret_counts_lines_of_every_line_ending() -> None:
    text = "a\r\nb\r\nc " + "AKIA" + "Z" * 16 + "\r\n"
    assert find_secret(text) == SecretFinding("an AWS access key ID", 3)


def test_a_text_without_secret_gives_none() -> None:
    assert find_secret("") is None
    assert find_secret("", ["known" + "Value2026"]) is None
    assert find_secret(_lines("# Organization Context", "", "- ORG-1: A rule.")) is None


# --------------------------------------------------------------------------- placeholders


@pytest.mark.parametrize(
    "line",
    [
        "token: ${TOKEN}",
        "api_key: ${API_KEY}",
        "token: $TOKEN",
        "api_key = $ANTHROPIC_API_KEY",
        "password: <password>",
        "password: <your password here>",
        "secret: {{ secret }}",
        "client_secret: {{client_secret}}",
        "password: ***",
        "password: ********",
        "password: xxxxxxxx",
        "access_key: XXXXXXXXXXXXXXXX",
        "password: [hidden]",
        "password: changeme",
        "password: REDACTED",
        "password: required",
        "The SFTP password: required for every upload; ops rotates it.",
        "api_key: your-api-key",
        "postgresql://user:<password>@host:5432/db",
        "postgresql://etl:${DB_PASSWORD}@db.internal/lab",
        "https://user:$PASSWORD@example.com",
        "https://user:***@example.com",
        "https://user:[hidden]@example.com",
        "https://user:password@example.com",
    ],
)
def test_placeholders_are_not_secrets(line: str) -> None:
    assert find_secret(_lines("# Systems", "", line)) is None


# --------------------------------------------------------------------------- known values

#: 24 characters, put together so that no literal looks like a credential.
KNOWN = "Zq7" + "Lm2" + "Pw9" + "Xc4" + "Vb8" + "Nk3" + "Rt5" + "Hy6"


def test_a_known_value_is_found_whole() -> None:
    finding = find_secret(_in_a_file(KNOWN), [KNOWN])
    assert finding == SecretFinding(KNOWN_SECRET_KIND, 3)
    assert finding.describe() == f"{KNOWN_SECRET_KIND} on line 3"
    assert KNOWN not in finding.describe()


def test_a_16_character_fragment_of_a_known_value_is_found() -> None:
    assert find_secret(_in_a_file(KNOWN[3:19]), [KNOWN]) == SecretFinding(KNOWN_SECRET_KIND, 3)
    assert find_secret(_in_a_file(KNOWN[3:18]), [KNOWN]) is None  # 15 characters are not enough


@pytest.mark.parametrize("separator", ["\n", " ", "\n  "], ids=["line-break", "space", "indented-line"])
def test_a_known_value_split_over_lines_or_spaces_is_found_on_line_1(separator: str) -> None:
    text = _lines("# Systems", "", KNOWN[:12] + separator + KNOWN[12:])
    assert find_secret(text, [KNOWN]) == SecretFinding(KNOWN_SECRET_KIND, 1)


def test_known_values_shorter_than_8_characters_are_ignored() -> None:
    short = "Ab3" + "Cd4" + "E"
    assert find_secret(_in_a_file(short), [short]) is None
    eight = short + "f"
    assert find_secret(_in_a_file(eight), [eight]) == SecretFinding(KNOWN_SECRET_KIND, 3)


def test_known_values_are_stripped_and_only_strings_count() -> None:
    known: list = [f"  {KNOWN}\n", None, 42]
    assert find_secret(_in_a_file(KNOWN), known) == SecretFinding(KNOWN_SECRET_KIND, 3)
    assert find_secret(_in_a_file("nothing"), [None, 42, ""]) is None  # type: ignore[list-item]


def test_patterns_are_looked_for_before_known_values() -> None:
    text = _lines(f"line one {KNOWN}", "line two", "AKIA" + "Z" * 16)
    assert find_secret(text, [KNOWN]) == SecretFinding("an AWS access key ID", 3)


# --------------------------------------------------------------------------- ordinary text


@pytest.mark.parametrize(
    "text",
    [
        "- ORG-1: Files from 2026-10-01 must still load, because old dates are run again.",
        "- ORG-2: The on-call phone is +54 11 5555-1234; write to data-eng@example.com.",
        "- LAB-3 (overrides ORG-3): An empty quantity counts as 0 since 2026-10-02T14:03:00Z.",
        "- CUS-1: `signup_date` is a date (2025-01-14) or an ISO-8601 timestamp in UTC.",
        "The CRM export (`data/incoming/customers.csv`): crm-team, Slack #crm.",
        "See https://example.com/docs/context.md#rules and postgresql://db.internal:5432/lab.",
        "Clone ssh://git@github.com:acme/etl.git and ask ana.perez@example.org for access.",
        "| SKU-1001 | 20 units | 150 units | 0.0.0.0 |",
        "```sql\nSELECT customer_id, email FROM customers WHERE signup_date >= '2026-01-01';\n```",
        "The token rotates every 90 days; the password policy needs 12 characters.",
        "A key such as AKIA... is never written here; nor is sk-... or ghp_...",
        "<!-- Never write secrets here: a file that holds one is not sent to the model. -->",
        "Order 1234567 of customer 7654321 was refunded on 2026-10-27 at 08:30.",
    ],
)
def test_ordinary_prose_and_markdown_are_not_secrets(text: str) -> None:
    assert find_secret(text) is None


# --------------------------------------------------------------------------- gaps


@pytest.mark.parametrize(
    "line",
    [
        "DB_PASSWORD=" + "Pr0d" + "Pass2026",
        "github_token: " + "abc123" + "def456ghi",
        "smtp_password = " + "s3cret" + "Value1",
        "export SNOWFLAKE_PASSWORD=" + "Sn0w" + "flake2026",
    ],
    ids=["env-variable", "yaml-key", "config-key", "shell-export"],
)
def test_a_credential_name_with_a_prefix_is_found(line: str) -> None:
    assert find_secret(line) == SecretFinding("a credential with its value", 1)


@pytest.mark.parametrize("value", ["a1B2" * 10 + "x", "aB" * 20 + "x"], ids=["with-digits", "letters-only"])
def test_a_longer_aws_secret_is_still_a_credential_with_its_value(value: str) -> None:
    finding = find_secret("aws_secret_access_key = " + value)
    assert finding is not None and finding.kind == "a credential with its value"


# --------------------------------------------------------------------------- value on the next line

NEXT_LINE_VALUE = "s3cr3t" + "Passw0rd"


@pytest.mark.parametrize(
    "lines",
    [
        pytest.param(("password:", "  " + NEXT_LINE_VALUE), id="yaml-plain"),
        pytest.param(("db_password: |", "", "  " + NEXT_LINE_VALUE), id="yaml-block-after-a-blank-line"),
        pytest.param(
            ("- ORG-4: Log in with password:", "  " + NEXT_LINE_VALUE + " and say nothing."), id="prose"
        ),
        pytest.param(('"api_key":', '  "' + NEXT_LINE_VALUE + '"'), id="quoted"),
    ],
)
def test_a_credential_whose_value_is_on_the_next_line_is_found(lines: tuple[str, ...]) -> None:
    text = _lines("# Systems", *lines)
    assert find_secret(text) == SecretFinding("a credential with its value", len(lines) + 1)


@pytest.mark.parametrize(
    "lines",
    [
        ("password:", "  is kept in the vault."),
        ("password:", "  ${DB_PASSWORD}"),
        ("## Secret:", "Rotation is quarterly."),
        ("password:", "", "- ORG-2: A rule."),
        ("The password: is required.", "Sentence two follows here."),
    ],
)
def test_a_line_after_a_credential_name_that_is_no_value_is_not_a_secret(lines: tuple[str, ...]) -> None:
    assert find_secret(_lines("# Systems", *lines)) is None


# --------------------------------------------------------------------------- ordinary lines about credentials

#: Lines a context file holds about credentials without holding one: how a fix fetches one, where
#: one lives, quoted errors. None of them may refuse a file.
ABOUT_CREDENTIALS = [
    'password = Variable.get("warehouse_password")',
    'conn_password = BaseHook.get_connection("warehouse").password',
    'api_key = os.environ.get("CRM_API_KEY")',
    "client_secret=settings.CRM_CLIENT_SECRET",
    'db_password = vault_client.read("secret/etl")',
    'secret = Variable.get("crm_secret", deserialize_json=True)',
    'token = os.getenv("API_TOKEN")',
    'password: "{{ conn.warehouse.password }}"',
    "password: '{{conn.wh.password}}'",
    'export PGPASSWORD="$WAREHOUSE_PW"',
    'secret="etl/prod/warehouse-credentials"',
    "pwd = /opt/airflow/dags/repo",
    "etl ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart airflow-worker",
    "- ORG-6: The warehouse password: stored-in-vault, rotated every 90 days.",
    "- ORG-7: The reporting database password: not-needed-with-IAM (the role signs in).",
    "- CUS-2: The CRM client_secret: provided-by-the-vendor, kept by crm-team.",
    "- ORG-8: Every API call sends its token: Authorization header from Vault.",
    "- INV-3: Login: password=ask-in-#data-oncall",
    "- ORG-9: secret: the account manager knows who to call.",
    "- ORG-10: Passphrase = correct horse battery staple is an example of a weak one.",
    "- ORG-11: Never hardcode a password: use Variable.get or BaseHook.get_connection.",
    "Authorization: Bearer <token from the vault>",
    "Authorization: Bearer ${CRM_TOKEN}",
    "The header is `Authorization: Bearer <token>`.",
    "Authorization: Basic followed by the base64 of user:password",
    "authorization: bearer tokens expire after one hour",
    'If the load fails with "password authentication failed for user etl", page #data-oncall.',
    'FATAL: password authentication failed for user "etl"',
    "botocore.exceptions.ClientError: An error occurred (ExpiredToken) when calling the GetObject operation",
    "The service account and its password:\netl_service_account_readonly",
    "- ORG-3: Never commit a password:\n  use the vault instead.",
    "secret:\n  Confidentiality-wise, everything in this file is internal.",
    "password:\n  description: The warehouse password, kept in the vault.",
    "AccountKey=<from the portal>",
    "The SendGrid template SG.welcome-email.v2 is used for onboarding mails.",
    "Our Vault paths look like hvs.policy-for-etl-readonly-users.",
    "npm_package_version_checker_tool_v2_installer_x is a CI step.",
    "GitLab tokens look like glpat-followed-by-twenty-characters.",
]


@pytest.mark.parametrize("line", ABOUT_CREDENTIALS)
def test_ordinary_lines_about_credentials_are_not_secrets(line: str) -> None:
    assert find_secret(_lines("# Systems", "", line)) is None


def test_a_token_shaped_example_is_still_refused() -> None:
    """A value shaped like a real token cannot be told from one: write it as dapi... instead."""
    finding = find_secret("dapi" + "0123456789abcdef" * 2 + " is how a Databricks token starts.")
    assert finding == SecretFinding("a Databricks token", 1)


# --------------------------------------------------------------------------- credentials that look like code

#: AWS's documented example secret key, put together from pieces.
AWS_EXAMPLE_SECRET = "wJalrXUtnFEMI" + "/K7MDENG/" + "bPxRfiCY" + "EXAMPLEKEY"
#: Values with letters and digits stay credentials whatever their separators: dots, a bracket or
#: slashes do not make them code or a path.
STILL_CREDENTIALS = [
    "password: " + "P4ss" + ".Word99",
    "password = " + "hunter2" + ".hunter2",
    "api_key = " + "abc123" + ".def456ghi789",
    "secret: " + "Tango.Down7" + ".Alpha",
    "password: " + "Passw0rd" + "(2026",
    "password: " + "S3cret" + "[prod]x",
    "client_secret: " + "8f9Xk2/Lm3pQ" + "/zR7tYv1",
    "token: " + "/9j4kLmN0pQr" + "S7tUvW2xYz",
    "aws_secret = " + AWS_EXAMPLE_SECRET,
    "AWS_SECRET=" + AWS_EXAMPLE_SECRET,
    "secret: " + AWS_EXAMPLE_SECRET,
    "password = " + AWS_EXAMPLE_SECRET,
]


@pytest.mark.parametrize("line", STILL_CREDENTIALS)
def test_a_value_with_letters_and_digits_is_a_credential_whatever_its_separators(line: str) -> None:
    assert find_secret(_lines("# Systems", "", line)) == SecretFinding("a credential with its value", 3)


#: What the check no longer finds, the price of reading the lines of ABOUT_CREDENTIALS
#: (docs/security.md, 4.7, "What remains").
ACCEPTED_MISSES = [
    "db password=" + "supersecret" + "value",
    "passphrase: " + "correcthorse" + "batterystaple",
    "password: " + "correct-horse" + "-battery-staple",
    "password: " + "Correct_Horse" + "_Battery_Staple",
    "Authorization: Basic " + "dXNlcjpw" + "YXNz",
    "Authorization: Bearer " + "abcdef" + "ghijkl",
    "glpat-" + "abcdefgh-ijklmnop" + "_qrstuv",
    "hvs." + "abcdefghijklm" + "nopqrstuvwxyz",
    "password:\n  " + "s3cr3t" + "Passw0rd: true",
]


@pytest.mark.parametrize("line", ACCEPTED_MISSES)
def test_the_accepted_misses_stay_documented(line: str) -> None:
    assert find_secret(_lines("# Systems", "", line)) is None
