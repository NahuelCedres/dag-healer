# Claude on Amazon Bedrock

dag-healer can run Claude through Amazon Bedrock in your own AWS account instead of the Claude
API: set `llm.provider: bedrock` in `healer.yaml`, or `HEALER_LLM_PROVIDER=bedrock` in the
worker's environment (`dag-healer worker --provider bedrock` does it for one worker). The
classification and the repairs use the same prompts, tools, limits and flow as with `anthropic`.
What changes is where the requests go, how they are signed, and the few features listed in "What
differs from the Claude API". The offline `fake` provider stays for tests and the demo.

This page sets up the AWS side: access to the model, its ID, the region, the credentials and their
IAM policy, an EC2 instance role, a VPC endpoint, and what `dag-healer doctor` reports. The
examples use the AWS account `123456789012`, the Region `eu-west-1` (Ireland) and Claude Opus 5.5
through its EU inference profile. [docs/security.md](security.md) describes what dag-healer sends
to the model and what the test sandbox keeps away from the code it runs, and
[docker/README.md](../docker/README.md) ("LLM provider") selects Bedrock in the development stack.

## How dag-healer calls Amazon Bedrock

The bedrock provider uses the `AnthropicBedrock` client of the `anthropic` SDK, which needs boto3
and botocore for the AWS credentials and the request signatures. Install them with the `aws`
extra:

```sh
pip install "dag-healer[aws]"
```

Every request goes to the Bedrock runtime endpoint (`bedrock-runtime`) of the region, as an
`InvokeModel` call (`POST /model/<model ID>/invoke`) signed with AWS Signature Version 4 for the
service `bedrock`. dag-healer never streams, so `bedrock:InvokeModel` is the only IAM action it
needs.

Bedrock also serves the Messages API on a second endpoint, `bedrock-mantle`
(`https://bedrock-mantle.<region>.api.aws/anthropic`). dag-healer does not use it and has no setting
for it: AWS recommends `bedrock-runtime` for new applications; only `bedrock-runtime` offers
cross-Region inference profiles, Guardrails, model invocation logging and application inference
profiles; and AWS lists Claude Opus 5.5 on `bedrock-mantle` in three Regions only (`us-east-1`,
`ap-southeast-4` and `us-gov-west-1`), while `bedrock-runtime` serves it, through inference
profiles, from most commercial Regions. `bedrock-mantle` would also need another IAM action
(`bedrock-mantle:CreateInference`). Everything that dag-healer uses works on `bedrock-runtime`:
structured outputs, `output_config.effort`, explicit prompt caching and client-side tools.

## Before you start: access to the model

An administrator of the AWS account does this once (per account, or once for an organization in
its management account):

1. **The First Time Use form.** Anthropic's models on `bedrock-runtime` need use case details
   first: in the Amazon Bedrock console, open an Anthropic model of the **Model catalog** in the
   playground, or the Anthropic provider page, and submit the form. Access can stay pending for a
   while after it; `aws bedrock get-foundation-model-availability --model-id
   anthropic.claude-opus-5-5` shows its state (`agreementAvailability`).
2. **The AWS Marketplace subscription.** Claude models are sold through AWS Marketplace and billed
   on the AWS bill. The first call of a model in the account subscribes the account to it, which
   needs `aws-marketplace:Subscribe`, `aws-marketplace:Unsubscribe`,
   `aws-marketplace:ViewSubscriptions` and a valid payment method. Make that first call as the
   administrator, for example in the model's playground. Once the model is enabled, invoking it
   needs no Marketplace permission, so the worker's role never gets one. The first call must meet
   the conditions that the administrator's own policies put on `bedrock:InvokeModel`.
3. **The model's access.** Check it in the Amazon Bedrock console. When the model still answers
   `AccessDeniedException` after the First Time Use form and the subscription, ask AWS for
   access.
4. **Data retention.** Bedrock keeps prompts and answers according to the data retention mode of
   the account in each Region. With `none`, nothing is stored; with `default` (or no setting), the
   model's own policy applies, and AWS may keep them for abuse detection. Claude Fable 5 and Fable
   5.1 require `aws_review`: AWS then keeps their prompts and answers for up to 30 days and may
   review them. The model provider never receives them, and with cross-Region inference, retained
   data is stored in the Region that processed the request. Check the mode before the worker
   sends health data or other regulated data: an administrator reads and sets it through the
   Bedrock control plane (`bedrock:GetAccountDataRetention`, `bedrock:PutAccountDataRetention`),
   and the worker needs neither permission.

## The model: `llm.model`

With `provider: bedrock`, `llm.model` holds a Bedrock model ID, an inference profile ID, or the
ARN of either (an application inference profile included). A Claude API model name such as
`claude-opus-5-5`, the default of `llm.model`, is refused before any request, and doctor reports
it:

```text
llm.model claude-opus-5-5 is a Claude API model ID; with llm.provider bedrock it must be a Bedrock model ID, inference profile ID or ARN, such as eu.anthropic.claude-opus-5-5 or global.anthropic.claude-opus-5-5 (docs/bedrock.md)
```

dag-healer does not translate the name because the Bedrock ID also decides where the requests are
processed. For Claude Opus 5.5 on `bedrock-runtime`, AWS's model card lists:

| `llm.model` | Called from | Processed in |
|---|---|---|
| `anthropic.claude-opus-5-5` | `eu-west-2` (London) only | `eu-west-2` |
| `eu.anthropic.claude-opus-5-5` | the EU Regions | from `eu-west-1`: `eu-central-1`, `eu-north-1`, `eu-south-1`, `eu-south-2`, `eu-west-1` or `eu-west-3`; from `eu-west-2` and `eu-central-2`, also that Region |
| `us.anthropic.claude-opus-5-5` | the US Regions, `ca-central-1`, `ca-west-1` | from `us-east-1`: `us-east-1`, `us-east-2` or `us-west-2`; from `us-west-1` and the Canadian Regions, also that Region |
| `au.anthropic.claude-opus-5-5` | `ap-southeast-2`, `ap-southeast-4` | `ap-southeast-2` or `ap-southeast-4` |
| `jp.anthropic.claude-opus-5-5` | `ap-northeast-1`, `ap-northeast-3` | `ap-northeast-1` or `ap-northeast-3` |
| `global.anthropic.claude-opus-5-5` | most commercial Regions | any commercial Region |

The Regions that a geographic profile routes to depend on the Region that calls it, and can
change: `aws bedrock get-inference-profile --inference-profile-identifier
eu.anthropic.claude-opus-5-5 --region eu-west-1` shows the current ones. For health data, use the
in-Region ID or the geographic profile of the data's jurisdiction, and deny global routing (see
"The IAM policy"). Anthropic's Bedrock documentation notes that regional routing (in-Region or
geographic) costs 10 percent more than global routing.

In the Bedrock console, model IDs are on each model's card in the **Model catalog** (Programmatic
Access), and inference profile IDs under **Cross-Region inference** in the navigation pane. Other
Claude models have IDs of the same form (`anthropic.claude-sonnet-5-5`,
`anthropic.claude-haiku-5-5`, ...) with Regions and profiles of their own: check their model cards.
Use Claude Opus 4.7 or a newer model: the `anthropic` SDK sends its own API version with every
request, and the ARN-versioned models (Claude Opus 4.6, `anthropic.claude-opus-4-6-v1`; Sonnet
4.6, `anthropic.claude-sonnet-4-6`; Haiku 4.5 and the older models) can refuse it with a 400.

The worker reads `llm.model` from `healer.yaml`. The `healer.yaml` that `dag-healer init` writes
takes it from `HEALER_LLM_MODEL` (`model: "${HEALER_LLM_MODEL:-claude-opus-5-5}"`), so the model
can come from the environment like `HEALER_LLM_PROVIDER`. A `healer.yaml` written by an earlier
version names the model literally (`model: claude-opus-5-5`): edit that line.

## The region and the endpoint

The provider takes the region from, in this order, `llm.aws_region`, `AWS_REGION`,
`AWS_DEFAULT_REGION`, and the region of the AWS profile (`llm.aws_profile`, `AWS_PROFILE` or the
default profile). Empty values count as unset, but an empty `AWS_DEFAULT_REGION` also hides the
profile's region from the AWS SDK: unset it instead. A value of spaces only in `AWS_REGION` or
`AWS_DEFAULT_REGION` is refused when no other region is set, because the SDK would take it as the
region (`AWS_REGION is set but blank, and the AWS SDK would take it as the region: unset it, or
set llm.aws_region`). An EC2 instance role gives credentials but no region: on EC2, set one of
the first three. Without a region, nothing is sent, and doctor reports:

```text
no AWS region for the bedrock provider: set llm.aws_region, AWS_REGION or AWS_DEFAULT_REGION, or a region in the AWS profile
```

The region names the host that receives the signed requests, so wherever it comes from it must
be an AWS Region code, such as `eu-west-1`, `us-gov-west-1` or `eusc-de-east-1`. Anything else
is refused before a request is sent, without its value: `AWS_REGION is not an AWS region such as
eu-west-1` (or `AWS_DEFAULT_REGION`, or `the region of the AWS profile`), and doctor shows
`region invalid`.

The requests go to `https://bedrock-runtime.<region>.amazonaws.com`, and the region is also the
source Region of an inference profile (the "Called from" column above). `llm.endpoint_url`
replaces that URL, for a VPC endpoint without private DNS (see "A VPC endpoint") or a gateway of
your own in front of `bedrock-runtime`. Without `llm.endpoint_url`, the SDK's
`ANTHROPIC_BEDROCK_BASE_URL` does the same, with the same rules: an `https` URL (`http` only for
a loopback host, such as a proxy on `127.0.0.1`) without user name, password, query or fragment.
An empty one is an error (`ANTHROPIC_BEDROCK_BASE_URL is set but empty: unset it, or set
llm.endpoint_url`), and so is one that breaks the rules (`ANTHROPIC_BEDROCK_BASE_URL is not an
https URL (http only for a loopback host) without credentials, query or fragment: fix or unset
it, or set llm.endpoint_url`). The provider hands the endpoint, and a region that does not come
from the profile, to the SDK itself. The requests stay signed for the region, so the endpoint
must be one of that Region. It receives every request as the SDK sends it: signed, with the
session token of temporary credentials, or with the Bedrock API key in its `Authorization`
header. Point it only at an endpoint or a gateway that you control.

The provider never follows a redirect. An answer such as a `307` ends the call after that one
request (`Amazon Bedrock answered <action> with a redirect (HTTP 307), which the bedrock provider
never follows: check llm.endpoint_url, ANTHROPIC_BEDROCK_BASE_URL and the proxy settings`), so
neither the request, with its credentials, nor the prompt goes to the host that the redirect
names.

For an outbound HTTP proxy, set `HTTPS_PROXY` instead (and, on EC2, `NO_PROXY=169.254.169.254`):
the SDK's HTTP client follows the proxy variables, and the endpoint stays `bedrock-runtime`. An
`http` endpoint on a loopback host never goes through a proxy, whatever `HTTP_PROXY` or
`ALL_PROXY` say: its requests, signed and with the prompts, travel in cleartext.

## Credentials

The AWS SDK looks for credentials along its default chain: the environment (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), the shared AWS files (`~/.aws/credentials` and
`~/.aws/config`, with their single sign-on and assume-role profiles), the credentials of an ECS
task or an EKS pod, and the role of an EC2 instance, through the instance metadata service.
`healer.yaml` holds no AWS credential and has no setting for one.

- **On AWS, use a role**: the EC2 instance role (see "On EC2: the instance role and IMDSv2"), the
  task role of an ECS task, or the IAM role of an EKS pod. The SDK renews their temporary
  credentials by itself.
- **For local development**, a profile of the shared AWS files: `llm.aws_profile` or `AWS_PROFILE`
  (run `aws sso login --profile <profile>` first for a single sign-on profile).
- **Bedrock API keys** work too, in `AWS_BEARER_TOKEN_BEDROCK`: the SDK sends the key in an
  `Authorization: Bearer` header instead of a signature. A short-term key lasts up to 12 hours and
  has the permissions of the identity that created it; once it expires, Bedrock refuses every call
  until the worker restarts with a new key. A long-term key creates an IAM user, works only in the
  Region that issued it, and AWS means it for exploration only. The action
  `bedrock:CallWithBearerToken` (condition key `bedrock:bearerTokenType`) controls the use of keys.
  Prefer a role.

`llm.aws_profile` and `AWS_BEARER_TOKEN_BEDROCK` exclude each other (`llm.aws_profile and
AWS_BEARER_TOKEN_BEDROCK are both set: unset one of them`). An empty `AWS_BEARER_TOKEN_BEDROCK`
counts as unset, and the requests are signed with the chain's credentials; next to
`llm.aws_profile`, the region must then come from `llm.aws_region` or `AWS_REGION` (see
"Troubleshooting").

The whitespace around the value of `AWS_BEARER_TOKEN_BEDROCK`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` or the session token (`AWS_SESSION_TOKEN`, or `AWS_SECURITY_TOKEN`, its
legacy name, which botocore reads first), such as the newline at the end of a value read from a
file, falls away. No credential holds whitespace, a control character or a non-ASCII character
within its value: such a value in one of these five variables is refused before any request,
without being quoted (`AWS_SESSION_TOKEN holds whitespace, a control character or a non-ASCII
character within its value: fix or unset it`).

## The IAM policy

Give the worker an identity of its own (a role) whose only permission is `bedrock:InvokeModel` on
the model it uses. With an inference profile, that is the profile in the Region that calls it,
and the foundation model in every Region that the profile routes to, the latter through that
profile only (the condition key `bedrock:InferenceProfileArn`). For
`eu.anthropic.claude-opus-5-5` called from `eu-west-1`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "InvokeTheInferenceProfile",
      "Effect": "Allow",
      "Action": "bedrock:InvokeModel",
      "Resource": "arn:aws:bedrock:eu-west-1:123456789012:inference-profile/eu.anthropic.claude-opus-5-5"
    },
    {
      "Sid": "InvokeTheModelThroughTheProfileOnly",
      "Effect": "Allow",
      "Action": "bedrock:InvokeModel",
      "Resource": [
        "arn:aws:bedrock:eu-central-1::foundation-model/anthropic.claude-opus-5-5",
        "arn:aws:bedrock:eu-north-1::foundation-model/anthropic.claude-opus-5-5",
        "arn:aws:bedrock:eu-south-1::foundation-model/anthropic.claude-opus-5-5",
        "arn:aws:bedrock:eu-south-2::foundation-model/anthropic.claude-opus-5-5",
        "arn:aws:bedrock:eu-west-1::foundation-model/anthropic.claude-opus-5-5",
        "arn:aws:bedrock:eu-west-3::foundation-model/anthropic.claude-opus-5-5"
      ],
      "Condition": {
        "StringEquals": {
          "bedrock:InferenceProfileArn": "arn:aws:bedrock:eu-west-1:123456789012:inference-profile/eu.anthropic.claude-opus-5-5"
        }
      }
    }
  ]
}
```

- **In-Region model** (`anthropic.claude-opus-5-5` in `eu-west-2`): one statement, with
  `bedrock:InvokeModel` on `arn:aws:bedrock:eu-west-2::foundation-model/anthropic.claude-opus-5-5`.
- **Global profile**: the first statement names
  `arn:aws:bedrock:eu-west-1:123456789012:inference-profile/global.anthropic.claude-opus-5-5`, and
  the second the foundation model in the calling Region
  (`arn:aws:bedrock:eu-west-1::foundation-model/anthropic.claude-opus-5-5`) and the Region-less
  `arn:aws:bedrock:::foundation-model/anthropic.claude-opus-5-5`, with the global profile's ARN in
  the condition.
- **Application inference profile**: its ARN (in `llm.model` too), and the foundation models that
  it routes to.

To keep the worker's requests from being routed globally, whatever else its policies allow, add
AWS's explicit deny of global routing to the statements:

```json
{
  "Sid": "NoGlobalRouting",
  "Effect": "Deny",
  "Action": "bedrock:InvokeModel*",
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "aws:RequestedRegion": "unspecified"
    }
  }
}
```

The policy needs no `bedrock:InvokeModelWithResponseStream`, since dag-healer never streams, and
no `aws-marketplace` action, since the administrator enabled the model. AWS's general page on the
prerequisites of inference profiles also grants `bedrock:GetInferenceProfile`; its examples for a
single profile, which this policy follows, grant only the invoke actions, and dag-healer calls
nothing else. If Bedrock refuses the first call with a 403 that names another action, add that
action on the profile's ARN. Service control policies, permissions boundaries and the policy of a
VPC endpoint apply on top of this policy.

## On EC2: the instance role and IMDSv2

1. Create a role for EC2 (IAM console: Roles, Create role, AWS service, EC2), for example
   `dag-healer-worker`, with the policy above as its only permissions.
2. Attach it to the worker's instance (EC2 console: the instance, Actions, Security, Modify IAM
   role).
3. Require IMDSv2. When the worker runs in a container on that instance (the development stack's
   `healer-worker`, or an image of your own), also raise the hop limit of the metadata responses
   to 2:

   ```sh
   aws ec2 modify-instance-metadata-options --instance-id i-0123456789abcdef0 \
     --http-tokens required --http-put-response-hop-limit 2
   ```

   With the default hop limit of 1, the answers of IMDSv2 do not reach a container behind a
   network bridge, one hop further away: the SDK in the container finds no credentials, and doctor
   reports `no AWS credentials for the bedrock provider: ...`. (Instances of an AMI registered
   with `imds-support` `v2.0` start with 2.) With 2, every container with network access on the
   instance can get the role's credentials: run nothing else on the worker's host
   ([docs/security.md](security.md), section 6). The test sandbox's containers have no network and
   cannot reach them.
4. Give the worker the region: `llm.aws_region`, or `AWS_REGION` in its environment (for a
   container, in the container's environment).
5. Run `dag-healer doctor` where the worker runs (on the host, or in its container): the `llm`
   line ends with `AWS credentials resolve (iam-role; values hidden)`.

Behind an HTTP proxy, set `NO_PROXY=169.254.169.254` next to `HTTPS_PROXY`, so that the requests
to the instance metadata service do not go through the proxy. Keep `AWS_EC2_METADATA_DISABLED`
out of the worker's own environment: it hides the instance role from the SDK (dag-healer sets it
in the environment of the test sandbox only).

On ECS or EKS, the task role or the pod's role reaches the worker through the same chain; doctor
then names `container-role` or `assume-role-with-web-identity`, and the hop limit does not apply.

## A VPC endpoint

To reach Bedrock without the internet, create an interface VPC endpoint (AWS PrivateLink) for
`com.amazonaws.eu-west-1.bedrock-runtime` in the worker's VPC.

- **With private DNS** enabled on the endpoint, `bedrock-runtime.eu-west-1.amazonaws.com` resolves
  to it inside the VPC: nothing changes in `healer.yaml`.
- **Without private DNS**, set `llm.endpoint_url` to the endpoint's DNS name, for example
  `https://vpce-0123456789abcdef0-abcdefgh.bedrock-runtime.eu-west-1.vpce.amazonaws.com`, and keep
  the region of the endpoint in `llm.aws_region` or `AWS_REGION`.

An endpoint policy can let only the worker's role call `bedrock:InvokeModel` through it:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:role/dag-healer-worker"
      },
      "Action": "bedrock:InvokeModel",
      "Resource": "*"
    }
  ]
}
```

A request that comes through a VPC endpoint carries no `aws:SourceIp`, so an `IpAddress` condition
on that key never matches it: restrict by `aws:SourceVpce` instead. The instance metadata service
needs no endpoint: an instance role works in a VPC without internet access.

## `healer.yaml` and `dag-healer doctor`

```yaml
llm:
  provider: bedrock                     # or HEALER_LLM_PROVIDER=bedrock
  model: eu.anthropic.claude-opus-5-5   # a Bedrock model ID, inference profile ID or ARN
  effort: high
  max_tokens: 16000
  fallbacks: false                      # Amazon Bedrock has no server-side refusal fallbacks
  aws_region: eu-west-1                 # else AWS_REGION, AWS_DEFAULT_REGION, the profile's region
  # aws_profile: dag-healer-dev         # a profile of the shared AWS files (local development)
  # endpoint_url: https://vpce-0123456789abcdef0-abcdefgh.bedrock-runtime.eu-west-1.vpce.amazonaws.com
```

`aws_region`, `aws_profile` and `endpoint_url` apply to the bedrock provider only. `aws_region`
must be an AWS Region code such as `eu-west-1`; `endpoint_url` an `https` URL (`http` only for a
loopback host: `localhost`, `127.0.0.0/8` or `::1`) without user name, password, query or
fragment. `model` loses the spaces around it and cannot hold whitespace or a control character.
A value that breaks a rule is refused without being quoted.

`dag-healer doctor` checks the provider that `HEALER_LLM_PROVIDER` or `llm.provider` names (it
does not see `dag-healer worker --provider`). For bedrock, its `llm` line shows the model, the
region (`invalid` for one that is no AWS Region code) and `llm.endpoint_url` when it is set, then
whether AWS credentials resolve, by where they come from (`env`, `shared-credentials-file`, `sso`,
`assume-role`, `container-role`, `iam-role`, ...), never by their values. doctor applies the
provider's own rules for the settings, so it refuses every setting that the worker would refuse:

```text
[ OK ] llm  provider bedrock (model eu.anthropic.claude-opus-5-5, region eu-west-1): AWS credentials resolve (iam-role; values hidden)
[ OK ] llm  provider bedrock (model eu.anthropic.claude-opus-5-5, region eu-west-1): AWS_BEARER_TOKEN_BEDROCK is set (value hidden)
[FAIL] llm  provider bedrock (model claude-opus-5-5, region eu-west-1): llm.model claude-opus-5-5 is a Claude API model ID; with llm.provider bedrock it must be a Bedrock model ID, inference profile ID or ARN, such as eu.anthropic.claude-opus-5-5 or global.anthropic.claude-opus-5-5 (docs/bedrock.md)
[FAIL] llm  provider bedrock (model eu.anthropic.claude-opus-5-5, region eu-west-1): no AWS credentials for the bedrock provider: attach an instance role to the worker's host (in a container on EC2, set the IMDSv2 hop limit to 2), or set AWS_PROFILE or AWS_BEARER_TOKEN_BEDROCK (docs/bedrock.md)
```

The column after `llm` is as wide as the longest check name of the run. A `[FAIL]` line lists
every problem it found, separated by `; `. Finding an instance role, or that there is none, takes
a few seconds at most (one attempt per request to the instance metadata service, with a 1 second
timeout). doctor never calls the model, AWS STS or the Bedrock control plane, so an OK line says
that the region, the form of the model ID and the credentials are in place; it does not say
that the role may invoke the model, that the model is enabled for the account or that the ID
exists in that Region. Keys of the environment whose `AWS_CREDENTIAL_EXPIRATION` is past, as
`aws configure export-credentials --format env` leaves them in a shell once they expire, are an
error, as they are for the worker (`the AWS credentials for the bedrock provider cannot be
resolved: RuntimeError: Credentials were refreshed, but the refreshed credentials are still
expired.`). For a profile that assumes a role or uses single sign-on, the line names
`assume-role` or `sso` without fetching the credentials, and doctor never refreshes the
credentials that a credential process prints: an expired single sign-on login, or expired
credentials of a credential process, show up only at the first call. The first classification
shows the rest, and so does the live test (last section).

## What differs from the Claude API

- **No server-side refusal fallbacks.** Bedrock has none, so the bedrock provider never asks for
  them, whatever `llm.fallbacks` says. A refusal ends the classification, which the policy then
  decides alone, or the repair, as on the Claude API with `fallbacks: false`. When `healer.yaml`
  itself sets `llm.fallbacks: true` (the `healer.yaml` that `dag-healer init` writes does),
  `dag-healer check` and the worker, once when it starts, warn: `llm.fallbacks is true, but
  Amazon Bedrock has no server-side refusal fallbacks: the bedrock provider sends none (set
  llm.fallbacks: false to silence this warning)`. Left out of the file, it stays silent.
- **Structured outputs, with a fallback.** The classification asks for an answer in the JSON
  schema of the classification (`output_config.format`), as on the Claude API. When Bedrock
  refuses that for the model (a 400 whose message names `format`, `output_format`, `json_schema`
  or structured outputs) or the answer does not validate, the provider logs a warning that names
  the model and, until the worker restarts, asks for one JSON object in the answer instead,
  validated against the same schema, with one retry. A second invalid answer ends the call with
  `Claude's classification on Amazon Bedrock was not a JSON object of the expected schema`, and
  the policy decides alone. The switch costs one more request, and the retry another.
- **The same**: `output_config.effort`, the system prompt as an explicit prompt-caching
  breakpoint (Bedrock caches only explicit breakpoints), client-side tools without a forced
  `tool_choice`, no `thinking` parameter, no streaming, and the whole conversation resent at
  every turn of a repair. dag-healer uses none of the Claude API features that Bedrock lacks
  (Message Batches, token counting, the Models and Files APIs, the server-side tools other than
  tool search) besides the fallbacks.
- **Errors, quotas and billing** are Amazon Bedrock's: dag-healer's error messages name Bedrock
  and the fix (see "Troubleshooting"), the quotas are Bedrock's per model and Region, and the usage
  appears on the AWS bill under the model provider, through AWS Marketplace. The tokens of every
  billed answer count in the incident's `tokens_used`, those of a classification that then fails
  included. `limits.max_tokens_per_incident` applies to each repair run, not to the
  classification ([docs/security.md](security.md), section 4.2).

## The test sandbox and the AWS credentials

The code of a repair (the repository's tests, the generated test and the fix) runs in the test
sandbox with a scrubbed environment. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`, `AWS_SECURITY_TOKEN`, `AWS_BEARER_TOKEN_BEDROCK`, the profile, role and
container-credential variables, the legacy `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`,
`AWS_CREDENTIAL_FILE`, `EC2_ACCESS_KEY` and `EC2_SECRET_KEY`, every `ANTHROPIC_*` variable and
botocore's Bedrock endpoint overrides are removed from it. `AWS_SHARED_CREDENTIALS_FILE`,
`AWS_CONFIG_FILE` and `BOTO_CONFIG` are set to empty values, so that botocore and boto3 read no
shared AWS file and no boto config file (`~/.boto`, `/etc/boto.cfg`), and
`AWS_EC2_METADATA_DISABLED=true` tells the AWS SDKs not to ask the instance metadata service.

- **Under the container backend** (the default), the test sandbox never sees the AWS credentials:
  the container gets none of them and none of the worker's files, and it runs without network, so
  the instance metadata service and the ECS credential endpoint are out of reach.
- **Under the `process` backend**, the code runs as the worker's OS user, with the worker's files
  and network, and the scrub cleans only its own environment. That code can read the worker's
  environment, every credential that the scrub removed included (on Linux in
  `/proc/<parent pid>/environ`, on macOS with `ps -E -p <parent pid>`). It can open the files that
  the worker's AWS variables point at, and the worker user's `~/.aws` and `~/.boto`. On EC2 it can
  ask the instance metadata service for the role's credentials itself
  (`AWS_EC2_METADATA_DISABLED` only advises the SDKs). AWS SDKs that treat an empty
  `AWS_CONFIG_FILE` as unset, such as the JavaScript v3 and Go v2 SDKs, read `~/.aws` as well.
  Keep the container backend wherever the worker holds AWS credentials.

[docs/security.md](security.md) (section 4.3) lists the rules.

## Troubleshooting

A classification that fails falls back to the deterministic policy: the worker logs `LLM
classification unavailable, using the policy alone: <message>`, and the incident's diagnosis
starts with `The LLM was unavailable (<message>)`. A repair that fails ends `repair_failed` with
`The LLM is unavailable: <message>`. `<action>` is `the incident classification`, `the repair
session (test)` or `the repair session (fix)`.

No message quotes a credential. The values of the credentials that the client authenticates with
(the Bedrock API key; the access key, secret key and session token of its AWS session, from a
profile or a role as well as from the environment) and of the credential variables of the
environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`,
`AWS_SECURITY_TOKEN`, `AWS_BEARER_TOKEN_BEDROCK`) are hidden as they are, URL-encoded or
JSON-escaped, and so is any run of 16 or more of their characters. So are access key IDs, the
value after a credential's name (a header, a JSON or XML field, a parameter) and long
random-looking strings: `[hidden]` stands in for them, and for the whole of a message in which
one of those credential values can still be read once its spaces are taken out. `<detail>` is
AWS's message, or, when that message talks about a token, a signature, a secret, an API key, a
password, a bearer token, authorization or an `x-amz-` header, the AWS error type of the answer
followed by `(AWS's message is not shown: it is about the credentials)`, or that note alone when
the answer has no error type. An answer without a JSON message gives its error type alone, or no
detail. The error type is the name in the answer's `x-amzn-ErrorType` header, else in its
`__type` field, without what comes after a `:` or before a `#`, such as `AccessDeniedException`
or `InternalFailure`. It is shown only when it is capitalised words of letters, 3 to 64 in all,
and holds no credential; any other value reads `an unknown error type`. A refusal ends the
classification with `Claude declined <action> (refusal category: <category>)`, where the
category is shown only when it is lower-case letters and `_`, 32 at most, such as `cyber`, and
holds no credential; `unspecified` stands in for any other value. The warning that the worker
logs for a refusal during a repair, `Claude declined the repair session (refusal category:
<category>)`, follows the same rule, with both providers. With the bedrock provider, the
worker's line at the end of a repair, `Claude repair session finished: stop_reason=<reason>,
...`, names a stop reason of Claude's (`end_turn`, `tool_use`, `max_tokens`, ...) only, `other`
standing in for any other value, and no server-side fallback is logged: Bedrock has none.

The logs show neither the headers nor the reason phrase of an answer, which AWS or a gateway
can fill with an echo of a credential. In the debug records (`-vv`) that would print the
headers, `[headers not shown]` stands in for them; in the records that would print the reason
phrase (`-v` and `-vv`, or `ANTHROPIC_LOG=info`), the standard one of the status code stands in
for it (`HTTP/1.1 403 Forbidden`). The records that quote the bytes of an answer that the HTTP
library could not read (a status line, a chunk header) are dropped.

| Message | Where | What to do |
|---|---|---|
| `the bedrock provider needs boto3 and botocore: pip install "dag-healer[aws]"` | doctor, worker | install the `aws` extra where the worker runs |
| `llm.model <model> is a Claude API model ID; with llm.provider bedrock it must be a Bedrock model ID, inference profile ID or ARN, ...` | doctor, worker | set `llm.model` (or `HEALER_LLM_MODEL`) to a Bedrock ID ("The model: `llm.model`") |
| `no AWS region for the bedrock provider: ...` | doctor, worker | set `llm.aws_region` or `AWS_REGION` |
| `AWS_REGION is not an AWS region such as eu-west-1` (or `AWS_DEFAULT_REGION`, `llm.aws_region`, `the region of the AWS profile`) | doctor, worker | write an AWS Region code, such as `eu-west-1`, where the message says |
| `AWS_REGION is set but blank, and the AWS SDK would take it as the region: unset it, or set llm.aws_region` (or `AWS_DEFAULT_REGION`) | doctor, worker | unset the variable, or set a region |
| `no AWS credentials for the bedrock provider: ...` | doctor, worker | attach the instance role; in a container on EC2, set the hop limit to 2 and keep `AWS_EC2_METADATA_DISABLED` unset; or set `AWS_PROFILE` or `AWS_BEARER_TOKEN_BEDROCK` |
| `llm.aws_profile and AWS_BEARER_TOKEN_BEDROCK are both set: unset one of them` | doctor, worker | keep one of them |
| `llm.aws_profile is set and AWS_BEARER_TOKEN_BEDROCK is set but empty: ...` | doctor, worker | unset `AWS_BEARER_TOKEN_BEDROCK`, or set `llm.aws_region` or `AWS_REGION` |
| `AWS_SESSION_TOKEN holds whitespace, a control character or a non-ASCII character within its value: fix or unset it` (or `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SECURITY_TOKEN`, `AWS_BEARER_TOKEN_BEDROCK`) | doctor, worker | copy the value again, whole and on one line, or unset the variable |
| `ANTHROPIC_BEDROCK_BASE_URL is set but empty: unset it, or set llm.endpoint_url` | doctor, worker | unset the variable, or set `llm.endpoint_url` |
| `ANTHROPIC_BEDROCK_BASE_URL is not an https URL (http only for a loopback host) without credentials, query or fragment: fix or unset it, or set llm.endpoint_url` | doctor, worker | an `https` URL of the endpoint, nothing after its path |
| `llm.model holds whitespace or a control character; ...` | worker | a model given to the provider in code; `healer.yaml` refuses such a value when it loads |
| `the AWS credentials for the bedrock provider cannot be resolved: <type>: ...` (from the worker: `... cannot be resolved during <action>: <type>: ...`) | doctor, worker | `ProfileNotFound`: the profile is missing from the shared AWS files of the worker's user, or `AWS_PROFILE` is set but empty; single sign-on errors: `aws sso login --profile <profile>` |
| `the AWS credentials for the bedrock provider cannot be resolved during <action>: RuntimeError: Credentials were refreshed, but the refreshed credentials are still expired.` (from doctor: `... cannot be resolved: RuntimeError: ...`) | doctor, worker | credentials found but expired, such as exported ones whose `AWS_CREDENTIAL_EXPIRATION` is past: export new ones and restart the worker |
| `the Amazon Bedrock client cannot be built: <type>: ...` | worker | check the region, the profile and `llm.endpoint_url` |
| `Amazon Bedrock rejected the AWS credentials during <action> (401): check the worker's AWS credentials`, then `; <detail>` when AWS's answer has a message or an error type | worker | renew the credentials or the Bedrock API key |
| `Amazon Bedrock refused <action> (403): <detail>; check the worker's AWS credentials, its permission bedrock:InvokeModel on <model> and the model access (docs/bedrock.md)` | worker | the IAM policy ("The IAM policy"), the model access ("Before you start"), an expired short-term key, a service control policy or the VPC endpoint's policy |
| `Amazon Bedrock returned 404 during <action>: check llm.model (<model>) and the region (<region>)`, then `; <detail>` when AWS's answer has a message or an error type | worker | the ID does not exist in that Region: check the model card. With the detail `Model use case details have not been submitted for this account`, the First Time Use form is missing ("Before you start") |
| `Amazon Bedrock throttled <action> (429); retry later or raise the quota of <model> in <region>` | worker | the SDK already retried twice; ask for a higher quota in Service Quotas, or lower `limits.max_incidents_per_hour` |
| `Amazon Bedrock error 400 during <action>: <detail> (use an inference profile ID in llm.model, such as eu.anthropic.claude-opus-5-5: docs/bedrock.md)` | worker | the bare model ID outside its in-Region Region: use an inference profile ID |
| `Amazon Bedrock error 400 during <action>: <detail> (the model does not accept the API version of the installed anthropic SDK: use Claude Opus 4.7 or later, docs/bedrock.md)` | worker | use Claude Opus 4.7 or a newer model |
| `Amazon Bedrock error 400 during <action>: <detail> (check that llm.model (<model>) is a Bedrock model or inference profile ID available in <region>: docs/bedrock.md)` | worker | a misspelled ID, or one that the Region does not offer |
| `Amazon Bedrock error 400 during <action>: <detail>`, where the detail names the data retention mode | worker | the model needs another data retention mode ("Before you start") |
| `Cannot reach Amazon Bedrock (<host>) during <action>: <type>` | worker | the network path: DNS, the VPC endpoint, `llm.endpoint_url`, the proxy variables |
| `Amazon Bedrock answered <action> with a redirect (HTTP <status>), which the bedrock provider never follows: ...` | worker | the endpoint, the gateway or a proxy answered with a redirect: point `llm.endpoint_url` at the endpoint itself |
| `Claude's classification on Amazon Bedrock was not a JSON object of the expected schema` | worker | rare; if it repeats, try another model |
| `llm.fallbacks is true, but Amazon Bedrock has no server-side refusal fallbacks: ...` | `dag-healer check`, worker (a warning) | set `llm.fallbacks: false` |

## The live test

`tests/live/test_bedrock_live.py` checks the setup against the real Amazon Bedrock of your account:
doctor's `llm` check for bedrock must be OK, then one classification of a small synthetic incident
(a `ValueError` in `src/etl/customers.py`, no real data), with `effort: low` and `max_tokens:
4096`, must return a category and count its tokens. It uses about 2,000 to 4,000 tokens, billed to
the account. It prints doctor's line, then the model, the region, whether structured outputs were
used, the category and the tokens, never a credential.

It runs only when `HEALER_BEDROCK_TEST_MODEL` names a Bedrock model or inference profile ID, and
neither a plain `pytest` run nor CI collects `tests/live/`. Run it from the repository root, with
the region and the credentials of the usual AWS settings (the instance role,
`AWS_PROFILE=<profile>` or `AWS_BEARER_TOKEN_BEDROCK`):

```sh
HEALER_BEDROCK_TEST_MODEL=eu.anthropic.claude-opus-5-5 AWS_REGION=eu-west-1 .venv/bin/python -m pytest -q -s -p no:cacheprovider tests/live/test_bedrock_live.py
```

`HEALER_BEDROCK_TEST_ENDPOINT_URL` sets `llm.endpoint_url` for it. On Windows, set the variables
first (`$env:HEALER_BEDROCK_TEST_MODEL = "eu.anthropic.claude-opus-5-5"` in PowerShell) and run
`.venv/Scripts/python` instead.
