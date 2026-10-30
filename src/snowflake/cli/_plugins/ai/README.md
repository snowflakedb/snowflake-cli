<!--
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# `snow ai` — coding agent launchers

Launches a local coding agent with its model traffic routed through the Snowflake AI
Gateway of the active connection. The agent's configuration files are not modified.
OAuth uses connector-managed credential storage and credential-free coordination
files under `~/.snowflake/ai-oauth-locks`; launch settings are process-local.

Setup: [docs/ai-gateway.md](../../../../../docs/ai-gateway.md). Kept outside `src/` so it
is not packaged into the published wheel.

## Commands

| Command | Agent | Wiring |
| --- | --- | --- |
| `snow ai claude` | Claude Code | Environment variables (`ANTHROPIC_*`, `CLAUDE_CODE_*`, `OTEL_*`) |
| `snow ai opencode` | OpenCode | Inline config via `OPENCODE_CONFIG_CONTENT`, merged with the user's own |

Options, both commands: `--agent-args`, `--gateway-name`, plus the standard `snow`
connection flags. The group is hidden from `snow --help` (`is_hidden`) while the feature
is in preview.

```console
snow ai claude
snow ai opencode
snow ai claude --agent-args -p 'explain this repo' --verbose
snow ai opencode --agent-args -m snowflake-cortex/openai-gpt-5.4
snow ai claude --gateway-name OTHER_GATEWAY
```

## What is implemented

**Gateway URL** built from the connection's account host plus the gateway name, which
defaults to `SNOWFLAKE`. The host is never caller-supplied, so the credential cannot be
addressed anywhere other than the account it belongs to, and a private link connection
resolves to its own endpoint. `--gateway-name` is restricted to Snowflake identifier
characters because it is interpolated into the URL path.

**Credential** derived from the active connection:

| Authenticator | Source |
| --- | --- |
| `PROGRAMMATIC_ACCESS_TOKEN` | `token_file_path` when present, otherwise `token` |
| `oauth` | Caller-supplied static access token from `token_file_path` or `token` |
| `oauth_authorization_code` | Connector-managed cache and renewable helper |
| anything else | refused before launch, pointing at a PAT |

Raw OAuth remains static because Snow CLI receives only the access token. For
authorization-code OAuth, the Python connector owns access/refresh token storage and
renewal. Claude invokes the helper through `apiKeyHelper`; OpenCode uses the bundled
`opencode_oauth.mjs` request hook. Both execute the same connector-backed helper.

Renewal requires connector 4.7.5 or newer, a Python installation on macOS/Linux, a named
connection with explicit user/role, and caching and refresh enabled. Only default
Snowflake-hosted OAuth is supported. Custom clients/scopes, secondary roles and
unsupported transport settings are rejected by name; unrelated session preferences
and unused credentials are ignored. The binding contains only account,
host, user and role. Unquoted roles normalize to uppercase; quoted roles preserve
case. The helper verifies the authenticated role against that identifier.

Connection resolution shares the normal connector's typed flags/config/environment
resolver. Empty or unreadable token files fail without inline-token fallback.
Claude retains native helper caching and any user-specified cache lifetime. OpenCode
loads the bundled plugin by local file URL and uses `chat.headers`, with a snapshot
of its launch environment. There is no HTTP proxy, watchdog or npm publication.

The driver session token is never used: the REST layer behind the gateway rejects it with
an empty-bodied 401, so passing it produced a confusing gateway error instead of a clear
CLI one.

Claude's environment builder keeps the PAT as `SecretType` until the subprocess call;
formatting that mapping masks the token. OpenCode's inline config is likewise wrapped
until assigned to the child environment. Both children still receive plaintext
credentials, so this guards against accidental logging, not child-process access.

**Agent identification** via `X-Snowflake-Application` (hardcoded `snowflake-cli`) and
`snow-agent-name` (`claude` / `opencode`) on every request. The first is the header
`sf-cli` already sends, so the launcher is legible to the same gateway-side tooling. Both
are client-supplied and **unauthenticated** — any caller can send the same values without
going near this CLI, so they are a usage signal and must not back quota, billing, routing
or policy decisions. For Claude Code they are merged into `ANTHROPIC_CUSTOM_HEADERS`
first-write-wins, so a parent process that already set them keeps its values.

**Telemetry:** Claude's existing connection-gated telemetry defaults are unchanged;
explicit variables are preserved and `OTEL_TRACES_EXPORTER` defaults to `console`.
The enhanced-telemetry payload still needs review before broader deployment.
OpenCode no longer implicitly loads an unpinned external telemetry package, so the
launcher does not supply OpenCode trace propagation. Users retain control of their
own plugins and exporter settings. The local OAuth plugin only supplies credentials.

**Conflicting environment refused.** Variables the agent reads in preference to what this
command passes — `ANTHROPIC_API_KEY`, `CLAUDE_CODE_USE_BEDROCK`, `CLAUDE_CODE_USE_VERTEX`
for Claude; `SNOWFLAKE_CORTEX_TOKEN`, `SNOWFLAKE_CORTEX_PAT` for OpenCode — cause a
refusal to launch. Authenticating with a credential other than the one whose
authenticator was just validated is the worst available outcome. `SNOWFLAKE_ACCOUNT` is
deliberately not on that list: it is a supported `snow` connection variable, and it can
change neither the credential nor the endpoint.

**Argument forwarding** after `--agent-args`, which is a terminator: the tail is sliced
off argv before click parses, and the root config/debug pre-scan stops there. `snow` and the agents share around
forty flag names (mostly `snow`'s global connection options), and anything reaching
`snow`'s parser risks being claimed by it.

## Model identifiers

The gateway takes **bare** model names — no vendor prefix, no namespace. `claude-sonnet-4-5`
is accepted; `anthropic-claude-sonnet-4-5` and `anthropic/claude-sonnet-4-5` are both
`400 unknown model`. This is not the `AI_COMPLETE` model list: `llama3.1-70b`,
`mistral-large2` and `deepseek-r1` are all unknown to the gateway.

| Family | Form | Examples |
| --- | --- | --- |
| Claude | `claude-<tier>-<major>-<minor>` | `claude-sonnet-4-5`, `claude-opus-4-5`, `claude-haiku-4-5` |
| OpenAI | `openai-<model>`, or bare | `openai-gpt-5.4`, `gpt-5.4`, `openai-gpt-4.1` |

**The family has to match the protocol path.** Claude models answer on Anthropic's
`/v1/messages` but return 503 on the OpenAI-style `/chat/completions`, so they work for
`snow ai claude` and not for `snow ai opencode`. GPT models are the inverse.

What the user types is not always what goes on the wire. Claude Code passes its `--model`
through verbatim, so it takes a bare name. OpenCode addresses models as
`<providerID>/<modelID>` and the AI SDK strips the provider half before the request, so
there the same model is `snowflake-cortex/openai-gpt-5.4` — which is why the default is
written that way in the generated config while `openai-gpt-5.4` is what the gateway sees.

There is no discovery endpoint: `/v1/models` and every variant return 400, so availability
cannot be listed and varies by account and deployment.

## Design notes worth knowing before changing this

- **The two SDKs disagree on where the API version belongs.** Anthropic's client appends
  `/v1/messages` to the base URL it is given; the Vercel AI SDK appends only
  `/chat/completions`, so it needs `/v1` in the base URL. Each command applies its own
  convention — do not unify them.
- **OpenCode drives the `snowflake-cortex` provider, not `anthropic`.** `@ai-sdk/anthropic`
  sends `x-api-key` and no `Authorization` header, which the gateway rejects with 401.
  `snowflake-cortex` is `@ai-sdk/openai-compatible` and sends Bearer. Its `account` option
  is load-bearing: the provider assembles its options, including the compat fetch wrapper
  that rewrites `max_tokens` to `max_completion_tokens` and repairs `"role":""` in streamed
  deltas, only past a guard that returns early when no account is resolved. Omit it and the
  request goes out with a raw `max_tokens`, which the gateway rejects. The value is only
  used to interpolate a default `baseURL` that `options.baseURL` overrides, so the gateway
  host is passed rather than a Snowflake account identifier. The provider reads
  `SNOWFLAKE_CORTEX_TOKEN`/`_PAT` from the environment in preference to this config,
  which is why the command refuses to launch when they are set. `SNOWFLAKE_ACCOUNT` also
  takes precedence, but only over the default `baseURL` we override anyway, so it is not
  refused.
- **`no_args_is_help=False` is deliberate** on both commands. The terminator can leave
  click an empty argument list, and click prints help rather than launching in that case.
- **`small_model` is pinned** alongside `model` for OpenCode, so overriding the main model
  natively cannot break thread-title requests against a model the gateway does not serve.
- **The credential's destination is not caller-supplied.** There is deliberately no
  `--gateway-url`: the host comes from the connection and only the gateway *name* is
  selectable, so no flag combination can send a live credential to an unrelated host.

## Known limitations

- Renewable OAuth currently supports only named, default-client Snowflake-hosted
  authorization-code connections with explicit user/role and connector 4.7.5 or newer on a
  Python installation for macOS or Linux. Custom clients/endpoints/scopes, secondary
  roles, unsupported transport options, Windows and frozen executables are
  rejected before agent launch.
- Connector-managed access and refresh tokens remain in the operating-system cache.
  Renewal creates owner-only lock files under `~/.snowflake/ai-oauth-locks`; agent
  configuration files are not modified.
- Claude models are not served to OpenCode through the gateway (503), which is why the
  default model is `openai-gpt-5.4` and why the command warns before launching. The
  warning is unconditional: deciding whether it applies would mean parsing the forwarded
  tail, which is what the terminator exists to avoid.
- No model discovery, so an unavailable model surfaces as a gateway error at first use
  rather than as a CLI validation failure.

## Tests

`tests/ai/` covers the launcher, typed resolution, helper restrictions, synthetic
refresh, identifier semantics, sanitized errors and telemetry exit classification.
POSIX tests skip Windows; explicit Windows rejection is tested.
`node --test tests/ai/test_opencode_oauth.mjs` runs offline plugin tests. CI checks
the installed wheel with `scripts/check_ai_wheel.py`. Help output is snapshotted in
`tests/__snapshots__/test_help_messages.ambr` and must be regenerated with
`pytest tests/test_help_messages.py --snapshot-update` when options or help text change.

Launcher tests mock the connection. `test_oauth_authentication_flow.py` instead runs
the real connector login and renewal sequence, faking only HTTP and credential storage.
It covers missing access tokens, 390318/390303 rejection, rotation, overlapping helpers,
terminal failures and identity checks against the pinned connector. Neither suite
proves live agent request-header behavior. Anything touching credential
or URL resolution needs a live run: point a PAT connection at a gateway and check both
`/v1/messages` and `/v1/chat/completions` return 200, and that forwarding survives a
colliding flag such as `--agent-args serve --port 3000`.
