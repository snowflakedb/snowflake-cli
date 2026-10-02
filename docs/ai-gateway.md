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

# `snow ai` — setup

Launches a local coding agent with its model traffic routed through the Snowflake AI
Gateway of the active connection. The agent's own configuration files are never
modified: launch configuration is passed to the child process. Connector-managed OAuth
credentials remain in the operating-system cache, and renewable OAuth creates
owner-only coordination files under `~/.snowflake/ai-oauth-locks`.

| Command | Agent | Wiring |
| --- | --- | --- |
| `snow ai claude` | Claude Code | Environment variables (`ANTHROPIC_*`, `CLAUDE_CODE_*`, `OTEL_*`) |
| `snow ai opencode` | OpenCode | Inline config via `OPENCODE_CONFIG_CONTENT` |

The command group is hidden from `snow --help` while the feature is in preview. Run
`snow ai --help` directly.

## 1. Install an agent

```bash
# Claude Code
curl -fsSL https://claude.ai/install.sh | bash

# OpenCode
curl -fsSL https://opencode.ai/install | bash
```

## 2. Choose authentication

Supported connections are `PROGRAMMATIC_ACCESS_TOKEN`, `oauth` (a supplied access
token), and `oauth_authorization_code` (browser login with credential caching).
Other authenticators are refused before the agent starts.

### OAuth

For browser-based login, use your account and a role with gateway access:

```toml
[ai-gateway-oauth]
account = "<org-account>"
user = "<your_user>"
role = "<gateway_role>"
authenticator = "oauth_authorization_code"
client_store_temporary_credential = true
oauth_enable_refresh_tokens = true
```

Run `snow ai claude -c ai-gateway-oauth` or `snow ai opencode -c ai-gateway-oauth`.
The connector performs initial login. For authorization-code OAuth, Claude calls
a process-only `apiKeyHelper`; OpenCode loads a JavaScript plugin bundled inside
Snow CLI through `OPENCODE_CONFIG_CONTENT`. No npm publication or persistent agent
configuration is needed. Both call the same connector-backed helper for access
tokens. The helper authenticates a fresh connection and captures its accepted
access token; refresh tokens stay in connector storage.

For an access token supplied by another application, use `authenticator = "oauth"`
and `token` or `token_file_path`. That application remains responsible for renewal.
If both are set, `token_file_path` wins, matching connector precedence. An unreadable
or empty token file fails rather than falling back to the inline token.

**Raw OAuth is static.** For `authenticator = "oauth"`, the token is copied at launch
and the supplying application must renew it before relaunching the agent.

**Renewable authorization-code OAuth is currently restricted.** It requires a
named connection, an explicit role and user, caching and refresh enabled, the
default Snowflake-hosted OAuth client, connector 4.7.5, and a Python installation
of Snow CLI on macOS or Linux. Custom OAuth clients/scopes, external identity
providers, secondary roles, temporary connections, Windows, and standalone frozen
executables are rejected. The helper uses private connector interfaces and is
version-gated deliberately: only 4.7.5 is validated, not an open-ended minimum.
Connector upgrades require rerunning the helper's authentication-flow and launch
wiring tests before changing the gate.

Connection flags, named configuration, generic environment fallbacks and string
boolean coercion use the same resolver as normal CLI connections. Unquoted role
identifiers are uppercased; quoted roles retain their case and escaped quotes.
The returned login role must match the resolved identifier exactly.

The helper's transport contract is deliberately narrow: default HTTPS on port 443,
default connector TLS/proxy/client behavior, and the selected account host. Explicit
transport options (including `port`, `protocol`, proxy settings, TLS/OCSP controls,
network/socket timeouts, and custom redirect URI) are rejected by name before agent
launch, even when set to their usual defaults. Query tags, session preferences,
application labels, initial login timeout, and credentials unused by OAuth do not
affect renewal and are not forwarded. The helper uses its own bounded timeouts.
Database, schema and warehouse selections are also unnecessary for renewal. The binding
contains only account, host, user and role, never proxy passwords or other secrets.
Inherited environment behavior is not overridden. PrivateLink hostnames are retained,
but configurations requiring extra transport settings are outside this contract.

Each helper call validates the token through a fresh connector login. This adds
connection latency; it is not a cached, zero-round-trip credential lookup. The
helper is terminated after 40 seconds, never opens a browser, and reports a
sanitized error instead of returning stale credentials. Exit categories distinguish
reauthentication (1), configuration/version/platform (2), storage/locks (3), identity
mismatch (4), timeout (5), and unexpected failure (6). Neither Python nor the bundled
JavaScript plugin exposes exception details or helper stderr. Initial browser
login must be completed in the launcher. The binding snapshots account, host,
user and role, so changing a default connection cannot redirect renewal.

Claude uses its native helper caching; the launcher does not override
`CLAUDE_CODE_API_KEY_HELPER_TTL_MS`. An explicitly inherited setting is preserved.
This avoids forcing a full Snowflake login for every model request, but each actual
helper invocation still opens and closes a connection. The process-local settings pin
`env.ANTHROPIC_BASE_URL` to the selected gateway so a saved Claude settings value
cannot override the inherited endpoint. Other settings, including hooks, remain
unchanged. Native `--settings` and `--safe-mode` are rejected
for this flow rather than silently masking the helper. OpenCode uses `chat.headers`
and a noncredential placeholder in `apiKey`; `--pure` and `--attach` are rejected.
An SDK retry of an already-prepared request can reuse its header; this adapter does
not implement forced refresh/replay on every HTTP 401. Managed settings and other
plugins can affect behavior and remain part of the local trust boundary. In particular,
Claude managed settings take precedence over inline `--settings`.

**Managed-settings restriction:** renewable Claude wiring is unsupported when
company-managed settings (MDM) override `apiKeyHelper` or `env.ANTHROPIC_BASE_URL`.
Those settings can replace the helper or gateway configured by `snow ai`. Ask your
administrator to remove the conflict before using this flow; the launcher does not
bypass managed policy. MDM-managed machines without these conflicts are not excluded.

Claude can send helper credentials in both `Authorization` and `X-Api-Key` headers.
Both must be treated as secrets in request capture and logging. Gateway header
precedence and redaction require deployment-specific verification before rollout;
the launcher does not guarantee server-side logging behavior.

The access token remains a live account credential, visible to the agent,
not a gateway-only capability. Use a least-privilege role. The
connector cache is keyed by user and issuer, not by connection name or role; do
not treat named connections sharing that cache as credential isolation boundaries.
Renewals from our helpers are serialized per user/issuer, but unrelated connector
processes do not participate in that lock. Refresh-token rotation across different
applications is not guaranteed safe by this launcher. Same-user software can invoke
the helper; this is credential lifecycle management, not a sandbox.
OpenCode snapshots the helper command, gateway and child environment when the plugin
initializes, preventing later environment changes from redirecting those helper
calls. This does not isolate credentials from malicious code running as the same
OS user. Binary provenance and same-user credential access remain security-review
requirements; switching from PAT to OAuth does not resolve them.

### Programmatic access token

**Use a minimum-privilege role and a network policy.** The token is placed in the
environment of an autonomous agent that has shell, filesystem and network access, and
every process it spawns inherits it. A PAT authenticates against the account's REST
surface with the role it carries, so scope that role to the least the agent needs, and
bind the token to a network policy so a leaked credential is not usable from anywhere.

```sql
-- A role for gateway access only, not your day-to-day role.
CREATE ROLE IF NOT EXISTS ai_gateway_user;
GRANT USAGE ON AI GATEWAY SNOWFLAKE TO ROLE ai_gateway_user;
GRANT ROLE ai_gateway_user TO USER <your_user>;

-- Require the token to come from a known network. Creating the policy is not
-- enough on its own: a PAT is constrained by the policy set on the user.
CREATE NETWORK POLICY IF NOT EXISTS ai_gateway_np
  ALLOWED_IP_LIST = ('<your.ip.or.cidr>');
ALTER USER <your_user> SET NETWORK_POLICY = 'ai_gateway_np';

ALTER USER <your_user> ADD PROGRAMMATIC ACCESS TOKEN ai_gateway
  ROLE_RESTRICTION = 'ai_gateway_user'
  DAYS_TO_EXPIRY = 7;
```

The token secret is returned once, when the statement runs. Copy it straight into the
connection below; it cannot be retrieved afterwards.

Treat the token as a live account credential: do not paste it into a terminal, a ticket
or a shared log.

## 3. Configure a PAT connection

```toml
# ~/.snowflake/connections.toml
[ai-gateway]
account = "<org-account>"
user = "<your_user>"
host = "<org-account>.snowflakecomputing.com"
authenticator = "PROGRAMMATIC_ACCESS_TOKEN"
token = "<pat>"          # or token_file_path = "/path/to/pat"
warehouse = "<warehouse>"
role = "ai_gateway_user"
```

Confirm it works before launching an agent. `snow ai` fails the same way if this does:

```bash
snow connection test -c ai-gateway
```

## 4. Launch

```bash
snow ai claude -c ai-gateway
snow ai opencode -c ai-gateway
```

Arguments for the agent go after `--agent-args`, which forwards everything following it
verbatim. `snow` and the agents share many flag names, so this is what keeps them from
colliding:

```bash
snow ai claude -c ai-gateway --agent-args -p 'explain this repo' --verbose
snow ai opencode -c ai-gateway --agent-args -m snowflake-cortex/openai-gpt-5.4
snow ai opencode -c ai-gateway --agent-args serve --port 3000
```

If you forget the terminator, `snow` rejects the unknown flag and names the fix rather
than silently consuming it.

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `--agent-args` | — | Everything after it is forwarded to the agent verbatim |
| `--gateway-name` | `SNOWFLAKE` | Which gateway to route through |

The gateway address is built from the authenticated connection host, not from an
independent URL flag. A private link connection therefore reaches its own endpoint with
no extra configuration. Connection configuration and same-user process state remain in
the local trust boundary. `--gateway-name` accepts only Snowflake identifier characters.

## Model identifiers

The gateway takes bare model names — no vendor prefix, no namespace, and not the
`AI_COMPLETE` model list.

| Family | Form | Examples |
| --- | --- | --- |
| Claude | `claude-<tier>-<major>-<minor>` | `claude-sonnet-4-5`, `claude-opus-4-5` |
| OpenAI | `openai-<model>`, or bare | `openai-gpt-5.4`, `gpt-5.4`, `openai-gpt-4.1` |

The family has to match the protocol path. Claude models answer on Anthropic's
`/v1/messages`, so they work for `snow ai claude`; they return 503 on the OpenAI-style
`/chat/completions` that OpenCode uses. OpenCode addresses models as
`<providerID>/<modelID>`, so there the same model is written
`snowflake-cortex/openai-gpt-5.4`.

There is no discovery endpoint, so availability varies by account and deployment.

For OpenCode's default `openai-gpt-5.4` model, the launcher sets the model option
`reasoningEffort` to `none`: the chat-completions endpoint rejects function tools
with reasoning enabled. A prompt saying "do not use tools" does not remove tool
definitions. This default does not change other models; explicit agent options or
model variants can override it and reintroduce the incompatible combination.

## Telemetry

`snow ai claude` asks Claude Code to emit telemetry, but only when telemetry is enabled
for the connection — the same switch that governs `snow`'s own. Variables you set
yourself are never overridden.

Claude Code needs a trace exporter configured for traceparent propagation. If
`OTEL_TRACES_EXPORTER` is unset, the launcher supplies `console`. If it is set to
`otlp`, `console`, `none`, or any other value, that value is preserved. Explicitly
disabling tracing can also disable propagation. The launcher does not configure an
OTLP endpoint, protocol, or export credentials.
The console default does not deliver client spans to the gateway. Configuring OTLP
with a static bearer does not make exporter authentication renewable; that requires
a separate exporter/collector authentication design.

OpenCode no longer receives an implicit unpinned third-party telemetry plugin.
The launcher does not supply OpenCode trace propagation by default. User-configured
plugins and exporter variables remain unchanged; review and pin any telemetry plugin
you choose to install, and use a collector trusted for its payload. The bundled OAuth
plugin only supplies authorization headers, not telemetry. Claude's existing telemetry
defaults, including enhanced telemetry, are unchanged by this renewal work; their
payload still requires review before broader deployment.

## Known limitations

- **Raw OAuth is static; authorization-code OAuth uses the helper.** See the
  platform, connector-version and configuration restrictions above.
- **Authorization-code OAuth requires credential caching** and private connector
  interfaces. Disabled, unavailable, or unreadable caches prevent launch. External
  identity providers and concurrent logins sharing an issuer/user cache need separate
  validation; cache entries are not isolated by role, client ID, or connection name.
- **Key pair, OAuth client credentials and workload identity are not supported.**
- **Claude models are not served to OpenCode** (503), so the default model is
  `openai-gpt-5.4` and the command warns before launching.
- **No model discovery**, so an unavailable model surfaces as a gateway error at first
  use rather than as a CLI validation failure.
- **The agent binary is whatever is first on `PATH`.** It receives a live credential in
  its environment; nothing verifies its version, signature or provenance.
