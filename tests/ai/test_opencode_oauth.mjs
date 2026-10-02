import assert from "node:assert/strict";
import test from "node:test";
import { fileURLToPath } from "node:url";
import plugin from "../../src/snowflake/cli/_plugins/ai/opencode_oauth.mjs";

const gateway = "https://test.snowflakecomputing.com/api/v2/aigateways/SNOWFLAKE/v1";
const input = {
  model: { providerID: "snowflake-cortex" },
  provider: { options: { baseURL: gateway } },
};

function configure() {
  process.env.SNOWFLAKE_AI_OAUTH_COMMAND = JSON.stringify([
    fileURLToPath(new URL("./oauth-helper-fixture", import.meta.url)),
    "-I", "-m", "snowflake.cli._plugins.ai.oauth",
  ]);
  process.env.SNOWFLAKE_AI_OAUTH_GATEWAY = gateway;
  delete process.env.OAUTH_FIXTURE_FAIL;
  delete process.env.OAUTH_FIXTURE_EXIT;
}

test("renews for successive requests and coalesces concurrent calls", { skip: process.platform === "win32" }, async () => {
  configure();
  const hooks = await plugin();
  const first = { headers: { authorization: "old", "snow-agent-name": "opencode" } };
  const concurrent = { headers: {} };
  await Promise.all([hooks["chat.headers"](input, first), hooks["chat.headers"](input, concurrent)]);
  assert.equal(first.headers.Authorization, concurrent.headers.Authorization);
  assert.match(first.headers.Authorization, /^Bearer fixture-token-/);
  assert.equal(first.headers.authorization, undefined);
  assert.equal(first.headers["snow-agent-name"], "opencode");
  const second = { headers: {} };
  await hooks["chat.headers"](input, second);
  assert.notEqual(second.headers.Authorization, first.headers.Authorization);
});

test("rejects destination drift before asking for a credential", async () => {
  configure();
  const hooks = await plugin();
  await assert.rejects(hooks["chat.headers"]({ ...input, provider: { options: { baseURL: "https://other.example" } } }, { headers: {} }), /configuration changed/);
});

test("snapshots helper identity and command before environment mutation", { skip: process.platform === "win32" }, async () => {
  configure();
  const previous = { ...process.env };
  try {
    process.env.OAUTH_FIXTURE_CHECK_BINDING = "1";
    process.env.SNOWFLAKE_AI_OAUTH_BINDING = "original-binding";
    const hooks = await plugin();
    process.env.SNOWFLAKE_AI_OAUTH_BINDING = "other-account";
    process.env.SNOWFLAKE_AI_OAUTH_COMMAND = "[]";
    process.env.SNOWFLAKE_AI_OAUTH_GATEWAY = "https://other.example";
    process.env.OAUTH_FIXTURE_FAIL = "1";
    const output = { headers: {} };
    await hooks["chat.headers"](input, output);
    assert.match(output.headers.Authorization, /^Bearer fixture-token-/);
  } finally {
    for (const key of Object.keys(process.env)) {
      if (!(key in previous)) delete process.env[key];
    }
    Object.assign(process.env, previous);
  }
});

test("ignores other providers", async () => {
  configure();
  const hooks = await plugin();
  const output = { headers: {} };
  await hooks["chat.headers"]({ model: { providerID: "other" } }, output);
  assert.deepEqual(output.headers, {});
});

test("sanitizes subprocess errors without stale-token fallback", { skip: process.platform === "win32" }, async () => {
  configure();
  process.env.OAUTH_FIXTURE_FAIL = "1";
  const hooks = await plugin();
  const output = { headers: {} };
  await assert.rejects(hooks["chat.headers"](input, output), (error) => {
    assert.equal(String(error).includes("fixture-sensitive-error"), false);
    return /renewal failed/.test(String(error));
  });
  assert.deepEqual(output.headers, {});
  delete process.env.OAUTH_FIXTURE_FAIL;
});

test("sanitizes malformed launch JSON and URLs", async () => {
  configure();
  process.env.SNOWFLAKE_AI_OAUTH_COMMAND = "invalid-synthetic-sensitive-value";
  await assert.rejects(plugin(), /unsupported Snow CLI OAuth configuration/);
  configure();
  process.env.SNOWFLAKE_AI_OAUTH_GATEWAY = "invalid-synthetic-sensitive-value";
  await assert.rejects(plugin(), /unsupported Snow CLI OAuth configuration/);
});

test("sanitizes malformed request destination", async () => {
  configure();
  const hooks = await plugin();
  await assert.rejects(hooks["chat.headers"]({ ...input, provider: { options: { baseURL: "invalid-synthetic-sensitive-value" } } }, { headers: {} }), /unsupported Snow CLI OAuth configuration/);
});

test("maps helper failure categories without exposing stderr", { skip: process.platform === "win32" }, async () => {
  configure();
  process.env.OAUTH_FIXTURE_EXIT = "3";
  const hooks = await plugin();
  await assert.rejects(hooks["chat.headers"](input, { headers: {} }), (error) => {
    assert.match(String(error), /storage or lock is unavailable/);
    assert.equal(String(error).includes("fixture-sensitive-error"), false);
    return true;
  });
  delete process.env.OAUTH_FIXTURE_EXIT;
});
