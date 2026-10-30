import { execFile } from "node:child_process";
import { isAbsolute } from "node:path";
import { promisify } from "node:util";

const execute = promisify(execFile);
const failures = {
  1: "Snow CLI OAuth renewal failed. Sign in again with snow connection test and relaunch snow ai.",
  2: "Invalid or unsupported Snow CLI OAuth configuration. Check the setup guide and relaunch.",
  3: "Snow CLI OAuth storage or lock is unavailable. Check local permissions and secure storage.",
  4: "Snow CLI OAuth identity changed. Check the selected user and role, then sign in again.",
  5: "Snow CLI OAuth renewal timed out. Check network and proxy connectivity, then retry.",
  6: "Snow CLI OAuth helper failed unexpectedly. Check CLI and connector versions, then relaunch.",
};

export default async function snowCliOAuth() {
  const helperEnv = Object.freeze({ ...process.env });
  let command, gateway;
  try {
    command = JSON.parse(helperEnv.SNOWFLAKE_AI_OAUTH_COMMAND || "null");
    gateway = new URL(helperEnv.SNOWFLAKE_AI_OAUTH_GATEWAY || "");
  } catch {
    throw new Error(failures[2]);
  }
  if (!Array.isArray(command) || command.length !== 4 ||
      !command.every((part) => typeof part === "string") || !isAbsolute(command[0]) ||
      command[1] !== "-I" || command[2] !== "-m" || command[3] !== "snowflake.cli._plugins.ai.oauth" ||
      gateway.protocol !== "https:" || gateway.username || gateway.password ||
      gateway.search || gateway.hash) {
    throw new Error("Invalid Snow CLI OAuth launch configuration");
  }
  let pending;
  async function credential() {
    if (!pending) {
      pending = execute(command[0], command.slice(1), {
        timeout: 45000,
        killSignal: "SIGKILL",
        maxBuffer: 32768,
        encoding: "utf8",
        windowsHide: true,
        env: helperEnv,
      }).then(({ stdout }) => {
        if (!/^[A-Za-z0-9._~+/-]+=*$/.test(stdout) || stdout.length > 32768) {
          throw new Error("Invalid credential helper output");
        }
        return stdout;
      }).catch((error) => {
        const category = error.killed ? 5 : error.code;
        throw new Error(Number.isInteger(category) && failures[category] || failures[6]);
      }).finally(() => { pending = undefined; });
    }
    return pending;
  }
  return {
    "chat.headers": async (input, output) => {
      if (input.model.providerID !== "snowflake-cortex") return;
      let configured;
      try {
        configured = new URL(input.provider.options.baseURL);
      } catch {
        throw new Error(failures[2]);
      }
      if (configured.href.replace(/\/$/, "") !== gateway.href.replace(/\/$/, "")) {
        throw new Error("Snow CLI OAuth gateway configuration changed");
      }
      for (const name of Object.keys(output.headers)) {
        if (name.toLowerCase() === "authorization") delete output.headers[name];
      }
      output.headers.Authorization = `Bearer ${await credential()}`;
    },
  };
}
