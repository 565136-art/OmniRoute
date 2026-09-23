import assert from "node:assert/strict";
import { accessSync, constants } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

import { MockUpstreamServer, buildCompletion } from "../../tests/e2e/helpers/mockUpstreamServer.ts";
import { HermesLocalAdapter } from "../../scripts/supervisor/adapters/hermesLocalAdapter.mjs";
import { ResourceLockManager } from "../../scripts/supervisor/resourceLocks.mjs";
import { SupervisorScheduler } from "../../scripts/supervisor/scheduler.mjs";

const REPO_ROOT = path.resolve(import.meta.dirname, "../..");
function resolveHermesExecutable() {
  if (process.env.HERMES_BIN) return process.env.HERMES_BIN;
  for (const directory of (process.env.PATH ?? "").split(path.delimiter)) {
    const candidate = path.join(directory, "hermes");
    try {
      accessSync(candidate, constants.X_OK);
      return candidate;
    } catch {}
  }
  throw new Error("Hermes CLI not found; set HERMES_BIN to its executable path");
}

const HERMES_BIN = resolveHermesExecutable();
const TEST_API_KEY = "local-identity-e2e-key";
const TASK_ID = "identity-e2e-task-stable";
const MISSION_ID = "identity-e2e-mission";
const TASK_DESCRIPTION = "Return the fixed local identity test response.";

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close((error) => (error ? reject(error) : resolve(address.port)));
    });
  });
}

function waitForExit(child, timeoutMs = 15_000) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve();
  return Promise.race([
    new Promise((resolve) => child.once("exit", resolve)),
    delay(timeoutMs).then(() => {
      child.kill("SIGKILL");
      throw new Error("Local OmniRoute test server did not stop");
    }),
  ]);
}

function sanitizedEnv(overrides) {
  const env = { ...process.env, ...overrides };
  for (const name of Object.keys(env)) {
    if (
      /(?:API_KEY|ACCESS_TOKEN|SECRET|PASSWORD|BEARER)/i.test(name) &&
      name !== "API_KEY_SECRET"
    ) {
      delete env[name];
    }
  }
  return env;
}

test(
  "Supervisor Hermes subprocess persists V2 correlation through local OmniRoute without inference",
  {
    timeout: 240_000,
  },
  async (t) => {
    const root = await mkdtemp(path.join(os.tmpdir(), "omniroute-identity-e2e-"));
    const dataDir = path.join(root, "data");
    const hermesHome = path.join(root, "hermes-home");
    const supervisorRoot = path.join(root, "supervisor");
    const mock = new MockUpstreamServer();
    const upstreamUrl = await mock.start();
    const port = await freePort();
    const omnirouteUrl = `http://127.0.0.1:${port}`;
    const mockResponse = JSON.stringify({
      status: "COMPLETE",
      task: TASK_DESCRIPTION,
      evidence: [{ type: "other", source: "fixed-local-mock", summary: "LOCAL_IDENTITY_E2E_OK" }],
      changes: [],
      tests: ["NOT_RUN"],
      blocker: null,
      next_action: "none",
      requires_approval: false,
    });
    mock.configureToken(TEST_API_KEY, {
      defaultResponse: buildCompletion(mockResponse, { model: "fixed-local-mock" }),
    });

    const originalDataDir = process.env.DATA_DIR;
    process.env.DATA_DIR = dataDir;
    process.env.REQUIRE_API_KEY = "false";
    process.env.API_KEY_SECRET = "local-e2e-only-api-key-secret";
    process.env.OMNIROUTE_ALLOW_PRIVATE_PROVIDER_URLS = "true";

    let serverProcess;
    let logs = "";
    let hermesStderr = "";
    try {
      await mkdir(dataDir, { recursive: true });
      await mkdir(hermesHome, { recursive: true, mode: 0o700 });

      const core = await import("../../src/lib/db/core.ts");
      const providerDb = await import("../../src/lib/db/providers.ts");
      await providerDb.createProviderNode({
        id: "openai-compatible-identity-e2e",
        type: "openai-compatible",
        name: "Local identity E2E mock",
        prefix: "identity-e2e",
        apiType: "chat",
        baseUrl: upstreamUrl,
      });
      await providerDb.createProviderConnection({
        provider: "openai-compatible-identity-e2e",
        authType: "apikey",
        name: "local-identity-e2e",
        apiKey: TEST_API_KEY,
        isActive: true,
        testStatus: "active",
        providerSpecificData: { baseUrl: upstreamUrl, apiType: "chat" },
      });
      core.resetDbInstance();

      serverProcess = spawn(process.execPath, [".build/next/standalone/server.js"], {
        cwd: REPO_ROOT,
        env: sanitizedEnv({
          DATA_DIR: dataDir,
          PORT: String(port),
          API_PORT: String(port),
          DASHBOARD_PORT: String(port),
          HOST: "127.0.0.1",
          REQUIRE_API_KEY: "false",
          API_KEY_SECRET: "local-e2e-only-api-key-secret",
          DISABLE_SQLITE_AUTO_BACKUP: "true",
          NEXT_TELEMETRY_DISABLED: "1",
          OMNIROUTE_ALLOW_PRIVATE_PROVIDER_URLS: "true",
          OMNIROUTE_DISABLE_BACKGROUND_SERVICES: "true",
          OMNIROUTE_DISABLE_TOKEN_HEALTHCHECK: "true",
          OMNIROUTE_DISABLE_LOCAL_HEALTHCHECK: "true",
          OMNIROUTE_HIDE_HEALTHCHECK_LOGS: "true",
          OMNIROUTE_E2E_BOOTSTRAP_MODE: "open",
          OMNIROUTE_USE_TURBOPACK: "0",
        }),
        stdio: ["ignore", "pipe", "pipe"],
      });
      serverProcess.stdout.on("data", (chunk) => {
        logs += chunk.toString();
      });
      serverProcess.stderr.on("data", (chunk) => {
        logs += chunk.toString();
      });

      const startedAt = Date.now();
      while (Date.now() - startedAt < 120_000) {
        if (serverProcess.exitCode !== null) throw new Error(`OmniRoute exited early:\n${logs}`);
        try {
          const response = await fetch(`${omnirouteUrl}/api/monitoring/health`, {
            signal: AbortSignal.timeout(2_000),
          });
          if (response.ok) break;
        } catch {}
        await delay(500);
      }
      assert.ok(Date.now() - startedAt < 120_000, `OmniRoute did not become healthy:\n${logs}`);

      const headerNames = [
        "X-OmniRoute-Contract-Version",
        "X-OmniRoute-Mission-ID",
        "X-OmniRoute-Task-ID",
        "X-OmniRoute-Attempt-ID",
        "X-OmniRoute-Request-ID",
        "X-OmniRoute-Correlation-ID",
      ];
      const environmentNames = [
        "JARVIS_OMNIROUTE_CONTRACT_VERSION",
        "JARVIS_OMNIROUTE_MISSION_ID",
        "JARVIS_OMNIROUTE_TASK_ID",
        "JARVIS_OMNIROUTE_ATTEMPT_ID",
        "JARVIS_OMNIROUTE_REQUEST_ID",
        "JARVIS_OMNIROUTE_CORRELATION_ID",
      ];
      const headerYaml = headerNames
        .map((name, index) => `    ${name}: "${"${"}${environmentNames[index]}}"`)
        .join("\n");
      const config = [
        "model:",
        "  provider: custom",
        "  default: identity-e2e/mock-model",
        `  base_url: ${JSON.stringify(`${omnirouteUrl}/api/v1`)}`,
        "  api_key: local-only-test-key",
        "  api_mode: chat_completions",
        "  extra_headers:",
        headerYaml,
        "custom_providers:",
        "  - name: identity-e2e",
        `    base_url: ${JSON.stringify(`${omnirouteUrl}/api/v1`)}`,
        "    api_key: local-only-test-key",
        "    api_mode: chat_completions",
        "    extra_headers:",
        headerYaml,
        "    models:",
        "      identity-e2e/mock-model: {}",
        "agent:",
        "  max_turns: 1",
        "  stream: false",
        "security:",
        "  tirith_enabled: false",
        "fallback_providers: []",
        "",
      ].join("\n");
      await writeFile(path.join(hermesHome, "config.yaml"), config, { mode: 0o600 });

      const { SupervisorScheduler } = await import("../../scripts/supervisor/scheduler.mjs");
      const { ResourceLockManager } = await import("../../scripts/supervisor/resourceLocks.mjs");
      const scheduler = new SupervisorScheduler({
        root: supervisorRoot,
        missionId: MISSION_ID,
        lockManager: new ResourceLockManager(supervisorRoot),
        adapterFactory: () => {
          const adapter = new HermesLocalAdapter({
            executable: HERMES_BIN,
            profile: "normal",
            timeoutMs: 90_000,
            args: [],
          });
          const waitForResult = adapter.wait_for_result.bind(adapter);
          adapter.wait_for_result = async (handle) => {
            try {
              return await waitForResult(handle);
            } catch (error) {
              const record = adapter.tasks.get(handle);
              hermesStderr = [
                error instanceof Error ? error.message : String(error),
                record?.error ?? "",
                (record?.output ?? "").slice(0, 1_000),
              ]
                .filter(Boolean)
                .join("\n");
              throw error;
            }
          };
          return adapter;
        },
      });
      await scheduler.initialize({
        goal: "Prove local identity propagation",
        tasks: [
          {
            task_id: TASK_ID,
            description: TASK_DESCRIPTION,
            task_type: "identity-e2e",
            depends_on: [],
            resources: [],
            can_parallelize: false,
            acceptance_criteria: [
              {
                id: "fixed-response",
                description: "The fixed local response is recorded",
                required_evidence: ["fixed-local-mock"],
                expected_evidence: [],
                status: "pending",
              },
            ],
            verifiers: [],
            max_attempts: 1,
            requires_approval: false,
            mutating: false,
          },
        ],
      });

      const priorHermesHome = process.env.HERMES_HOME;
      process.env.HERMES_HOME = hermesHome;
      const removedProviderEnv = new Map();
      for (const [name, value] of Object.entries(process.env)) {
        if (
          /(?:API_KEY|ACCESS_TOKEN|SECRET|PASSWORD|BEARER)/i.test(name) &&
          !["API_KEY_SECRET"].includes(name) &&
          !name.startsWith("JARVIS_OMNIROUTE_")
        ) {
          removedProviderEnv.set(name, value);
          delete process.env[name];
        }
      }
      try {
        const finalState = await scheduler.run();
        const lane = finalState.lanes[TASK_ID];
        assert.equal(lane.shadow_task_id, TASK_ID);
        assert.match(lane.attempt_id, /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/);
        assert.equal(lane.request_id, lane.correlation_id);
        assert.notEqual(lane.correlation_id, lane.attempt_id);
        assert.equal(
          lane.status,
          "COMPLETE",
          `${JSON.stringify({ blocker: lane.blocker, attempt_count: lane.attempt_count, execution_retry_count: lane.execution_retry_count, result: lane.result })}\n${hermesStderr}\n${logs}`
        );
        assert.equal(
          mock.getState(TEST_API_KEY).hits >= 1,
          true,
          "the local fixed mock must be called"
        );

        const dbPath = path.join(dataDir, "storage.sqlite");
        const query = [
          "import json, sqlite3, sys",
          "db = sqlite3.connect(sys.argv[1])",
          "row = db.execute('SELECT correlation_id FROM call_logs ORDER BY timestamp DESC LIMIT 1').fetchone()",
          "print(json.dumps({'correlation_id': row[0] if row else None}))",
        ].join("\n");
        const { execFileSync } = await import("node:child_process");
        const persisted = JSON.parse(
          execFileSync("python3", ["-c", query, dbPath], { encoding: "utf8" })
        );
        assert.equal(persisted.correlation_id, lane.correlation_id);
        assert.notEqual(persisted.correlation_id, "");
        assert.ok(
          mock.getState(TEST_API_KEY).hits <= 2,
          "only the main request and optional local compression are allowed"
        );

        t.diagnostic(
          JSON.stringify({
            hermesHome,
            endpoint: omnirouteUrl,
            database: dbPath,
            task_id: lane.shadow_task_id,
            attempt_id: lane.attempt_id,
            request_id: lane.request_id,
            correlation_id: persisted.correlation_id,
            mock_upstream_calls: mock.getState(TEST_API_KEY).hits,
          })
        );
      } finally {
        if (priorHermesHome === undefined) delete process.env.HERMES_HOME;
        else process.env.HERMES_HOME = priorHermesHome;
        for (const [name, value] of removedProviderEnv) process.env[name] = value;
      }
    } catch (error) {
      throw new Error(`${error instanceof Error ? error.message : String(error)}\n${logs}`);
    } finally {
      if (serverProcess && serverProcess.exitCode === null) {
        serverProcess.kill("SIGTERM");
        await waitForExit(serverProcess);
      }
      await mock.stop();
      if (originalDataDir === undefined) delete process.env.DATA_DIR;
      else process.env.DATA_DIR = originalDataDir;
      await rm(root, { recursive: true, force: true });
    }
  }
);
