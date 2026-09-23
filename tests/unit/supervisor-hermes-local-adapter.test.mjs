import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  HermesLocalAdapter,
  normalizeHermesTransportResult,
} from "../../scripts/supervisor/adapters/hermesLocalAdapter.mjs";
import { SupervisorEngine } from "../../scripts/supervisor/engine.mjs";
import { decideResult, evaluateAcceptance } from "../../scripts/supervisor/evaluator.mjs";
import { parseHermesResult } from "../../scripts/supervisor/resultContract.mjs";
import { createInitialState } from "../../scripts/supervisor/schema.mjs";
import { MissionStateStore } from "../../scripts/supervisor/stateStore.mjs";

async function waitForFile(file) {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      return await readFile(file, "utf8");
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }
  throw new Error(`Timed out waiting for ${file}`);
}

const GOOD_RESULT = JSON.stringify({
  status: "COMPLETE",
  task: "Return the current hostname and nothing else.",
  evidence: [{ type: "command_output", source: "hostname", summary: "test-host" }],
  changes: [],
  tests: ["hostname returned"],
  blocker: null,
  next_action: "none",
  requires_approval: false,
});

async function fixtureAdapter(mode, options = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), "hermes-local-adapter-"));
  const fixture = path.join(root, "fake-hermes.sh");
  const calls = path.join(root, "calls.txt");
  await writeFile(
    fixture,
    `printf '%s\\n' "$@" > ${JSON.stringify(calls)}
case " $* " in *" --version "*) printf '%s\\n' 'Hermes 3.2.0'; exit 0;; esac
if [ ${JSON.stringify(mode)} = timeout ]; then sleep 1; fi
if [ ${JSON.stringify(mode)} = malformed ]; then
  printf '%s\\n' 'not a structured result'
elif [ ${JSON.stringify(mode)} = empty ]; then
  printf ''
elif [ ${JSON.stringify(mode)} = incomplete ]; then
  printf '%s\\n' 'STATUS=COMPLETE' 'TASK=Return the current hostname and nothing else.' 'EVIDENCE=test-host'
elif [ ${JSON.stringify(mode)} = failure ]; then
  printf '%s\\n' 'local executor diagnostic'
  exit 1
else
  printf '%s\\n' '${GOOD_RESULT}'
fi
`
  );
  return {
    adapter: new HermesLocalAdapter({
      executable: "/bin/sh",
      args: [fixture],
      timeoutMs: options.timeoutMs ?? 1_000,
    }),
    calls,
  };
}

function task() {
  return {
    prompt: "Return the current hostname and nothing else.",
    description: "Return the current hostname and nothing else.",
    mission_id: "mission-123",
    task_id: "hostname-task",
    attempt_id: "attempt-123",
    request_id: "request-123",
    correlation_id: "request-123",
    mutating: false,
    permission_decision: "AUTO_CONTINUE",
  };
}

test("Hermes local adapter health-checks the configured executable", async () => {
  const { adapter } = await fixtureAdapter("success");
  assert.deepEqual(await adapter.health_check(), {
    status: "healthy",
    transport: "hermes-local",
    version: "Hermes 3.2.0",
  });
});

test("Hermes local adapter executes a harmless task and propagates task identity", async () => {
  const { adapter, calls } = await fixtureAdapter("success");
  const handle = await adapter.send_task(task());
  const parsed = parseHermesResult(await adapter.wait_for_result(handle));
  assert.equal(parsed.status, "COMPLETE");
  assert.equal((await adapter.poll_status(handle)).status, "complete");
  const invocation = (await readFile(calls, "utf8")).trim().split("\n");
  assert.deepEqual(invocation.slice(0, -2), [
    "chat",
    "--ignore-rules",
    "--quiet",
    "--max-turns",
    "4",
    "--format",
    "stream-json",
    "--source",
    "codex-supervisor:hostname-task:attempt-123:request-123",
    "--query",
  ]);
  assert.equal(
    invocation.slice(-2).join("\n"),
    "Use only the terminal tool. Execute the task; do not guess or describe the answer.\nTASK: Return the current hostname and nothing else."
  );
});

test("Supervisor Hermes profile omits repository context and contract instructions", async () => {
  const { adapter, calls } = await fixtureAdapter("success");
  const handle = await adapter.send_task({
    ...task(),
    prompt: "CONTEXT={large supervisor state}\nSTATUS=\nTASK=\nAGENTS.md",
  });
  await adapter.wait_for_result(handle);
  const invocation = await readFile(calls, "utf8");
  assert.match(invocation, /--ignore-rules/);
  assert.match(invocation, /Use only the terminal tool/);
  assert.match(invocation, /Return the current hostname and nothing else/);
  assert.doesNotMatch(invocation, /AGENTS\.md|CONTEXT=|REQUIRES_APPROVAL|STATUS=/);
});

test("normal Hermes profile remains opt-in and unchanged", async () => {
  const { adapter: fixture, calls } = await fixtureAdapter("success");
  const adapter = new HermesLocalAdapter({
    executable: fixture.executable,
    args: fixture.args,
    profile: "normal",
  });
  const handle = await adapter.send_task(task());
  await adapter.wait_for_result(handle);
  const invocation = await readFile(calls, "utf8");
  assert.doesNotMatch(invocation, /--ignore-rules/);
  assert.match(invocation, new RegExp(task().prompt.replaceAll(".", "\\.")));
});

test("stream-json normalization preserves terminal command and stdout as distinct evidence", () => {
  const stream = [
    { type: "system", subtype: "init", model: "local", session_id: "s1" },
    {
      type: "tool_use",
      name: "terminal",
      tool_call_id: "call-1",
      input: { command: "hostname" },
    },
    {
      type: "tool_result",
      name: "terminal",
      tool_call_id: "call-1",
      output: JSON.stringify({ output: "lois\n", exit_code: 0, error: null }),
      is_error: false,
    },
    { type: "result", session_id: "s1", exit_code: 0, text: "lois" },
  ]
    .map((event) => JSON.stringify(event))
    .join("\n");
  const parsed = parseHermesResult(normalizeHermesTransportResult(stream, task()));
  assert.equal(parsed.status, "TRANSPORT_COMPLETE");
  assert.equal(parsed.evidence[0].source, "hermes-local terminal command");
  assert.equal(parsed.evidence[0].summary, "hostname");
  assert.equal(parsed.evidence[1].source, "hermes-local terminal stdout");
  assert.equal(parsed.evidence[1].summary, "lois\n");
  assert.equal(parsed.evidence[2].source, "hermes-local final response");
  assert.equal(parsed.evidence[2].summary, "lois");
});

test("stream-json final text without terminal events cannot satisfy terminal acceptance", () => {
  const stream = [
    { type: "system", subtype: "init", model: "local", session_id: "s1" },
    { type: "result", session_id: "s1", exit_code: 0, text: "lois" },
  ]
    .map((event) => JSON.stringify(event))
    .join("\n");
  const parsed = parseHermesResult(normalizeHermesTransportResult(stream, task()));
  const terminalState = createInitialState({
    mission_id: "terminal-proof",
    milestone: "Supervisor V1",
    goal: "Prove terminal hostname",
    current_task: { description: task().description, mutating: false, scope: ["hostname"] },
    acceptance_criteria: [
      {
        id: "terminal-hostname",
        description: "Terminal ran hostname and returned expected stdout",
        required_evidence: ["hermes-local terminal command", "hermes-local terminal stdout"],
        expected_evidence: ["lois"],
        status: "pending",
      },
    ],
  });
  assert.equal(evaluateAcceptance(terminalState, parsed).complete, false);
});

test("Hermes local adapter normalizes incomplete output with bounded hashed provenance", async () => {
  const { adapter } = await fixtureAdapter("incomplete");
  const handle = await adapter.send_task(task());
  const parsed = parseHermesResult(await adapter.wait_for_result(handle));
  assert.equal(parsed.status, "TRANSPORT_COMPLETE");
  assert.equal(parsed.task, task().description);
  assert.equal(parsed.evidence[0].source, "hermes-local stdout");
  assert.match(parsed.evidence[0].summary, /EVIDENCE=test-host/);
  assert.match(parsed.evidence[0].hash, /^[a-f0-9]{64}$/);
  assert.deepEqual(parsed.tests, ["NOT_RUN"]);
  assert.deepEqual(parsed.changes, []);
  assert.equal(parsed.requires_approval, false);
});

test("transport normalization preserves useful plain output but empty output fails closed", async () => {
  const parsed = parseHermesResult(normalizeHermesTransportResult("test-host\n", task()));
  assert.equal(parsed.evidence[0].summary, "test-host");
  const { adapter } = await fixtureAdapter("empty");
  const handle = await adapter.send_task(task());
  await assert.rejects(() => adapter.wait_for_result(handle), /empty output/i);
});

test("transport normalization cannot remove approval from a mutating task", () => {
  const parsed = parseHermesResult(
    normalizeHermesTransportResult("operation finished", { ...task(), mutating: true })
  );
  assert.equal(parsed.requires_approval, true);
});

test("Hermes local adapter times out and fails closed when unavailable", async () => {
  const { adapter } = await fixtureAdapter("timeout", { timeoutMs: 20 });
  const handle = await adapter.send_task(task());
  await assert.rejects(() => adapter.wait_for_result(handle), /timed out/i);

  const missing = new HermesLocalAdapter({ executable: "/missing/hermes" });
  assert.equal((await missing.health_check()).status, "unhealthy");
  await assert.rejects(() => missing.send_task(task()), /unavailable/i);
});

test("inference timeout starts only after endpoint admission", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "hermes-admission-timing-"));
  const { adapter: first } = await fixtureAdapter("timeout", { timeoutMs: 2_000 });
  const { adapter: second } = await fixtureAdapter("success", { timeoutMs: 50 });
  for (const adapter of [first, second]) {
    adapter.admissionRoot = root;
    adapter.endpointKey = "http://127.0.0.1:8080/v1";
    adapter.queueTimeoutMs = 2_000;
  }
  const firstHandle = await first.send_task({ ...task(), attempt_id: "first" });
  const secondHandle = await second.send_task({ ...task(), attempt_id: "second" });
  await first.cancel_task(firstHandle);
  await assert.rejects(() => first.wait_for_result(firstHandle), /cancelled/i);
  const parsed = parseHermesResult(await second.wait_for_result(secondHandle));
  assert.equal(parsed.status, "COMPLETE");
});

test("three concurrent Hermes adapters serialize execution for one endpoint slot", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "hermes-admission-three-"));
  const fixture = path.join(root, "serialized-hermes.sh");
  const active = path.join(root, "active");
  const violation = path.join(root, "overlap");
  await writeFile(
    fixture,
    `if ! mkdir ${JSON.stringify(active)} 2>/dev/null; then printf overlap > ${JSON.stringify(violation)}; exit 9; fi
sleep 0.04
rmdir ${JSON.stringify(active)}
printf '%s\\n' '${GOOD_RESULT}'
`
  );
  const adapters = Array.from(
    { length: 3 },
    () =>
      new HermesLocalAdapter({
        executable: "/bin/sh",
        args: [fixture],
        timeoutMs: 500,
        queueTimeoutMs: 1_000,
        admissionRoot: root,
        endpointKey: "http://127.0.0.1:8080/v1",
        endpointCapacity: 1,
      })
  );
  const results = await Promise.all(
    adapters.map(async (adapter, index) => {
      const handle = await adapter.send_task({
        ...task(),
        task_id: `task-${index}`,
        attempt_id: `attempt-${index}`,
      });
      return parseHermesResult(await adapter.wait_for_result(handle));
    })
  );
  assert.equal(results.length, 3);
  await assert.rejects(() => readFile(violation, "utf8"), { code: "ENOENT" });
});

test("terminal evidence received before inference timeout is returned as blocked partial evidence", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "hermes-partial-"));
  const fixture = path.join(root, "partial-hermes.sh");
  await writeFile(
    fixture,
    `printf '%s\\n' '${JSON.stringify({ type: "system", subtype: "init" })}'
printf '%s\\n' '${JSON.stringify({ type: "tool_use", name: "terminal", input: { command: "pwd" } })}'
printf '%s\\n' '${JSON.stringify({ type: "tool_result", name: "terminal", output: JSON.stringify({ output: "/work", exit_code: 0, error: null }), is_error: false })}'
sleep 1
`
  );
  const adapter = new HermesLocalAdapter({
    executable: "/bin/sh",
    args: [fixture],
    timeoutMs: 30,
    admissionRoot: root,
    endpointKey: "http://127.0.0.1:8080/v1",
  });
  const handle = await adapter.send_task(task());
  const parsed = parseHermesResult(await adapter.wait_for_result(handle));
  assert.equal(parsed.status, "TRANSPORT_PARTIAL");
  assert.match(parsed.blocker, /timed out/i);
  assert.equal(parsed.evidence[0].summary, "pwd");
  assert.equal(parsed.evidence[1].summary, "/work");
  const state = createInitialState({
    mission_id: "partial",
    milestone: "partial",
    goal: "partial",
    current_task: { description: task().description, mutating: false, scope: ["pwd"] },
    acceptance_criteria: [
      {
        id: "pwd",
        description: "pwd",
        required_evidence: ["hermes-local terminal stdout"],
        expected_evidence: ["/work"],
        status: "pending",
      },
    ],
  });
  assert.equal(decideResult(state, parsed).action, "BLOCKED");
});

test("Hermes local adapter preserves bounded stdout diagnostics on failure", async () => {
  const { adapter } = await fixtureAdapter("failure");
  const handle = await adapter.send_task(task());
  await assert.rejects(() => adapter.wait_for_result(handle), /local executor diagnostic/i);
});

test("Hermes local adapter deduplicates the same attempt and supports cancellation", async () => {
  const { adapter, calls } = await fixtureAdapter("timeout", { timeoutMs: 1_000 });
  const first = await adapter.send_task(task());
  const duplicate = await adapter.send_task(task());
  assert.equal(duplicate, first);
  assert.equal((await waitForFile(calls)).match(/codex-supervisor:/g)?.length, 1);
  assert.equal((await adapter.cancel_task(first)).status, "cancelled");
  await assert.rejects(() => adapter.wait_for_result(first), /cancelled/i);
});

test("Supervisor parses a local Hermes result and persists redacted state and audit", async () => {
  const { adapter } = await fixtureAdapter("success");
  const root = await mkdtemp(path.join(os.tmpdir(), "hermes-local-supervisor-"));
  const store = new MissionStateStore(root, "hostname");
  await store.save(
    createInitialState({
      mission_id: "hostname",
      milestone: "Supervisor V1 real adapter",
      goal: "Return the current hostname",
      authoritative_facts: ["Harmless read-only task"],
      current_task: {
        description: "Return the current hostname and nothing else.",
        mutating: false,
        scope: ["hostname"],
      },
      acceptance_criteria: [
        {
          id: "hostname",
          description: "Hostname is returned",
          required_evidence: ["hostname"],
          status: "pending",
        },
      ],
      forbidden_topics: [],
      forbidden_paths: [],
      forbidden_actions: ["file writes", "service changes"],
    })
  );
  const result = await new SupervisorEngine({ store, adapter }).runOnce();
  assert.equal(result.status, "COMPLETE");
  assert.equal(result.acceptance_criteria[0].status, "pass");
  const persisted = `${await readFile(store.statePath, "utf8")}\n${await readFile(
    store.auditPath,
    "utf8"
  )}`;
  assert.match(persisted, /hostname-task|hostname/);
  assert.doesNotMatch(persisted, /api[_-]?key\s*[:=]\s*[^[]/i);
});

test("normalized transport evidence cannot manufacture acceptance", async () => {
  const { adapter } = await fixtureAdapter("malformed");
  const root = await mkdtemp(path.join(os.tmpdir(), "hermes-local-normalized-"));
  const store = new MissionStateStore(root, "hostname");
  await store.save(
    createInitialState({
      mission_id: "hostname",
      milestone: "Supervisor V1 real adapter",
      goal: "Return the current hostname",
      authoritative_facts: ["Expected hostname is test-host"],
      current_task: {
        description: "Return the current hostname and nothing else.",
        mutating: false,
        scope: ["hostname"],
      },
      acceptance_criteria: [
        {
          id: "hostname",
          description: "Expected hostname is returned",
          required_evidence: ["test-host"],
          status: "pending",
        },
      ],
      forbidden_topics: [],
      forbidden_paths: [],
      forbidden_actions: [],
    })
  );
  const result = await new SupervisorEngine({ store, adapter }).runOnce();
  assert.equal(result.status, "CORRECTING");
  assert.notEqual(result.acceptance_criteria[0].status, "pass");
});
