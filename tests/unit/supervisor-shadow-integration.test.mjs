import assert from "node:assert/strict";
import { access, mkdir, mkdtemp, readdir, symlink, writeFile, readFile } from "node:fs/promises";
import { spawn, spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  createAttemptIdentity,
  createShadowRecord,
  DurableShadowSpool,
  mergeShadowRecord,
  validateShadowRecord,
} from "../../scripts/supervisor/routing/shadowAdvisory.mjs";
import { hermesIdentityEnvironment } from "../../scripts/supervisor/adapters/hermesLocalAdapter.mjs";

const base = (overrides = {}) =>
  createShadowRecord({
    task_id: "task-1",
    attempt_id: "attempt-1",
    request_id: "corr-1",
    correlation_id: "corr-1",
    task_type: "code-review",
    authoritative_route: "local",
    created_at: "2026-09-22T12:00:00.000Z",
    updated_at: "2026-09-22T12:00:00.000Z",
    ...overrides,
  });

test("Supervisor emits the complete flattened v1 contract accepted by the importer", () => {
  const record = validateShadowRecord(base());
  assert.equal(record.schema_version, 1);
  assert.equal(record.authoritative_route, "local");
  assert.equal(record.agreement, "unknown");
  assert.equal(record.counterfactual_quality, "UNKNOWN");
  assert.equal(record.outcome_state, "pending");
  assert.equal(record.final_success, null);
  for (const field of [
    "task_id",
    "attempt_id",
    "task_type",
    "request_id",
    "correlation_id",
    "actual_executor",
    "actual_model",
    "actual_provider",
    "laya_selected",
    "laya_abstain",
    "laya_confidence",
    "laya_latency_ms",
    "laya_peak_rss_bytes",
    "laya_fallback",
    "laya_fallback_reason",
    "laya_error_category",
    "laya_adapter_version",
    "input_tokens",
    "output_tokens",
    "cached_tokens",
    "cost",
    "retries",
    "final_success",
    "outcome_state",
    "agreement",
    "counterfactual_quality",
    "created_at",
    "updated_at",
  ])
    assert.ok(Object.hasOwn(record, field), `missing ${field}`);
});

test("Supervisor-produced record round-trips through the Task Observer importer and shared schema copy", async (t) => {
  const observerRoot = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../../task-observer-shadow-work/src"
  );
  const consumerSchemaPath = path.join(
    observerRoot,
    "schemas",
    "laya-shadow-record-v1.schema.json"
  );
  try {
    await access(consumerSchemaPath);
  } catch {
    t.skip("Task Observer implementation worktree is not available in this checkout");
    return;
  }
  const producerSchema = JSON.parse(
    await readFile(
      new URL("../../schemas/laya-shadow-record-v1.schema.json", import.meta.url),
      "utf8"
    )
  );
  const consumerSchema = JSON.parse(await readFile(consumerSchemaPath, "utf8"));
  assert.deepEqual(producerSchema, consumerSchema);
  const produced = base({
    actual_executor: "local",
    input_tokens: 41,
    outcome_state: "success",
    final_success: true,
  });
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-roundtrip-"));
  const source = path.join(root, "records");
  const store = path.join(root, "observer.jsonl");
  const jobId = "a".repeat(64);
  await mkdir(source);
  await writeFile(path.join(source, `${jobId}.json`), `${JSON.stringify(produced)}\n`, {
    mode: 0o600,
  });
  const script = [
    "import json, sys",
    "from task_observer.shadow import import_shadow_records, ShadowTelemetryStore",
    "result = import_shadow_records(sys.argv[1], sys.argv[2])",
    "record = ShadowTelemetryStore(sys.argv[2]).get_record('task-1', 'attempt-1')",
    "print(json.dumps({'result': result, 'record': record}, sort_keys=True))",
  ].join("\n");
  const completed = spawnSync("python3", ["-c", script, source, store], {
    encoding: "utf8",
    maxBuffer: 16_384,
    env: { ...process.env, PYTHONPATH: observerRoot },
  });
  assert.equal(completed.status, 0, completed.stderr);
  const imported = JSON.parse(completed.stdout);
  assert.equal(imported.result.imported, 1);
  assert.deepEqual(Object.keys(imported.record).sort(), Object.keys(produced).sort());
  assert.equal(imported.record.task_id, produced.task_id);
  assert.equal(imported.record.attempt_id, produced.attempt_id);
  assert.equal(imported.record.request_id, produced.request_id);
  assert.equal(imported.record.correlation_id, produced.correlation_id);
  assert.equal(imported.record.actual_executor, "local");
  assert.equal(imported.record.input_tokens, 41);
  assert.equal(imported.record.final_success, true);
});

test("record validation uses shared ID bounds and documented route and agreement values", () => {
  assert.throws(() => validateShadowRecord(base({ task_id: "t".repeat(129) })));
  assert.throws(() => validateShadowRecord(base({ attempt_id: "a".repeat(129) })));
  assert.throws(() => validateShadowRecord(base({ authoritative_route: { executor: "local" } })));
  assert.throws(() => validateShadowRecord(base({ agreement: true })));
  assert.throws(() => validateShadowRecord(base({ agreement: "maybe" })));
  assert.throws(() => validateShadowRecord(base({ laya_selected: "unlisted" })));
});

test("attempt identity preserves logical task and never fabricates call-log correlation", () => {
  const first = createAttemptIdentity("task-1");
  const retry = createAttemptIdentity("task-1");
  assert.equal(first.task_id, retry.task_id);
  assert.notEqual(first.attempt_id, retry.attempt_id);
  assert.notEqual(first.request_id, retry.request_id);
  assert.equal(first.request_id, first.correlation_id);
  assert.notEqual(first.correlation_id, first.attempt_id);
});

test("Hermes subprocess receives per-attempt v2 header values through env expansion", () => {
  const identity = createAttemptIdentity("task-1");
  const environment = hermesIdentityEnvironment({ mission_id: "mission-1", ...identity }, {});
  assert.equal(environment.JARVIS_OMNIROUTE_CONTRACT_VERSION, "2");
  assert.equal(environment.JARVIS_OMNIROUTE_TASK_ID, "task-1");
  assert.equal(environment.JARVIS_OMNIROUTE_ATTEMPT_ID, identity.attempt_id);
  assert.equal(environment.JARVIS_OMNIROUTE_REQUEST_ID, identity.request_id);
  assert.equal(environment.JARVIS_OMNIROUTE_CORRELATION_ID, identity.correlation_id);
  const invalid = hermesIdentityEnvironment(
    {
      mission_id: "mission-1",
      task_id: "x".repeat(129),
      attempt_id: "attempt-1",
      request_id: "corr-1",
      correlation_id: "corr-1",
    },
    { JARVIS_OMNIROUTE_TASK_ID: "stale-task", JARVIS_OMNIROUTE_CORRELATION_ID: "stale-correlation" }
  );
  assert.equal(Object.hasOwn(invalid, "JARVIS_OMNIROUTE_TASK_ID"), false);
  assert.equal(Object.hasOwn(invalid, "JARVIS_OMNIROUTE_CORRELATION_ID"), false);
});

test("verified telemetry fills unknown fields and null or missing patches preserve known values", () => {
  const known = base({
    actual_executor: "local",
    input_tokens: 37,
    final_success: true,
    outcome_state: "success",
  });
  const merged = mergeShadowRecord(known, {
    actual_executor: null,
    input_tokens: null,
    final_success: null,
    outcome_state: "unknown",
  });
  assert.equal(merged.actual_executor, "local");
  assert.equal(merged.input_tokens, 37);
  assert.equal(merged.final_success, true);
  assert.equal(merged.outcome_state, "success");
  const verified = mergeShadowRecord(base(), { actual_executor: "local", input_tokens: 37 });
  assert.equal(verified.actual_executor, "local");
  assert.equal(verified.input_tokens, 37);
});

test("waiting approval is nonterminal and leaves final_success unknown", () => {
  const waiting = mergeShadowRecord(base(), {
    outcome_state: "waiting_approval",
    final_success: null,
  });
  assert.equal(waiting.outcome_state, "waiting_approval");
  assert.equal(waiting.final_success, null);
  const unknown = mergeShadowRecord(waiting, { outcome_state: "unknown", final_success: null });
  assert.equal(unknown.outcome_state, "waiting_approval");
  const completed = mergeShadowRecord(unknown, { outcome_state: "success", final_success: true });
  const staleFailure = mergeShadowRecord(completed, {
    outcome_state: "failure",
    final_success: false,
  });
  assert.equal(staleFailure.outcome_state, "success");
  assert.equal(staleFailure.final_success, true);
});

test("durable spool survives worker restart, reclaims expired claims, and acknowledges idempotently", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-spool-"));
  const spool = new DurableShadowSpool(root, { maxQueue: 2, leaseMs: 200 });
  const initial = base();
  const jobInput = {
    task_id: initial.task_id,
    attempt_id: initial.attempt_id,
    request_id: initial.request_id,
    correlation_id: initial.correlation_id,
    task_type: initial.task_type,
    candidates: [
      { id: "local", model: null, provider: null },
      { id: "other", model: null, provider: null },
    ],
    metadata: {
      bounded_redacted_description: "Review this sufficiently detailed code change safely.",
      required_tool_ids: [],
      cost_or_escalation_band: "premium-escalation",
    },
    schema_version: 1,
  };
  await spool.enqueue(jobInput, initial);
  const worker1 = await spool.claim("worker-1");
  assert.equal(worker1.record.task_id, "task-1");
  const afterRestart = new DurableShadowSpool(root, { maxQueue: 2, leaseMs: 200 });
  assert.equal(await afterRestart.claim("worker-2"), null);
  await new Promise((resolve) => setTimeout(resolve, 250));
  const reclaimed = await afterRestart.claim("worker-2");
  assert.equal(reclaimed.job_id, worker1.job_id);
  const result = {
    task_id: "task-1",
    attempt_id: "attempt-1",
    selected: "local",
    abstain: false,
    confidence: 0.8,
    latency_ms: 12,
    peak_rss_bytes: 4096,
    fallback: false,
    adapter_version: "adapter-v1",
  };
  assert.equal(await afterRestart.complete(reclaimed, result), true);
  const resultPath = path.join(root, "records", `${reclaimed.job_id}.json`);
  const firstBytes = await readFile(resultPath);
  assert.equal(await afterRestart.complete(reclaimed, result), true);
  assert.deepEqual(await readFile(resultPath), firstBytes);
  assert.equal(await afterRestart.claim("worker-3"), null);
  assert.equal((await afterRestart.listResults()).length, 1);
});

test("spool writes are atomic, concurrent enqueue is bounded, and temp tails are ignored", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-bounds-"));
  const spool = new DurableShadowSpool(root, { maxQueue: 2 });
  const enqueue = (index) => {
    const taskId = `task-${index}`;
    const attemptId = `attempt-${index}`;
    const correlation = `corr-${index}`;
    return spool.enqueue(
      {
        task_id: taskId,
        attempt_id: attemptId,
        request_id: correlation,
        correlation_id: correlation,
        task_type: "code-review",
        candidates: [{ id: "local" }, { id: "premium" }],
        metadata: {
          bounded_redacted_description: "Review this sufficiently detailed code change safely.",
          required_tool_ids: [],
          cost_or_escalation_band: "premium-escalation",
        },
        schema_version: 1,
      },
      base({
        task_id: taskId,
        attempt_id: attemptId,
        request_id: correlation,
        correlation_id: correlation,
      })
    );
  };
  const outcomes = await Promise.all([enqueue(1), enqueue(2), enqueue(3)]);
  assert.equal(outcomes.filter(Boolean).length, 2);
  const pending = await readdir(path.join(root, "pending"));
  assert.equal(pending.filter((name) => name.endsWith(".json")).length, 2);
  await writeFile(path.join(root, "pending", ".partial-tail"), '{"schema_version":1');
  assert.equal((await spool.claim("worker"))?.record.schema_version, 1);
});

test("spool lock serializes independent producer processes", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-process-lock-"));
  const moduleUrl = new URL("../../scripts/supervisor/routing/shadowAdvisory.mjs", import.meta.url)
    .href;
  const scriptFor = (index) => `
    import { createShadowRecord, DurableShadowSpool } from ${JSON.stringify(moduleUrl)};
    const i = ${index};
    const task = "task-" + i, attempt = "attempt-" + i, corr = "corr-" + i;
    const value = createShadowRecord({ task_id: task, attempt_id: attempt, request_id: corr, correlation_id: corr, task_type: "review", authoritative_route: "local" });
    const input = { task_id: task, attempt_id: attempt, request_id: corr, correlation_id: corr, task_type: "review", candidates: [{ id: "local" }, { id: "remote" }], metadata: { bounded_redacted_description: "Review this detailed change for correctness and safety.", required_tool_ids: [], cost_or_escalation_band: "premium-escalation" }, schema_version: 1 };
    const accepted = await new DurableShadowSpool(${JSON.stringify(root)}, { maxQueue: 5 }).enqueue(input, value);
    process.stdout.write(JSON.stringify({ accepted }));
  `;
  const runChild = (script) =>
    new Promise((resolve, reject) => {
      const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      child.stdout.on("data", (chunk) => {
        stdout += chunk.toString("utf8");
      });
      child.stderr.on("data", (chunk) => {
        stderr += chunk.toString("utf8");
      });
      child.on("error", reject);
      child.on("close", (code) =>
        code === 0 ? resolve(JSON.parse(stdout)) : reject(new Error(stderr.slice(0, 500)))
      );
    });
  const outcomes = await Promise.all(
    Array.from({ length: 8 }, (_, index) => runChild(scriptFor(index + 1)))
  );
  assert.equal(outcomes.filter((result) => result.accepted).length, 5);
  const pending = await readdir(path.join(root, "pending"));
  assert.equal(pending.filter((name) => name.endsWith(".json")).length, 5);
  for (const name of pending.filter((item) => item.endsWith(".json"))) {
    const envelope = JSON.parse(await readFile(path.join(root, "pending", name), "utf8"));
    assert.equal(envelope.input.task_id, envelope.record.task_id);
    assert.equal(envelope.input.attempt_id, envelope.record.attempt_id);
  }
});

test("spool rejects symlinked storage boundaries", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-path-"));
  const outside = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-outside-"));
  await symlink(outside, path.join(root, "pending"));
  const spool = new DurableShadowSpool(root);
  const initial = base();
  const value = {
    task_id: initial.task_id,
    attempt_id: initial.attempt_id,
    request_id: initial.request_id,
    correlation_id: initial.correlation_id,
    task_type: initial.task_type,
    candidates: [{ id: "local" }, { id: "other" }],
    metadata: {
      bounded_redacted_description: "Review this sufficiently detailed code change safely.",
      required_tool_ids: [],
      cost_or_escalation_band: "premium-escalation",
    },
    schema_version: 1,
  };
  await assert.rejects(spool.enqueue(value, initial), /unsafe|boundary/i);
});
