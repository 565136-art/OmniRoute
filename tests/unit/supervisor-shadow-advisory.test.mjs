import assert from "node:assert/strict";
import { mkdtemp, readdir, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { ResourceLockManager } from "../../scripts/supervisor/resourceLocks.mjs";
import { SupervisorScheduler } from "../../scripts/supervisor/scheduler.mjs";
import { RoutingObservationStore } from "../../scripts/supervisor/routing/observationStore.mjs";
import {
  DurableShadowSpool,
  ShadowAdvisoryDispatcher,
  createAttemptIdentity,
  createShadowRecord,
  isShadowEligible,
  normalizeShadowInput,
  normalizeShadowOutput,
  shadowSample,
} from "../../scripts/supervisor/routing/shadowAdvisory.mjs";

const identity = createAttemptIdentity("task-1", { correlationId: "corr-1" });
const input = (overrides = {}) => ({
  task_id: "task-1",
  attempt_id: "attempt-1",
  request_id: "corr-1",
  correlation_id: "corr-1",
  task_type: "code-review",
  candidates: [
    { id: "local", model: "qwen", provider: "lmstudio", estimated_cost: 0 },
    {
      id: "premium",
      model: "premium-model",
      provider: "remote",
      tier: "premium",
      estimated_cost: 1,
    },
  ],
  metadata: {
    bounded_redacted_description: "Review the patch for correctness and security.",
    required_tool_ids: ["terminal"],
    cost_or_escalation_band: "premium-escalation",
  },
  schema_version: 1,
  ...overrides,
});

const record = (overrides = {}) =>
  createShadowRecord({
    task_id: "task-1",
    attempt_id: "attempt-1",
    request_id: "corr-1",
    correlation_id: "corr-1",
    task_type: "code-review",
    authoritative_route: "local",
    ...overrides,
  });

function eligibleInput(overrides = {}) {
  return input({
    task_id: identity.task_id,
    attempt_id: identity.attempt_id,
    request_id: identity.request_id,
    correlation_id: identity.correlation_id,
    ...overrides,
  });
}

test("shadow is disabled by default and disabled enqueue has no transport effect", async () => {
  let writes = 0;
  const dispatcher = new ShadowAdvisoryDispatcher({
    transport: {
      async enqueue() {
        writes += 1;
      },
    },
  });
  assert.equal(dispatcher.enqueue(eligibleInput(), record({ ...identity })), false);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(writes, 0);
});

test("eligibility omits trivial, urgent, metadata, health and non-escalation work", () => {
  const value = eligibleInput();
  assert.equal(isShadowEligible(value), true);
  assert.equal(isShadowEligible(eligibleInput({ task_type: "health-check" })), false);
  assert.equal(isShadowEligible(eligibleInput({ task_type: "metadata-read" })), false);
  assert.equal(
    isShadowEligible(
      eligibleInput({
        task_type: "code-review",
        metadata: {
          ...value.metadata,
          bounded_redacted_description: "Urgent: please immediately review this patch.",
        },
      })
    ),
    false
  );
  assert.equal(
    isShadowEligible(
      eligibleInput({ metadata: { ...value.metadata, cost_or_escalation_band: "standard" } })
    ),
    false
  );
});

test("sampling is deterministic by logical task and unique attempt", () => {
  const value = eligibleInput();
  assert.equal(shadowSample(value, 0.4), shadowSample(value, 0.4));
  assert.equal(typeof shadowSample(eligibleInput({ attempt_id: "attempt-2" }), 0.4), "boolean");
  assert.equal(shadowSample(value, 0), false);
  assert.equal(shadowSample(value, 1), true);
});

test("request, attempt, and call-log correlation identities are distinct and retry-safe", () => {
  const first = createAttemptIdentity("task-stable", { correlationId: "corr-one" });
  const retry = createAttemptIdentity("task-stable", { correlationId: "corr-two" });
  assert.equal(first.task_id, retry.task_id);
  assert.notEqual(first.attempt_id, retry.attempt_id);
  assert.notEqual(first.attempt_id, first.correlation_id);
  assert.equal(first.request_id, first.correlation_id);
  assert.notEqual(first.correlation_id, retry.correlation_id);
});

test("input and output are bounded and recommendations must remain policy-valid", () => {
  assert.equal(normalizeShadowInput(eligibleInput()).request_id, "corr-1");
  assert.throws(() => normalizeShadowInput(eligibleInput({ task_id: "x".repeat(129) })));
  assert.throws(() => normalizeShadowInput(eligibleInput({ request_id: null })));
  assert.throws(() => normalizeShadowInput(eligibleInput({ correlation_id: "attempt-1" })));
  const value = normalizeShadowInput(eligibleInput());
  assert.throws(() =>
    normalizeShadowOutput(value, {
      task_id: value.task_id,
      attempt_id: value.attempt_id,
      selected: "unlisted",
      abstain: false,
      confidence: 0.9,
      latency_ms: 10,
      peak_rss_bytes: 10,
      fallback: false,
    })
  );
  assert.throws(() =>
    normalizeShadowInput(
      eligibleInput({
        metadata: {
          ...eligibleInput().metadata,
          bounded_redacted_description: "x".repeat(601),
        },
      })
    )
  );
});

test("one durable spool claim is active globally and a worker restart reclaims expired work", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-restart-"));
  const spool = new DurableShadowSpool(root, { maxQueue: 3, leaseMs: 200 });
  const source = record({ ...identity });
  const queued = eligibleInput({ ...identity });
  assert.equal(await spool.enqueue(queued, source), true);
  const firstWorker = await spool.claim("wsl-worker");
  assert.equal(firstWorker.record.task_id, "task-1");
  assert.equal(await spool.claim("another-worker"), null);
  const afterRestart = new DurableShadowSpool(root, { maxQueue: 3, leaseMs: 200 });
  assert.equal(await afterRestart.claim("restarted-worker"), null);
  await new Promise((resolve) => setTimeout(resolve, 250));
  const reclaimed = await afterRestart.claim("restarted-worker");
  assert.equal(reclaimed.job_id, firstWorker.job_id);
  const advice = {
    task_id: reclaimed.input.task_id,
    attempt_id: reclaimed.input.attempt_id,
    selected: "premium",
    abstain: false,
    confidence: 0.8,
    latency_ms: 12,
    peak_rss_bytes: 4096,
    fallback: false,
    adapter_version: "adapter-v1",
  };
  assert.equal(await afterRestart.complete(reclaimed, advice), true);
  const bytesAfterFirstCompletion = await import("node:fs/promises").then(({ readFile }) =>
    readFile(path.join(root, "records", `${reclaimed.job_id}.json`))
  );
  assert.equal(await afterRestart.complete(reclaimed, advice), true);
  const bytesAfterDuplicate = await import("node:fs/promises").then(({ readFile }) =>
    readFile(path.join(root, "records", `${reclaimed.job_id}.json`))
  );
  assert.deepEqual(bytesAfterDuplicate, bytesAfterFirstCompletion);
  assert.equal(await afterRestart.claim("next-worker"), null);
});

test("bounded spool writes ignore partial temp files and reject symlink boundaries", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-bounds-"));
  const spool = new DurableShadowSpool(root, { maxQueue: 2 });
  const queue = (index) =>
    spool.enqueue(
      eligibleInput({
        task_id: `task-${index}`,
        attempt_id: `attempt-${index}`,
        request_id: `corr-${index}`,
        correlation_id: `corr-${index}`,
      }),
      record({
        task_id: `task-${index}`,
        attempt_id: `attempt-${index}`,
        request_id: `corr-${index}`,
        correlation_id: `corr-${index}`,
      })
    );
  const outcomes = await Promise.all([queue(1), queue(2), queue(3)]);
  assert.equal(outcomes.filter(Boolean).length, 2);
  const pending = await readdir(path.join(root, "pending"));
  assert.equal(pending.filter((name) => name.endsWith(".json")).length, 2);
  await writeFile(path.join(root, "pending", ".partial-tail"), '{"schema_version":1');
  assert.equal((await spool.claim("worker"))?.record.schema_version, 1);

  const unsafeRoot = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-path-"));
  const outside = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-outside-"));
  const source = record();
  assert.throws(() => new DurableShadowSpool(`${unsafeRoot}/../outside`), /path/i);
  await symlink(outside, path.join(unsafeRoot, "pending"));
  await assert.rejects(
    new DurableShadowSpool(unsafeRoot).enqueue(
      eligibleInput({
        task_id: source.task_id,
        attempt_id: source.attempt_id,
        request_id: source.request_id,
        correlation_id: source.correlation_id,
      }),
      source
    ),
    /unsafe|boundary/i
  );
});

test("sampled advisory writes are asynchronous and cannot change routeTask or executor selection", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-nonauthority-"));
  const observations = new RoutingObservationStore(root);
  let finishEnqueue;
  let routeSelected;
  let actualExecutor;
  let persistedRecord;
  const dispatcher = new ShadowAdvisoryDispatcher({
    enabled: true,
    sampleRate: 1,
    transport: {
      enqueue(_input, initial) {
        persistedRecord = initial;
        return new Promise((resolve) => {
          finishEnqueue = resolve;
        });
      },
      async updateRecord(patch) {
        persistedRecord = { ...persistedRecord, ...patch };
      },
    },
  });
  const scheduler = new SupervisorScheduler({
    root,
    missionId: "shadow-no-authority",
    lockManager: new ResourceLockManager(root),
    adapterFactory(lane, candidate) {
      actualExecutor = candidate.executor;
      routeSelected = lane.route_decision.selected.executor;
      return {
        async send_task() {
          return "handle";
        },
        async wait_for_result() {
          return JSON.stringify({
            status: "COMPLETE",
            task: lane.description,
            evidence: [{ type: "other", source: "test", summary: "verified" }],
            changes: [],
            tests: ["NOT_RUN"],
            blocker: null,
            next_action: "done",
            requires_approval: false,
          });
        },
      };
    },
    routing: {
      enabled: true,
      store: observations,
      shadowDispatcher: dispatcher,
      candidates: [
        {
          executor: "local",
          model: "qwen",
          provider: "lmstudio",
          available: true,
          tools: ["terminal"],
          context_ids: [],
          estimated_cost: 0,
        },
        {
          executor: "premium",
          model: "remote-model",
          provider: "remote",
          tier: "premium",
          available: true,
          tools: ["terminal"],
          context_ids: [],
          estimated_cost: 1,
        },
      ],
      conservative_fallback: "local",
      minimum_samples: 5,
    },
  });
  await scheduler.initialize({
    goal: "Route without shadow influence",
    tasks: [
      {
        task_id: "task-1",
        task_type: "code-review",
        description: "Review this code change for correctness and security.",
        cost_or_escalation_band: "premium-escalation",
        depends_on: [],
        resources: [],
        can_parallelize: true,
        required_tools: ["terminal"],
        acceptance_criteria: [
          { id: "proof", description: "proof", required_evidence: ["verified"], status: "pending" },
        ],
        verifiers: [],
        max_attempts: 1,
        requires_approval: false,
      },
    ],
  });
  const state = await Promise.race([
    scheduler.run(),
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("shadow enqueue delayed execution")), 500)
    ),
  ]);
  assert.equal(state.lanes["task-1"].status, "COMPLETE");
  assert.equal(actualExecutor, "local");
  assert.equal(routeSelected, "local");
  assert.equal(persistedRecord.authoritative_route, "local");
  assert.equal(persistedRecord.actual_executor, null);
  assert.equal(persistedRecord.final_success, null);
  assert.equal(persistedRecord.counterfactual_quality, "UNKNOWN");
  finishEnqueue(true);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(persistedRecord.outcome_state, "success");
  assert.equal(persistedRecord.final_success, true);
});

test("scheduler retries preserve task identity and issue new per-attempt correlation IDs", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "laya-shadow-retry-"));
  const observations = new RoutingObservationStore(root);
  const records = new Map();
  const dispatched = [];
  const dispatcher = new ShadowAdvisoryDispatcher({
    enabled: true,
    sampleRate: 1,
    transport: {
      async enqueue(job, initial) {
        records.set(job.attempt_id, { job, record: initial });
        return true;
      },
      async updateRecord(patch) {
        const current = records.get(patch.attempt_id);
        if (current) current.record = { ...current.record, ...patch };
      },
    },
  });
  let runs = 0;
  const scheduler = new SupervisorScheduler({
    root,
    missionId: "shadow-retry",
    lockManager: new ResourceLockManager(root),
    adapterFactory(lane) {
      return {
        async send_task(request) {
          dispatched.push({ ...request });
          return "handle";
        },
        async wait_for_result() {
          runs += 1;
          if (runs === 1) throw new Error("transient executor failure");
          return JSON.stringify({
            status: "COMPLETE",
            task: lane.description,
            evidence: [{ type: "other", source: "test", summary: "verified" }],
            changes: [],
            tests: ["NOT_RUN"],
            blocker: null,
            next_action: "done",
            requires_approval: false,
          });
        },
      };
    },
    routing: {
      enabled: true,
      store: observations,
      shadowDispatcher: dispatcher,
      candidates: [
        { executor: "local", available: true, tools: [], context_ids: [], estimated_cost: 0 },
        {
          executor: "premium",
          available: true,
          tier: "premium",
          tools: [],
          context_ids: [],
          estimated_cost: 1,
        },
      ],
      conservative_fallback: "local",
      minimum_samples: 5,
    },
  });
  await scheduler.initialize({
    goal: "retry with attempt-scoped shadow metadata",
    tasks: [
      {
        task_id: "stable-task",
        task_type: "code-review",
        description: "Review this code change for correctness and security.",
        cost_or_escalation_band: "premium-escalation",
        depends_on: [],
        resources: [],
        can_parallelize: true,
        acceptance_criteria: [
          { id: "proof", description: "proof", required_evidence: ["verified"], status: "pending" },
        ],
        verifiers: [],
        max_attempts: 2,
        requires_approval: false,
      },
    ],
  });
  const state = await scheduler.run();
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(state.lanes["stable-task"].status, "COMPLETE");
  assert.equal(dispatched.length, 2);
  assert.equal(dispatched[0].task_id, dispatched[1].task_id);
  assert.notEqual(dispatched[0].attempt_id, dispatched[1].attempt_id);
  assert.equal(dispatched[0].request_id, dispatched[0].correlation_id);
  assert.equal(dispatched[1].request_id, dispatched[1].correlation_id);
  assert.notEqual(dispatched[0].correlation_id, dispatched[1].correlation_id);
  assert.equal(records.get(dispatched[0].attempt_id).record.outcome_state, "unknown");
  assert.equal(records.get(dispatched[0].attempt_id).record.final_success, null);
  assert.equal(records.get(dispatched[1].attempt_id).record.outcome_state, "success");
  assert.equal(records.get(dispatched[1].attempt_id).record.final_success, true);
});
