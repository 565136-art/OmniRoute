import assert from "node:assert/strict";
import test from "node:test";

import { routeTask } from "../../scripts/supervisor/routing/empiricalRouter.mjs";

const candidate = (executor, overrides = {}) => ({
  executor,
  model: `${executor}-model`,
  provider: executor === "local" ? "custom" : "omniroute",
  available: true,
  tools: ["terminal", "edit"],
  context_ids: ["repo"],
  estimated_cost: executor === "local" ? 0 : 2,
  ...overrides,
});
const stat = (executor, samples, successes, overrides = {}) => ({
  task_type: "shell-read",
  executor,
  model: `${executor}-model`,
  provider: executor === "local" ? "custom" : "omniroute",
  tool_profile: null,
  samples,
  successes,
  verifier_passes: successes,
  acceptance_passes: successes,
  smoothed_success: (successes + 1) / (samples + 2),
  smoothed_verified: (successes + 1) / (samples + 2),
  average_latency_ms: executor === "local" ? 20 : 100,
  average_cost: executor === "local" ? 0 : 2,
  average_retries: 0,
  recent_failures: samples - successes,
  ...overrides,
});
const input = (overrides = {}) => ({
  task_type: "shell-read",
  candidates: [candidate("local"), candidate("codex")],
  statistics: [stat("local", 10, 10), stat("codex", 10, 8)],
  permission_decision: "AUTO_CONTINUE",
  conservative_fallback: "codex",
  required_tools: ["terminal"],
  required_context_ids: [],
  minimum_samples: 5,
  exploration_interval: 10,
  exploration_sequence: 1,
  mutating: false,
  requires_approval: false,
  safe_read_only: true,
  ...overrides,
});

const degradedLocal = (overrides = {}) =>
  stat("local", 5, 3, {
    smoothed_success: 4 / 7,
    smoothed_verified: 4 / 7,
    smoothed_acceptance: 4 / 7,
    average_latency_ms: 694_159.6,
    recent_average_latency_ms: 840_000,
    average_retries: 0.8,
    recent_average_retries: 1.6,
    retry_exhaustions: 2,
    recent_retry_exhaustions: 2,
    recent_failure_streak: 2,
    recent_success_streak: 0,
    ...overrides,
  });

test("known high-performing local executor wins and failing local escalates", () => {
  assert.equal(routeTask(input()).selected.executor, "local");
  const failed = input({ statistics: [stat("local", 10, 2), stat("codex", 10, 9)] });
  assert.equal(routeTask(failed).selected.executor, "codex");
});

test("insufficient data uses conservative fallback and unavailable candidates are excluded", () => {
  const result = routeTask(input({ statistics: [stat("local", 2, 2), stat("codex", 1, 1)] }));
  assert.equal(result.selected.executor, "codex");
  const unavailable = routeTask(
    input({ candidates: [candidate("local", { available: false }), candidate("codex")] })
  );
  assert.equal(unavailable.selected.executor, "codex");
});

test("acceptance reliability outranks cost and Laya cannot overrule empirical evidence", () => {
  const result = routeTask(
    input({
      statistics: [stat("local", 20, 12), stat("codex", 20, 20)],
      laya: { selected: ["local"], confidence: 1, fallback: false },
    })
  );
  assert.equal(result.selected.executor, "codex");
  assert.equal(result.explanation.LAYA_USED, "NO");
});

test("empirical routing ignores Laya advice, including scoring ties", () => {
  const result = routeTask(
    input({
      statistics: [
        stat("local", 10, 10, { average_cost: 0, average_latency_ms: 20 }),
        stat("codex", 10, 10, { average_cost: 0, average_latency_ms: 20 }),
      ],
      laya: {
        selected: ["local"],
        confidence: 0.9,
        fallback: false,
        model: "local-laya",
        latency_ms: 12,
      },
    })
  );
  assert.equal(result.selected.executor, "codex");
  assert.equal(result.explanation.LAYA_USED, "NO");
  assert.equal(result.explanation.LAYA_RECOMMENDATION, null);
  assert.equal(result.explanation.EMPIRICAL_SELECTION, "codex");
  assert.equal(result.explanation.FINAL_SELECTION, "codex");
  assert.throws(() => routeTask(input({ permission_decision: "FORBIDDEN" })), /forbidden/i);
  assert.throws(
    () => routeTask(input({ permission_decision: "REQUIRES_USER_APPROVAL" })),
    /approval/i
  );
});

test("one anomaly is smoothed and bounded exploration remains possible", () => {
  const stable = routeTask(input({ statistics: [stat("local", 11, 10), stat("codex", 11, 9)] }));
  assert.equal(stable.selected.executor, "local");
  const explored = routeTask(
    input({
      statistics: [stat("local", 20, 20), stat("codex", 1, 1)],
      exploration_sequence: 10,
    })
  );
  assert.equal(explored.selected.executor, "codex");
  assert.match(explored.explanation.SELECTION_REASON, /exploration/i);
});

test("route decisions own candidate snapshots so parallel state redaction is stable", () => {
  const shared = candidate("local");
  const first = routeTask(input({ candidates: [shared] }));
  const second = routeTask(input({ candidates: [shared] }));
  assert.notEqual(first.selected, shared);
  assert.notEqual(first.selected, second.selected);
  assert.deepEqual(first.selected, shared);
});

test("3/5 success with retry exhaustion and very high latency is degraded and loses normal preference", () => {
  const result = routeTask(
    input({
      statistics: [degradedLocal(), stat("codex", 5, 4, { average_cost: 1 })],
    })
  );
  const local = result.scored_candidates.find((entry) => entry.executor === "local");
  assert.equal(local.health_state, "DEGRADED");
  assert.ok(local.latency_penalty > 0);
  assert.ok(local.retry_penalty > 0);
  assert.equal(result.selected.executor, "codex");
});

test("repeated recent retry-exhausted failures trigger degradation", () => {
  const result = routeTask(
    input({ candidates: [candidate("local")], statistics: [degradedLocal()] })
  );
  assert.equal(result.scored_candidates[0].health_state, "DEGRADED");
  assert.equal(result.explanation.EXECUTOR_HEALTH_STATE, "DEGRADED");
});

test("one later success does not instantly clear degradation", () => {
  const result = routeTask(
    input({
      candidates: [candidate("local")],
      statistics: [
        degradedLocal({
          samples: 6,
          successes: 4,
          smoothed_success: 5 / 8,
          smoothed_verified: 5 / 8,
          smoothed_acceptance: 5 / 8,
          recent_failure_streak: 0,
          recent_success_streak: 1,
        }),
      ],
    })
  );
  assert.equal(result.scored_candidates[0].health_state, "DEGRADED");
});

test("sustained recent successes recover through probation to healthy", () => {
  const probation = routeTask(
    input({
      candidates: [candidate("local")],
      statistics: [
        degradedLocal({
          samples: 7,
          successes: 5,
          smoothed_success: 6 / 9,
          smoothed_verified: 6 / 9,
          smoothed_acceptance: 6 / 9,
          recent_failure_streak: 0,
          recent_success_streak: 2,
          recent_retry_exhaustions: 1,
          recent_average_latency_ms: 80,
          recent_average_retries: 0.4,
        }),
      ],
    })
  );
  const healthy = routeTask(
    input({
      candidates: [candidate("local")],
      statistics: [
        degradedLocal({
          samples: 10,
          successes: 8,
          smoothed_success: 9 / 12,
          smoothed_verified: 9 / 12,
          smoothed_acceptance: 9 / 12,
          recent_failure_streak: 0,
          recent_success_streak: 5,
          recent_retry_exhaustions: 0,
          recent_average_latency_ms: 80,
          recent_average_retries: 0,
        }),
      ],
    })
  );
  assert.equal(probation.scored_candidates[0].health_state, "PROBATION");
  assert.equal(healthy.scored_candidates[0].health_state, "HEALTHY");
});

test("zero cost cannot outweigh poor verified completion", () => {
  const result = routeTask(
    input({
      statistics: [
        degradedLocal(),
        stat("codex", 5, 4, { average_cost: 100, smoothed_verified: 5 / 7 }),
      ],
    })
  );
  assert.equal(result.selected.executor, "codex");
});

test("acceptance reliability dominates latency and cost", () => {
  const result = routeTask(
    input({
      statistics: [
        stat("local", 10, 9, {
          smoothed_verified: 10 / 12,
          smoothed_acceptance: 6 / 12,
          average_latency_ms: 1,
          average_cost: 0,
        }),
        stat("codex", 10, 9, {
          smoothed_verified: 10 / 12,
          smoothed_acceptance: 10 / 12,
          average_latency_ms: 10_000,
          average_cost: 100,
        }),
      ],
    })
  );
  assert.equal(result.selected.executor, "codex");
});

test("degraded executor remains eligible for bounded safe exploration", () => {
  const result = routeTask(
    input({
      statistics: [degradedLocal(), stat("codex", 20, 19)],
      exploration_sequence: 10,
    })
  );
  assert.equal(result.selected.executor, "local");
  assert.match(result.explanation.SELECTION_REASON, /exploration/i);
});

test("mutation and risky tasks never use exploration", () => {
  for (const unsafe of [
    { mutating: true },
    { requires_approval: true },
    { safe_read_only: false },
  ]) {
    const result = routeTask(
      input({
        statistics: [degradedLocal(), stat("codex", 20, 19)],
        exploration_sequence: 10,
        ...unsafe,
      })
    );
    assert.equal(result.selected.executor, "codex");
    assert.doesNotMatch(result.explanation.SELECTION_REASON, /exploration/i);
  }
});

test("under-sampled challengers cannot bypass conservative fallback outside exploration", () => {
  const result = routeTask(
    input({
      conservative_fallback: "local",
      statistics: [stat("local", 20, 13), stat("codex", 4, 4)],
    })
  );
  assert.equal(result.selected.executor, "local");
  assert.doesNotMatch(result.explanation.SELECTION_REASON, /exploration/i);
});

test("routing matches statistics to the requested tool profile", () => {
  const result = routeTask(
    input({
      candidates: [candidate("local")],
      tool_profile: "terminal-read",
      statistics: [
        stat("local", 10, 1, { tool_profile: "edit" }),
        stat("local", 10, 10, { tool_profile: "terminal-read" }),
      ],
    })
  );
  assert.equal(result.scored_candidates[0].empirical_reliability, 11 / 12);
  assert.equal(result.scored_candidates[0].health_state, "HEALTHY");
});

test("unknown latency and retry metrics do not manufacture degradation", () => {
  const result = routeTask(
    input({
      candidates: [candidate("local")],
      statistics: [
        stat("local", 10, 10, {
          average_latency_ms: null,
          recent_average_latency_ms: null,
          average_retries: null,
          recent_average_retries: null,
        }),
      ],
    })
  );
  assert.equal(result.scored_candidates[0].latency_penalty, 0);
  assert.equal(result.scored_candidates[0].health_state, "HEALTHY");
});
