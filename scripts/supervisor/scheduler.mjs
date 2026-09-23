import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename } from "node:fs/promises";
import path from "node:path";

import { appendAuditEvent } from "./auditLog.mjs";
import { MAX_AUTONOMOUS_ATTEMPTS } from "./constants.mjs";
import { decideResult } from "./evaluator.mjs";
import { redactValue } from "./redaction.mjs";
import { formatHermesPrompt, parseHermesResult } from "./resultContract.mjs";
import { createInitialState } from "./schema.mjs";
import { acquireMissionLock } from "./stateStore.mjs";
import { routeTask } from "./routing/empiricalRouter.mjs";
import { createAttemptIdentity } from "./routing/shadowAdvisory.mjs";

const TERMINAL = new Set(["WAITING_APPROVAL", "BLOCKED", "COMPLETE", "FAILED"]);
const UNCERTAIN = new Set(["RUNNING", "VERIFYING"]);

function validateTasks(tasks) {
  if (!Array.isArray(tasks) || !tasks.length) throw new Error("DAG requires tasks");
  const ids = new Set();
  for (const task of tasks) {
    if (!task.task_id || ids.has(task.task_id)) throw new Error("Task IDs must be unique");
    ids.add(task.task_id);
    if ((task.max_attempts ?? 3) > MAX_AUTONOMOUS_ATTEMPTS) {
      throw new Error("Task max_attempts exceeds Supervisor limit");
    }
    for (const resource of task.resources ?? []) {
      if (!resource.id || !["read", "write"].includes(resource.mode)) {
        throw new Error("Invalid task resource");
      }
    }
  }
  for (const task of tasks) {
    for (const dependency of task.depends_on ?? []) {
      if (!ids.has(dependency)) throw new Error(`Unknown dependency: ${dependency}`);
    }
  }
  const visiting = new Set();
  const visited = new Set();
  const byId = new Map(tasks.map((task) => [task.task_id, task]));
  function visit(id) {
    if (visiting.has(id)) throw new Error("Task graph contains a cycle");
    if (visited.has(id)) return;
    visiting.add(id);
    for (const dependency of byId.get(id).depends_on ?? []) visit(dependency);
    visiting.delete(id);
    visited.add(id);
  }
  for (const id of ids) visit(id);
}

function laneFromTask(task) {
  const now = new Date().toISOString();
  return {
    lane_id: `lane-${randomUUID()}`,
    task_id: task.task_id,
    description: task.description,
    depends_on: [...(task.depends_on ?? [])],
    resources: [...(task.resources ?? [])],
    can_parallelize: task.can_parallelize !== false,
    acceptance_criteria: task.acceptance_criteria.map((criterion) => ({
      expected_evidence: [],
      ...criterion,
    })),
    verifiers: [...(task.verifiers ?? [])],
    max_attempts: task.max_attempts ?? MAX_AUTONOMOUS_ATTEMPTS,
    requires_approval: task.requires_approval ?? false,
    approved: task.approved ?? false,
    mutating: task.mutating ?? false,
    authoritative_facts: [...(task.authoritative_facts ?? [])],
    forbidden_topics: [...(task.forbidden_topics ?? [])],
    forbidden_paths: [...(task.forbidden_paths ?? [])],
    forbidden_actions: [...(task.forbidden_actions ?? [])],
    task_type: task.task_type ?? "unknown",
    cost_or_escalation_band: task.cost_or_escalation_band ?? task.escalation_band ?? null,
    required_tools: [...(task.required_tools ?? [])],
    required_context_ids: [...(task.required_context_ids ?? [])],
    tool_profile: task.tool_profile ?? null,
    route_decision: null,
    route_started_at: null,
    routing_observation_recorded: false,
    status: "PENDING",
    attempt_id: null,
    request_id: null,
    correlation_id: null,
    shadow_task_id: null,
    shadow_advisory_attempt_id: null,
    attempt_count: 0,
    execution_retry_count: 0,
    drift_correction_count: 0,
    verification_retry_count: 0,
    evidence: [],
    verifier_results: [],
    latest_result: null,
    blocker: null,
    created_at: now,
    updated_at: now,
  };
}

function permissionForResources(lane) {
  const productionWrite = lane.resources.some(
    (resource) => resource.mode === "write" && /^(?:service|db|deployment):/i.test(resource.id)
  );
  return lane.requires_approval || (productionWrite && !lane.approved);
}

function decisionState(missionId, lane) {
  const state = createInitialState({
    mission_id: `${missionId}:${lane.lane_id}`,
    milestone: "Supervisor V2 lane",
    goal: lane.description,
    authoritative_facts: lane.authoritative_facts,
    current_task: {
      task_id: lane.task_id,
      description: lane.description,
      mutating: lane.mutating,
      scope: [lane.description],
    },
    acceptance_criteria: lane.acceptance_criteria,
    forbidden_topics: lane.forbidden_topics,
    forbidden_paths: lane.forbidden_paths,
    forbidden_actions: lane.forbidden_actions,
    max_attempts: lane.max_attempts,
  });
  return {
    ...state,
    attempt_count: lane.attempt_count,
    execution_retry_count: lane.execution_retry_count,
    drift_correction_count: lane.drift_correction_count,
    verification_retry_count: lane.verification_retry_count,
  };
}

function promptState(missionId, lane) {
  return decisionState(missionId, lane);
}

export class SupervisorScheduler {
  constructor({
    root,
    missionId,
    adapterFactory,
    lockManager,
    verifierHarness = null,
    maxConcurrency = 3,
    routing = null,
  }) {
    this.root = root;
    this.missionId = missionId;
    this.missionDir = path.join(root, missionId);
    this.statePath = path.join(this.missionDir, "dag-state.json");
    this.auditPath = path.join(this.missionDir, "audit.jsonl");
    this.adapterFactory = adapterFactory;
    this.lockManager = lockManager;
    this.verifierHarness = verifierHarness;
    this.maxConcurrency = maxConcurrency ?? 3;
    this.routing = routing;
    this.saveQueue = Promise.resolve();
  }

  async initialize(config) {
    validateTasks(config.tasks);
    const now = new Date().toISOString();
    const state = {
      schema_version: 2,
      mission_id: this.missionId,
      goal: config.goal,
      status: "PENDING",
      max_concurrency: config.max_concurrency ?? this.maxConcurrency ?? 3,
      lanes: Object.fromEntries(config.tasks.map((task) => [task.task_id, laneFromTask(task)])),
      metrics: {
        dispatch_count: 0,
        retries: 0,
        verifier_count: 0,
        evidence_cache_hits: 0,
        routing_decisions: 0,
      },
      blocker: null,
      created_at: now,
      updated_at: now,
    };
    await this.save(state);
    return state;
  }

  async load() {
    return JSON.parse(await readFile(this.statePath, "utf8"));
  }

  async save(state) {
    const snapshot = redactValue({ ...state, updated_at: new Date().toISOString() });
    this.saveQueue = this.saveQueue.then(async () => {
      await mkdir(this.missionDir, { recursive: true, mode: 0o700 });
      const temporary = `${this.statePath}.${process.pid}.${randomUUID()}.tmp`;
      const handle = await open(temporary, "wx", 0o600);
      try {
        await handle.writeFile(`${JSON.stringify(snapshot, null, 2)}\n`);
        await handle.sync();
      } finally {
        await handle.close();
      }
      await rename(temporary, this.statePath);
    });
    await this.saveQueue;
    return snapshot;
  }

  async audit(lane, decision, evidence = []) {
    await this.recordRoutingOutcome(lane, decision);
    return appendAuditEvent(this.auditPath, {
      mission_id: this.missionId,
      lane_id: lane.lane_id,
      task_id: lane.task_id,
      attempt_id: lane.attempt_id,
      correlation_id: lane.correlation_id,
      hermes_status: lane.latest_result?.status ?? null,
      supervisor_decision: decision,
      evidence,
      routing: lane.route_decision?.explanation ?? null,
      approval_stop_reason: lane.status === "WAITING_APPROVAL" ? lane.blocker : null,
    });
  }

  async prepareRoute(state, lane) {
    if (!this.routing?.enabled || lane.route_decision) return;
    const statistics = await this.routing.store.stats();
    state.metrics.routing_decisions = (state.metrics.routing_decisions ?? 0) + 1;
    lane.route_started_at = new Date().toISOString();
    lane.route_decision = routeTask({
      task_type: lane.task_type,
      candidates: this.routing.candidates,
      statistics,
      permission_decision: "AUTO_CONTINUE",
      conservative_fallback: this.routing.conservative_fallback,
      required_tools: lane.required_tools,
      required_context_ids: lane.required_context_ids,
      tool_profile: lane.tool_profile,
      minimum_samples: this.routing.minimum_samples ?? 5,
      exploration_interval: this.routing.exploration_interval ?? 10,
      exploration_sequence:
        (this.routing.exploration_sequence ?? 0) + state.metrics.routing_decisions,
      mutating: lane.mutating,
      requires_approval: lane.requires_approval,
      safe_read_only:
        !lane.mutating &&
        !lane.requires_approval &&
        lane.resources.every((resource) => resource.mode === "read"),
    });
  }

  enqueueShadowAdvisory(lane) {
    try {
      const dispatcher = this.routing?.shadowDispatcher;
      if (
        !dispatcher ||
        !lane.shadow_task_id ||
        !lane.attempt_id ||
        !lane.route_decision ||
        !lane.request_id ||
        !lane.correlation_id
      )
        return;
      const policyCandidateIds = new Set(
        (lane.route_decision.scored_candidates ?? []).map((candidate) => candidate.executor)
      );
      const candidates = this.routing.candidates
        .filter((candidate) => policyCandidateIds.has(candidate.executor))
        .map((candidate) => ({
          id: candidate.executor,
          model: candidate.model ?? null,
          provider: candidate.provider ?? null,
          tier: candidate.tier ?? null,
          cost_band: candidate.cost_band ?? null,
          estimated_cost: candidate.estimated_cost ?? null,
        }));
      const selected = candidates.find(
        (candidate) => candidate.id === lane.route_decision.selected.executor
      );
      if (!selected) return;
      const premiumSelection =
        /premium|high|escalat/i.test(`${selected.tier ?? ""} ${selected.cost_band ?? ""}`) ||
        (selected.estimated_cost ?? 0) > 0;
      const band = lane.cost_or_escalation_band
        ? String(lane.cost_or_escalation_band)
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, "-")
            .replace(/^-|-$/g, "")
            .slice(0, 60)
        : premiumSelection
          ? "premium-escalation"
          : "standard";
      const accepted = dispatcher.enqueue(
        {
          task_id: lane.shadow_task_id,
          attempt_id: lane.attempt_id,
          request_id: lane.request_id,
          correlation_id: lane.correlation_id,
          task_type: lane.task_type,
          candidates,
          metadata: {
            bounded_redacted_description: lane.description,
            required_tool_ids: lane.required_tools,
            cost_or_escalation_band: band,
          },
          schema_version: 1,
        },
        {
          authoritative_route: lane.route_decision.selected.executor,
          actual_executor: null,
          actual_model: null,
          actual_provider: null,
          retries: Math.max(0, lane.attempt_count - 1),
          final_success: null,
          outcome_state: "pending",
          agreement: "unknown",
          counterfactual_quality: "UNKNOWN",
        }
      );
      if (accepted) lane.shadow_advisory_attempt_id = lane.attempt_id;
    } catch {
      // Shadow telemetry is best effort and must not affect task execution.
    }
  }

  async recordRoutingOutcome(lane, decision) {
    this.recordShadowOutcome(lane, decision);
    if (!this.routing?.enabled || !lane.route_decision || lane.routing_observation_recorded) return;
    if (!TERMINAL.has(decision) || decision === "WAITING_APPROVAL") return;
    const completedAt = new Date();
    const startedAt = new Date(lane.route_started_at ?? lane.updated_at ?? lane.created_at);
    const verifierStatuses = lane.verifier_results.map((item) => item.status);
    await this.routing.store.append({
      task_id: lane.task_id,
      correlation_id: lane.correlation_id,
      task_type: lane.task_type,
      executor: lane.route_decision.selected.executor,
      model: lane.route_decision.selected.model ?? null,
      provider: lane.route_decision.selected.provider ?? null,
      started_at: startedAt.toISOString(),
      completed_at: completedAt.toISOString(),
      latency_ms: Math.max(0, completedAt.getTime() - startedAt.getTime()),
      input_tokens: null,
      output_tokens: null,
      estimated_cost: null,
      retries: Math.max(0, lane.attempt_count - 1),
      failure_reason: decision === "COMPLETE" ? null : (lane.blocker ?? decision),
      verifier_result: verifierStatuses.includes("FAIL")
        ? "FAIL"
        : verifierStatuses.includes("BLOCKED")
          ? "BLOCKED"
          : verifierStatuses.length
            ? "PASS"
            : null,
      acceptance_result: lane.acceptance_criteria.every((item) => item.status === "pass")
        ? "PASS"
        : "FAIL",
      success: decision === "COMPLETE",
      quality_score: null,
      context_bytes: null,
      tool_count: lane.required_tools.length || null,
      tool_profile: lane.tool_profile,
    });
    lane.routing_observation_recorded = true;
  }

  recordShadowOutcome(lane, decision) {
    const dispatcher = this.routing?.shadowDispatcher;
    if (!dispatcher || lane.shadow_advisory_attempt_id !== lane.attempt_id || !lane.attempt_id)
      return;
    const outcomeState =
      decision === "COMPLETE"
        ? "success"
        : decision === "FAILED"
          ? "failure"
          : decision === "WAITING_APPROVAL"
            ? "waiting_approval"
            : decision === "CANCELLED"
              ? "cancelled"
              : "unknown";
    try {
      dispatcher.updateRecord({
        task_id: lane.shadow_task_id,
        attempt_id: lane.attempt_id,
        retries: Math.max(0, lane.attempt_count - 1),
        outcome_state: outcomeState,
        final_success: ["success", "failure", "cancelled"].includes(outcomeState)
          ? outcomeState === "success"
          : null,
      });
    } catch {
      // Shadow telemetry is best effort and cannot affect task completion.
    }
  }

  updateDependencies(state) {
    for (const lane of Object.values(state.lanes)) {
      if (TERMINAL.has(lane.status) || UNCERTAIN.has(lane.status)) continue;
      const dependencies = lane.depends_on.map((id) => state.lanes[id]);
      if (
        dependencies.some((parent) =>
          ["FAILED", "BLOCKED", "WAITING_APPROVAL"].includes(parent.status)
        )
      ) {
        lane.status = "BLOCKED";
        lane.blocker = "Dependency did not complete";
      } else if (dependencies.every((parent) => parent.status === "COMPLETE")) {
        lane.status = "READY";
      } else {
        lane.status = "PENDING";
      }
    }
  }

  aggregate(state) {
    const lanes = Object.values(state.lanes);
    const statuses = lanes.map((lane) => lane.status);
    if (statuses.every((status) => status === "COMPLETE")) {
      state.status = "COMPLETE";
      state.blocker = null;
    } else if (statuses.some((status) => status === "WAITING_APPROVAL"))
      state.status = "WAITING_APPROVAL";
    else if (statuses.every((status) => TERMINAL.has(status))) {
      state.status = statuses.some((status) => status === "FAILED") ? "FAILED" : "BLOCKED";
    } else state.status = "RUNNING";
    if (["FAILED", "BLOCKED", "WAITING_APPROVAL"].includes(state.status)) {
      state.blocker = lanes
        .filter((lane) => lane.blocker)
        .map((lane) => `${lane.task_id}: ${lane.blocker}`)
        .join("; ");
    }
    return state;
  }

  async runVerifiers(state, lane) {
    lane.verifier_results = [];
    for (const spec of lane.verifiers) {
      if (!this.verifierHarness) {
        lane.verifier_results.push({
          verifier_id: spec.verifier_id,
          status: "BLOCKED",
          evidence: [],
          reason: "Verifier harness unavailable",
          retryable: false,
        });
        continue;
      }
      const verified = await this.verifierHarness.run(spec, { approved: lane.approved, lane });
      lane.verifier_results.push(verified);
      state.metrics.verifier_count += 1;
      if (verified.cache_hit) state.metrics.evidence_cache_hits += 1;
      lane.evidence.push(...(verified.evidence ?? []));
    }
    return lane.verifier_results.every((result) => result.status === "PASS");
  }

  async dispatchLane(state, lane) {
    if (permissionForResources(lane)) {
      lane.status = "WAITING_APPROVAL";
      lane.blocker = "Production or explicitly risky write requires approval";
      await this.save(state);
      await this.audit(lane, lane.status);
      return;
    }
    await this.prepareRoute(state, lane);
    while (lane.attempt_count < lane.max_attempts) {
      lane.attempt_count += 1;
      if (lane.attempt_count > 1) state.metrics.retries += 1;
      try {
        const identity = createAttemptIdentity(lane.task_id);
        lane.shadow_task_id = identity.task_id;
        lane.attempt_id = identity.attempt_id;
        lane.request_id = identity.request_id;
        lane.correlation_id = identity.correlation_id;
      } catch {
        // Unsupported logical IDs disable only the optional shadow join.
        lane.shadow_task_id = null;
        lane.attempt_id = randomUUID();
        lane.request_id = randomUUID();
        lane.correlation_id = lane.request_id;
      }
      lane.shadow_advisory_attempt_id = null;
      let lease;
      try {
        lease = await this.lockManager.acquire(lane.resources, {
          mission_id: this.missionId,
          lane_id: lane.lane_id,
          task_id: lane.task_id,
          attempt_id: lane.attempt_id,
        });
      } catch (error) {
        if (/resource conflict/i.test(error.message)) {
          lane.attempt_count -= 1;
          lane.attempt_id = null;
          lane.request_id = null;
          lane.correlation_id = null;
          lane.shadow_task_id = null;
          return "LOCK_CONFLICT";
        }
        lane.status = "BLOCKED";
        lane.blocker = error.message;
        return;
      }
      try {
        lane.status = "RUNNING";
        lane.updated_at = new Date().toISOString();
        state.metrics.dispatch_count += 1;
        await this.save(state);
        this.enqueueShadowAdvisory(lane);
        const adapter = this.adapterFactory(lane, lane.route_decision?.selected ?? null);
        let raw;
        try {
          const handle = await adapter.send_task({
            prompt: formatHermesPrompt(promptState(this.missionId, lane)),
            description: lane.description,
            task_id: lane.shadow_task_id ?? lane.task_id,
            attempt_id: lane.attempt_id,
            request_id: lane.request_id,
            correlation_id: lane.correlation_id,
            mission_id: this.missionId,
            mutating: lane.mutating,
            permission_decision: "AUTO_CONTINUE",
          });
          raw = await adapter.wait_for_result(handle);
        } catch (error) {
          lane.execution_retry_count += 1;
          if (lane.mutating) {
            lane.status = "WAITING_APPROVAL";
            lane.blocker = "Mutating lane result is uncertain; verify before resend";
            await this.audit(lane, lane.status);
            return;
          }
          if (lane.attempt_count >= lane.max_attempts) {
            lane.status = "BLOCKED";
            lane.blocker = "Maximum autonomous attempts reached after adapter failure";
            this.recordShadowOutcome(lane, "BLOCKED");
            await this.audit(lane, lane.status);
            return;
          }
          this.recordShadowOutcome(lane, "UNKNOWN");
          continue;
        }
        lane.status = "VERIFYING";
        let result;
        try {
          result = parseHermesResult(raw);
        } catch {
          lane.verification_retry_count += 1;
          if (lane.attempt_count >= lane.max_attempts) {
            lane.status = "BLOCKED";
            lane.blocker = "Maximum autonomous attempts reached after contract failure";
            this.recordShadowOutcome(lane, "BLOCKED");
            await this.audit(lane, lane.status);
            return;
          }
          this.recordShadowOutcome(lane, "UNKNOWN");
          continue;
        }
        lane.latest_result = result;
        lane.evidence = [...result.evidence];
        const decision = decideResult(decisionState(this.missionId, lane), result);
        if (decision.acceptance) lane.acceptance_criteria = decision.acceptance.criteria;
        if (decision.permission?.decision) lane.permission_decision = decision.permission.decision;
        if (decision.action === "COMPLETE") {
          if (await this.runVerifiers(state, lane)) {
            lane.status = "COMPLETE";
            lane.blocker = null;
          } else {
            lane.status = "FAILED";
            lane.blocker = "Verifier did not pass";
          }
          await this.audit(lane, lane.status, lane.evidence);
          return;
        }
        if (decision.action === "WAITING_APPROVAL") {
          lane.status = "WAITING_APPROVAL";
          lane.blocker = decision.reason;
          await this.audit(lane, lane.status, lane.evidence);
          return;
        }
        if (decision.action === "BLOCKED") {
          lane.status = result.status === "FAILED" ? "FAILED" : "BLOCKED";
          lane.blocker = decision.reason;
          await this.audit(lane, lane.status, lane.evidence);
          return;
        }
        if (decision.action === "CORRECT") lane.drift_correction_count += 1;
        else lane.verification_retry_count += 1;
        if (lane.attempt_count >= lane.max_attempts) {
          lane.status = "BLOCKED";
          lane.blocker = "Maximum autonomous attempts reached";
          this.recordShadowOutcome(lane, decision.action === "CORRECT" ? "FAILED" : "BLOCKED");
          await this.audit(lane, lane.status, lane.evidence);
          return;
        }
        this.recordShadowOutcome(lane, decision.action === "CORRECT" ? "FAILED" : "UNKNOWN");
      } finally {
        await this.lockManager.release(lease);
      }
    }
  }

  async runUnlocked(state) {
    const running = new Map();
    const lockBlocked = new Set();
    while (true) {
      this.updateDependencies(state);
      const candidates = Object.values(state.lanes).filter(
        (lane) =>
          lane.status === "READY" && !running.has(lane.lane_id) && !lockBlocked.has(lane.lane_id)
      );
      let admitted = false;
      for (const lane of candidates) {
        if (running.size >= state.max_concurrency) break;
        if (!lane.can_parallelize && running.size) continue;
        if ([...running.values()].some((entry) => !entry.lane.can_parallelize)) continue;
        const promise = this.dispatchLane(state, lane)
          .then((result) => ({ lane, result }))
          .finally(() => running.delete(lane.lane_id));
        running.set(lane.lane_id, { lane, promise });
        admitted = true;
        await new Promise((resolve) => setImmediate(resolve));
      }
      if (running.size) {
        const settled = await Promise.race([...running.values()].map((entry) => entry.promise));
        if (settled.result === "LOCK_CONFLICT") lockBlocked.add(settled.lane.lane_id);
        else lockBlocked.clear();
        continue;
      }
      this.updateDependencies(state);
      if (
        (!admitted || lockBlocked.size) &&
        Object.values(state.lanes).some((lane) => lane.status === "READY")
      ) {
        for (const lane of Object.values(state.lanes).filter((item) => item.status === "READY")) {
          lane.status = "BLOCKED";
          lane.blocker = "Required resource remains locked";
        }
      }
      break;
    }
    this.aggregate(state);
    await this.save(state);
    return state;
  }

  async run() {
    const missionLock = await acquireMissionLock(this.root, this.missionId);
    try {
      const state = await this.load();
      if (TERMINAL.has(state.status)) return state;
      return this.runUnlocked(state);
    } finally {
      await missionLock.release();
    }
  }

  async recover() {
    const missionLock = await acquireMissionLock(this.root, this.missionId);
    try {
      const state = await this.load();
      for (const lane of Object.values(state.lanes).filter((item) => UNCERTAIN.has(item.status))) {
        if (!lane.mutating) {
          lane.status = "READY";
          lane.blocker = null;
          continue;
        }
        lane.status = "VERIFYING";
        const passed = lane.verifiers.length > 0 && (await this.runVerifiers(state, lane));
        if (passed) {
          lane.status = "COMPLETE";
          lane.blocker = null;
        } else {
          lane.status = "WAITING_APPROVAL";
          lane.blocker = "Uncertain mutating lane could not be proven complete; no replay";
        }
        await this.audit(lane, lane.status, lane.evidence);
      }
      await this.save(state);
      return this.runUnlocked(state);
    } finally {
      await missionLock.release();
    }
  }
}
