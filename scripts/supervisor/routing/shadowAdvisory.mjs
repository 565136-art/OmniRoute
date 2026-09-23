import { createHash, randomUUID } from "node:crypto";
import { redactValue } from "../redaction.mjs";

export { DurableShadowSpool, SshShadowSpoolClient } from "./shadowSpool.mjs";

export const SHADOW_SCHEMA_VERSION = 1;
export const SHADOW_RECORD_MAX_BYTES = 8_192;
export const SHADOW_ID_MAX_LENGTH = 128;
export const SHADOW_AGREEMENT = Object.freeze(["agree", "disagree", "unknown"]);
export const SHADOW_OUTCOME_STATES = Object.freeze([
  "pending",
  "waiting_approval",
  "success",
  "failure",
  "cancelled",
  "unknown",
]);
export const SHADOW_COUNTERFACTUAL_QUALITY = Object.freeze([
  "UNKNOWN",
  "SAME_QUALITY",
  "QUALITY_IMPROVEMENT",
  "QUALITY_REGRESSION",
  "FAILURE",
]);

const RECORD_FIELDS = Object.freeze([
  "schema_version",
  "task_id",
  "attempt_id",
  "task_type",
  "request_id",
  "correlation_id",
  "authoritative_route",
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
]);

const NULLABLE_STRINGS = new Map([
  ["request_id", 128],
  ["correlation_id", 128],
  ["actual_executor", 128],
  ["actual_model", 256],
  ["actual_provider", 256],
  ["laya_selected", 128],
  ["laya_fallback_reason", 256],
  ["laya_error_category", 256],
  ["laya_adapter_version", 256],
]);
const NUMERIC_NULLABLE = new Set([
  "laya_confidence",
  "laya_latency_ms",
  "laya_peak_rss_bytes",
  "input_tokens",
  "output_tokens",
  "cached_tokens",
  "cost",
  "retries",
]);
const BOOLEAN_NULLABLE = new Set(["laya_abstain", "laya_fallback", "final_success"]);
const MAX_CANDIDATES = 20;
const MAX_TOOLS = 50;
const MAX_DESCRIPTION = 600;
const MAX_JOB_INPUT_BYTES = 8_192;
const SKIP_TASK = /(?:health|metadata|trivial|tiny|heartbeat|status[-_ ]check)/i;
const URGENT = /(?:urgent|asap|time[-_ ]critical|latency[-_ ]sensitive|immediate|emergency)/i;
const ADAPTER_VERSION = "cacf798f3b19241cc02722827f9e030ca11eed70";

function byteSize(value) {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}

function text(value, field, max) {
  if (
    typeof value !== "string" ||
    !value.length ||
    value.length > max ||
    /[\u0000-\u001f]/.test(value)
  ) {
    throw new TypeError(`Invalid shadow ${field}`);
  }
  return redactValue(value);
}

const SAFE_TOKEN = /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/;
const SAFE_LABEL = /^[A-Za-z0-9_.:/-]+$/;
const SECRET_LABEL =
  /(?:bearer\s+\S+|(?:api[_-]?key|access[_-]?token|password)\s*[:=]\s*\S+|sk-[a-z0-9]{16,})/i;
function token(value, field, max = SHADOW_ID_MAX_LENGTH) {
  const result = text(value, field, max);
  if (!SAFE_TOKEN.test(result)) throw new TypeError(`Invalid shadow ${field}`);
  return result;
}

function label(value, field, max = 256) {
  const result = text(value, field, max);
  if (!SAFE_LABEL.test(result) || SECRET_LABEL.test(result))
    throw new TypeError(`Invalid shadow ${field}`);
  return result;
}

function nullableNumber(value, field, { integer = false, max = Number.MAX_SAFE_INTEGER } = {}) {
  if (value === null) return null;
  if (
    typeof value !== "number" ||
    !Number.isFinite(value) ||
    value < 0 ||
    value > max ||
    (integer && !Number.isSafeInteger(value))
  )
    throw new TypeError(`Invalid shadow ${field}`);
  return value;
}

function isIsoDate(value) {
  return (
    typeof value === "string" &&
    value.length <= 64 &&
    /(?:Z|[+-]\d{2}:\d{2})$/.test(value) &&
    Number.isFinite(Date.parse(value))
  );
}

function emptyRecord({
  task_id,
  attempt_id,
  task_type,
  authoritative_route,
  request_id = null,
  correlation_id = null,
  created_at = null,
}) {
  const now = new Date().toISOString();
  return {
    schema_version: SHADOW_SCHEMA_VERSION,
    task_id,
    attempt_id,
    task_type,
    request_id,
    correlation_id,
    authoritative_route,
    actual_executor: null,
    actual_model: null,
    actual_provider: null,
    laya_selected: null,
    laya_abstain: null,
    laya_confidence: null,
    laya_latency_ms: null,
    laya_peak_rss_bytes: null,
    laya_fallback: null,
    laya_fallback_reason: null,
    laya_error_category: null,
    laya_adapter_version: null,
    input_tokens: null,
    output_tokens: null,
    cached_tokens: null,
    cost: null,
    retries: null,
    final_success: null,
    outcome_state: "pending",
    agreement: "unknown",
    counterfactual_quality: "UNKNOWN",
    created_at: created_at ?? now,
    updated_at: now,
  };
}

export function createAttemptIdentity(taskId, { correlationId = randomUUID() } = {}) {
  const correlation = token(correlationId, "correlation ID");
  return {
    task_id: token(taskId, "task ID"),
    attempt_id: randomUUID(),
    // Task Observer's request_id is the actual call_logs correlation identity.
    request_id: correlation,
    correlation_id: correlation,
  };
}

export function createShadowRecord(value) {
  const now = new Date().toISOString();
  const seed = emptyRecord({
    task_id: token(value?.task_id, "task ID"),
    attempt_id: token(value?.attempt_id, "attempt ID"),
    task_type: label(value?.task_type, "task type"),
    authoritative_route: token(value?.authoritative_route, "authoritative route"),
    request_id: value?.request_id == null ? null : token(value.request_id, "request ID"),
    correlation_id:
      value?.correlation_id == null ? null : token(value.correlation_id, "correlation ID"),
    created_at: value?.created_at ?? now,
  });
  return validateShadowRecord({
    ...seed,
    ...value,
    schema_version: SHADOW_SCHEMA_VERSION,
    updated_at: value?.updated_at ?? now,
  });
}

export function validateShadowRecord(value) {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("Invalid shadow record");
  if (Object.keys(value).some((key) => !RECORD_FIELDS.includes(key)))
    throw new TypeError("Unexpected shadow record field");
  if (RECORD_FIELDS.some((key) => !Object.hasOwn(value, key)))
    throw new TypeError("Incomplete shadow record");
  if (value.schema_version !== SHADOW_SCHEMA_VERSION)
    throw new TypeError("Unsupported shadow record schema");

  const record = { ...value };
  record.task_id = token(record.task_id, "task ID");
  record.attempt_id = token(record.attempt_id, "attempt ID");
  record.task_type = label(record.task_type, "task type");
  record.request_id = record.request_id === null ? null : token(record.request_id, "request ID");
  record.correlation_id =
    record.correlation_id === null ? null : token(record.correlation_id, "correlation ID");
  record.authoritative_route =
    record.authoritative_route === null
      ? null
      : token(record.authoritative_route, "authoritative route");
  for (const [key, max] of NULLABLE_STRINGS) {
    record[key] =
      record[key] === null
        ? null
        : [
              "actual_model",
              "actual_provider",
              "laya_fallback_reason",
              "laya_error_category",
              "laya_adapter_version",
            ].includes(key)
          ? label(record[key], key, max)
          : token(record[key], key, max);
  }
  for (const key of BOOLEAN_NULLABLE) {
    if (record[key] !== null && typeof record[key] !== "boolean")
      throw new TypeError(`Invalid shadow ${key}`);
  }
  for (const key of NUMERIC_NULLABLE) {
    record[key] = nullableNumber(record[key], key, {
      integer: [
        "laya_latency_ms",
        "laya_peak_rss_bytes",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "retries",
      ].includes(key),
      max: {
        laya_confidence: 1,
        laya_latency_ms: 86_400_000,
        laya_peak_rss_bytes: 1_099_511_627_776,
        input_tokens: 1_000_000_000,
        output_tokens: 1_000_000_000,
        cached_tokens: 1_000_000_000,
        cost: 1_000_000_000,
        retries: 1_000,
      }[key],
    });
  }
  if (!SHADOW_OUTCOME_STATES.includes(record.outcome_state))
    throw new TypeError("Invalid shadow outcome state");
  if (!SHADOW_AGREEMENT.includes(record.agreement)) throw new TypeError("Invalid shadow agreement");
  if (!SHADOW_COUNTERFACTUAL_QUALITY.includes(record.counterfactual_quality))
    throw new TypeError("Invalid counterfactual quality");
  if (!isIsoDate(record.created_at) || !isIsoDate(record.updated_at))
    throw new TypeError("Invalid shadow timestamp");
  if (
    (record.laya_selected === null && record.laya_abstain === false) ||
    (record.laya_selected !== null && record.laya_abstain !== false)
  ) {
    throw new TypeError("Shadow abstention flag and selection disagree");
  }
  if (record.outcome_state === "success" && record.final_success !== true)
    throw new TypeError("Successful outcome requires final_success=true");
  if (["failure", "cancelled"].includes(record.outcome_state) && record.final_success !== false)
    throw new TypeError("Failed or cancelled outcome requires final_success=false");
  if (
    ["pending", "waiting_approval", "unknown"].includes(record.outcome_state) &&
    record.final_success !== null
  )
    throw new TypeError("Nonterminal outcome cannot set final_success");
  const expectedAgreement =
    record.laya_selected == null || record.authoritative_route == null
      ? "unknown"
      : record.laya_selected === record.authoritative_route
        ? "agree"
        : "disagree";
  if (record.agreement !== expectedAgreement)
    throw new TypeError("Shadow agreement does not match route and advice");
  if (byteSize(record) > SHADOW_RECORD_MAX_BYTES)
    throw new TypeError("Shadow record exceeds size limit");
  return record;
}

const KNOWN_SENTINELS = new Set([null, undefined, "unknown", "UNKNOWN"]);
const TERMINAL_STATES = new Set(["success", "failure", "cancelled"]);

export function mergeShadowRecord(currentValue, patchValue) {
  const current = validateShadowRecord(currentValue);
  if (!patchValue || typeof patchValue !== "object" || Array.isArray(patchValue))
    throw new TypeError("Invalid shadow patch");
  const patch = { ...patchValue };
  if (Object.keys(patch).some((key) => !RECORD_FIELDS.includes(key)))
    throw new TypeError("Unexpected shadow patch field");
  for (const key of [
    "schema_version",
    "task_id",
    "attempt_id",
    "task_type",
    "request_id",
    "correlation_id",
    "authoritative_route",
    "created_at",
  ]) {
    if (Object.hasOwn(patch, key) && patch[key] !== current[key])
      throw new TypeError(`Shadow identity changed: ${key}`);
  }
  const merged = { ...current };
  let changed = false;
  for (const [key, value] of Object.entries(patch)) {
    if (
      [
        "schema_version",
        "task_id",
        "attempt_id",
        "task_type",
        "request_id",
        "correlation_id",
        "authoritative_route",
        "created_at",
        "updated_at",
        "agreement",
      ].includes(key)
    )
      continue;
    if (key === "outcome_state") {
      const canAdvance =
        current.outcome_state === "pending" ||
        (["waiting_approval", "unknown"].includes(current.outcome_state) &&
          TERMINAL_STATES.has(value)) ||
        (current.outcome_state === "unknown" && value === "unknown");
      if (!canAdvance) continue;
      if (merged[key] !== value) {
        merged[key] = value;
        changed = true;
      }
      continue;
    }
    if (KNOWN_SENTINELS.has(value) && !KNOWN_SENTINELS.has(current[key])) continue;
    if (key === "retries" && typeof current[key] === "number" && typeof value === "number") {
      merged[key] = Math.max(current[key], value);
    } else if (KNOWN_SENTINELS.has(current[key]) || current[key] === value) {
      merged[key] = value;
    }
    if (merged[key] !== current[key]) changed = true;
  }
  if (merged.laya_selected == null || merged.authoritative_route == null)
    merged.agreement = "unknown";
  else
    merged.agreement = merged.laya_selected === merged.authoritative_route ? "agree" : "disagree";
  if (["pending", "waiting_approval", "unknown"].includes(merged.outcome_state))
    merged.final_success = null;
  else merged.final_success = merged.outcome_state === "success";
  if (merged.agreement !== current.agreement || merged.final_success !== current.final_success)
    changed = true;
  if (changed) merged.updated_at = new Date().toISOString();
  return validateShadowRecord(merged);
}

function boundedJobString(value, name, max) {
  return text(value, name, max);
}

export function normalizeShadowInput(value) {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("Invalid shadow input");
  const allowed = new Set([
    "task_id",
    "attempt_id",
    "request_id",
    "correlation_id",
    "task_type",
    "candidates",
    "metadata",
    "schema_version",
  ]);
  if (Object.keys(value).some((key) => !allowed.has(key)))
    throw new TypeError("Unexpected shadow input field");
  if (value.schema_version !== SHADOW_SCHEMA_VERSION)
    throw new TypeError("Unsupported shadow input schema");
  if (
    !Array.isArray(value.candidates) ||
    !value.candidates.length ||
    value.candidates.length > MAX_CANDIDATES
  )
    throw new TypeError("Invalid shadow candidate set");
  const candidates = value.candidates.map((candidate) => {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate))
      throw new TypeError("Invalid shadow candidate");
    const allowedCandidate = new Set([
      "id",
      "model",
      "provider",
      "tier",
      "cost_band",
      "estimated_cost",
    ]);
    if (Object.keys(candidate).some((key) => !allowedCandidate.has(key)))
      throw new TypeError("Unexpected shadow candidate field");
    const estimated_cost =
      candidate.estimated_cost == null
        ? null
        : nullableNumber(candidate.estimated_cost, "candidate cost", { max: 1_000_000_000 });
    return {
      id: token(candidate.id, "candidate ID"),
      model: candidate.model == null ? null : label(candidate.model, "candidate model"),
      provider: candidate.provider == null ? null : label(candidate.provider, "candidate provider"),
      tier: candidate.tier == null ? null : token(candidate.tier, "candidate tier", 40),
      cost_band:
        candidate.cost_band == null ? null : token(candidate.cost_band, "candidate cost band", 40),
      estimated_cost,
    };
  });
  if (new Set(candidates.map((candidate) => candidate.id)).size !== candidates.length)
    throw new TypeError("Duplicate shadow candidate ID");
  const metadata = value.metadata;
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata))
    throw new TypeError("Invalid shadow metadata");
  if (
    Object.keys(metadata).some(
      (key) =>
        !["bounded_redacted_description", "required_tool_ids", "cost_or_escalation_band"].includes(
          key
        )
    )
  )
    throw new TypeError("Unexpected shadow metadata field");
  if (!Array.isArray(metadata.required_tool_ids) || metadata.required_tool_ids.length > MAX_TOOLS)
    throw new TypeError("Invalid shadow required tools");
  const normalized = {
    task_id: token(value.task_id, "task ID"),
    attempt_id: token(value.attempt_id, "attempt ID"),
    request_id: token(value.request_id, "request ID"),
    correlation_id: token(value.correlation_id, "correlation ID"),
    task_type: label(value.task_type, "task type"),
    candidates,
    metadata: {
      bounded_redacted_description: boundedJobString(
        metadata.bounded_redacted_description,
        "description",
        MAX_DESCRIPTION
      ),
      required_tool_ids: metadata.required_tool_ids.map((item) => token(item, "tool ID")),
      cost_or_escalation_band: token(metadata.cost_or_escalation_band, "escalation band", 60),
    },
    schema_version: SHADOW_SCHEMA_VERSION,
  };
  if (
    normalized.request_id !== normalized.correlation_id ||
    normalized.correlation_id === normalized.attempt_id
  ) {
    throw new TypeError("Shadow request and call-log correlation identity mismatch");
  }
  if (byteSize(normalized) > MAX_JOB_INPUT_BYTES)
    throw new TypeError("Shadow input exceeds size limit");
  return normalized;
}

export function isShadowEligible(value) {
  try {
    const input = normalizeShadowInput(value);
    const textValue = `${input.task_type} ${input.metadata.bounded_redacted_description}`;
    if (
      SKIP_TASK.test(textValue) ||
      URGENT.test(textValue) ||
      input.metadata.bounded_redacted_description.trim().length < 32
    )
      return false;
    if (input.candidates.length < 2) return false;
    return /premium|escalat|high[-_ ]cost/i.test(input.metadata.cost_or_escalation_band);
  } catch {
    return false;
  }
}

export function shadowSample(input, rate) {
  if (!Number.isFinite(rate) || rate <= 0) return false;
  if (rate >= 1) return true;
  const fraction =
    createHash("sha256").update(`${input.task_id}\0${input.attempt_id}`).digest().readUInt32BE(0) /
    0x1_0000_0000;
  return fraction < rate;
}

export function normalizeShadowOutput(inputValue, value) {
  const input = normalizeShadowInput(inputValue);
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new TypeError("Invalid shadow output");
  const allowed = new Set([
    "task_id",
    "attempt_id",
    "selected",
    "abstain",
    "confidence",
    "latency_ms",
    "peak_rss_bytes",
    "fallback",
    "fallback_reason",
    "error_category",
    "adapter_version",
  ]);
  if (Object.keys(value).some((key) => !allowed.has(key)))
    throw new TypeError("Unexpected shadow output field");
  if (value.task_id !== input.task_id || value.attempt_id !== input.attempt_id)
    throw new TypeError("Shadow output identity mismatch");
  if (
    value.selected !== null &&
    !input.candidates.some((candidate) => candidate.id === value.selected)
  )
    throw new TypeError("Shadow recommendation is not a supplied candidate");
  if (typeof value.abstain !== "boolean" || typeof value.fallback !== "boolean")
    throw new TypeError("Invalid shadow result flags");
  const confidence = nullableNumber(value.confidence, "confidence", { max: 1 });
  const unroundedLatency =
    value.latency_ms == null
      ? null
      : nullableNumber(value.latency_ms, "latency", { max: 86_400_000 });
  const latency = unroundedLatency === null ? null : Math.round(unroundedLatency);
  const latency_ms = nullableNumber(latency, "latency", { integer: true, max: 86_400_000 });
  const peak_rss_bytes = nullableNumber(value.peak_rss_bytes, "RSS", {
    integer: true,
    max: 1_099_511_627_776,
  });
  if (
    (value.abstain && value.selected !== null) ||
    (!value.abstain && value.selected === null) ||
    (value.fallback && value.selected !== null)
  ) {
    throw new TypeError("Shadow result flags disagree with selection");
  }
  return {
    task_id: input.task_id,
    attempt_id: input.attempt_id,
    laya_selected: value.selected,
    laya_abstain: value.abstain,
    laya_confidence: confidence,
    laya_latency_ms: latency_ms,
    laya_peak_rss_bytes: peak_rss_bytes,
    laya_fallback: value.fallback,
    laya_fallback_reason:
      value.fallback_reason == null
        ? null
        : safeLabel(value.fallback_reason, "fallback reason", 256),
    laya_error_category:
      value.error_category == null ? null : label(value.error_category, "error category"),
    laya_adapter_version:
      value.adapter_version == null
        ? ADAPTER_VERSION
        : label(value.adapter_version, "adapter version"),
  };
}

function safeLabel(value, field, max) {
  if (typeof value !== "string") throw new TypeError(`Invalid shadow ${field}`);
  const redacted = redactValue(value)
    .trim()
    .replace(/[^A-Za-z0-9._:/-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return label(redacted, field, max);
}

export class ShadowAdvisoryDispatcher {
  constructor({ enabled = false, sampleRate = 0.05, maxQueue = 100, transport = null } = {}) {
    if (!Number.isFinite(sampleRate) || sampleRate < 0 || sampleRate > 1)
      throw new TypeError("Invalid shadow sample rate");
    if (!Number.isInteger(maxQueue) || maxQueue < 1 || maxQueue > 100)
      throw new TypeError("Invalid shadow queue limit");
    this.enabled = enabled === true;
    this.sampleRate = sampleRate;
    this.concurrency = 1;
    this.maxQueue = maxQueue;
    this.transport = transport;
    this.writes = new Map();
  }

  enqueue(value, initialRecord = {}) {
    if (!this.enabled || !this.transport || !isShadowEligible(value)) return false;
    let input;
    let record;
    try {
      input = normalizeShadowInput(value);
      record = createShadowRecord({
        ...initialRecord,
        task_id: input.task_id,
        attempt_id: input.attempt_id,
        request_id: input.request_id,
        correlation_id: input.correlation_id,
        task_type: input.task_type,
        authoritative_route: initialRecord.authoritative_route,
      });
    } catch {
      return false;
    }
    if (!shadowSample(input, this.sampleRate)) return false;
    // The only work here is a scheduled durable-spool write. It never invokes Laya and never delays routeTask.
    const key = `${input.task_id}\0${input.attempt_id}`;
    if (!this.writes.has(key) && this.writes.size >= this.maxQueue) return false;
    const previous = this.writes.get(key) ?? Promise.resolve();
    const write = previous.then(() => this.transport.enqueue(input, record)).catch(() => false);
    this.trackWrite(key, write);
    return true;
  }

  updateRecord(patch) {
    if (!this.enabled || !this.transport || typeof this.transport.updateRecord !== "function")
      return false;
    const key = `${patch?.task_id ?? ""}\0${patch?.attempt_id ?? ""}`;
    if (!this.writes.has(key) && this.writes.size >= this.maxQueue) return false;
    const previous = this.writes.get(key) ?? Promise.resolve();
    const write = previous.then(() => this.transport.updateRecord(patch)).catch(() => false);
    this.trackWrite(key, write);
    return true;
  }

  trackWrite(key, promise) {
    this.writes.set(key, promise);
    promise
      .finally(() => {
        if (this.writes.get(key) === promise) this.writes.delete(key);
      })
      .catch(() => {});
  }
}

export function makeMissingShadowAdvice(input, category) {
  return {
    task_id: input.task_id,
    attempt_id: input.attempt_id,
    laya_selected: null,
    laya_abstain: true,
    laya_confidence: null,
    laya_latency_ms: null,
    laya_peak_rss_bytes: null,
    laya_fallback: true,
    laya_fallback_reason: category,
    laya_error_category: category,
    laya_adapter_version: ADAPTER_VERSION,
  };
}
