import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";

import { EndpointAdmissionGate } from "../endpointAdmissionGate.mjs";

const execFileAsync = promisify(execFile);
const CONTRACT_FIELDS = [
  "STATUS",
  "TASK",
  "EVIDENCE",
  "CHANGES",
  "TESTS",
  "BLOCKER",
  "NEXT_ACTION",
  "REQUIRES_APPROVAL",
];
const MAX_TRANSPORT_EVIDENCE_BYTES = 8_192;

function hasCompleteContract(output) {
  const trimmed = output.trim();
  if (trimmed.startsWith("{")) {
    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch {
      throw new Error("Hermes transport returned malformed JSON");
    }
    return [
      "status",
      "task",
      "evidence",
      "changes",
      "tests",
      "blocker",
      "next_action",
      "requires_approval",
    ].every((field) => Object.hasOwn(parsed, field));
  }
  const fields = new Set(
    trimmed
      .split(/\r?\n/)
      .map((line) => /^([A-Z_]+)=/.exec(line)?.[1])
      .filter(Boolean)
  );
  return CONTRACT_FIELDS.every((field) => fields.has(field));
}

function parseStreamJson(output, { requireTerminal = true } = {}) {
  const lines = output.trim().split(/\r?\n/);
  if (lines.length < 2) return null;
  let events;
  try {
    events = lines.map((line) => JSON.parse(line));
  } catch {
    return null;
  }
  if (!events.every((event) => event && typeof event === "object" && event.type)) return null;
  if (!events.some((event) => event.type === "system" && event.subtype === "init")) return null;
  if (requireTerminal && !events.some((event) => event.type === "result")) return null;
  return events;
}

function terminalEvidence(events, { includeMetadata = false } = {}) {
  const evidence = [];
  for (const event of events) {
    if (event.type === "tool_use" && event.name === "terminal") {
      const command = typeof event.input?.command === "string" ? event.input.command : "";
      if (command) {
        evidence.push({
          type: "command_output",
          source: "hermes-local terminal command",
          summary: command.slice(0, MAX_TRANSPORT_EVIDENCE_BYTES),
        });
      }
    }
    if (event.type === "tool_result" && event.name === "terminal") {
      let output = typeof event.output === "string" ? event.output : "";
      let error = event.is_error ? output : "";
      let exitCode = event.is_error ? null : 0;
      try {
        const result = JSON.parse(output);
        if (result && typeof result === "object") {
          output = typeof result.output === "string" ? result.output : "";
          error = typeof result.error === "string" ? result.error : "";
          exitCode = result.exit_code ?? null;
        }
      } catch {
        // Non-JSON output remains direct transport evidence.
      }
      if (output) {
        evidence.push({
          type: "command_output",
          source: "hermes-local terminal stdout",
          summary: Buffer.from(output, "utf8")
            .subarray(0, MAX_TRANSPORT_EVIDENCE_BYTES)
            .toString("utf8"),
        });
      }
      if (error) {
        evidence.push({
          type: "command_output",
          source: "hermes-local terminal stderr",
          summary: Buffer.from(error, "utf8")
            .subarray(0, MAX_TRANSPORT_EVIDENCE_BYTES)
            .toString("utf8"),
        });
      }
      if (includeMetadata) {
        evidence.push({
          type: "other",
          source: "hermes-local terminal metadata",
          summary: JSON.stringify({
            tool_call_id: event.tool_call_id ?? null,
            exit_code: exitCode,
            is_error: Boolean(event.is_error),
          }),
        });
      }
    }
  }
  return evidence;
}

function normalizeStreamJson(events, task) {
  const terminal = events.findLast((event) => event.type === "result");
  if (!terminal) throw new Error("Hermes stream-json transport omitted its terminal result");
  if (terminal.exit_code !== 0 || terminal.error) {
    throw new Error("Hermes stream-json transport reported failure");
  }
  const evidence = terminalEvidence(events);
  if (typeof terminal.text === "string" && terminal.text) {
    evidence.push({
      type: "other",
      source: "hermes-local final response",
      summary: Buffer.from(terminal.text, "utf8")
        .subarray(0, MAX_TRANSPORT_EVIDENCE_BYTES)
        .toString("utf8"),
    });
  }
  if (!evidence.length) throw new Error("Hermes stream-json transport returned no evidence");
  return JSON.stringify({
    status: "TRANSPORT_COMPLETE",
    task: task.description ?? task.prompt,
    evidence,
    changes: [],
    tests: ["NOT_RUN"],
    blocker: null,
    next_action: "Supervisor must evaluate transport evidence",
    requires_approval: task.mutating || task.permission_decision !== "AUTO_CONTINUE",
  });
}

function normalizePartialStreamJson(output, task, reason) {
  const events = parseStreamJson(output, { requireTerminal: false });
  if (!events) return null;
  const evidence = terminalEvidence(events, { includeMetadata: true });
  if (!evidence.some((item) => item.source === "hermes-local terminal stdout")) return null;
  return JSON.stringify({
    status: "TRANSPORT_PARTIAL",
    task: task.description ?? task.prompt,
    evidence,
    changes: [],
    tests: ["NOT_RUN"],
    blocker: reason,
    next_action: "Supervisor must evaluate preserved partial evidence; do not infer completion",
    requires_approval: task.mutating || task.permission_decision !== "AUTO_CONTINUE",
  });
}

export function normalizeHermesTransportResult(output, task) {
  if (typeof output !== "string" || !output.trim()) {
    throw new Error("Hermes transport returned empty output");
  }
  if (/\0|[\u0001-\u0008\u000b\u000c\u000e-\u001f]/.test(output)) {
    throw new Error("Hermes transport returned malformed output");
  }
  const streamEvents = parseStreamJson(output);
  if (streamEvents) return normalizeStreamJson(streamEvents, task);
  if (hasCompleteContract(output)) return output;
  const bounded = Buffer.from(output.trim(), "utf8")
    .subarray(0, MAX_TRANSPORT_EVIDENCE_BYTES)
    .toString("utf8");
  return JSON.stringify({
    status: "TRANSPORT_COMPLETE",
    task: task.description ?? task.prompt,
    evidence: [
      {
        type: "command_output",
        source: "hermes-local stdout",
        summary: bounded,
      },
    ],
    changes: [],
    tests: ["NOT_RUN"],
    blocker: null,
    next_action: "Supervisor must evaluate transport evidence",
    requires_approval: task.mutating || task.permission_decision !== "AUTO_CONTINUE",
  });
}

function boundedSource(task) {
  const sourcePart = (value) => {
    const raw = String(value ?? "unlinked");
    return raw.length <= 128 && /^[A-Za-z0-9][A-Za-z0-9._:/-]*$/.test(raw)
      ? raw
      : createHash("sha256").update(raw).digest("hex");
  };
  const value = `codex-supervisor:${sourcePart(task.task_id)}:${sourcePart(task.attempt_id)}:${sourcePart(task.request_id ?? task.correlation_id ?? "unlinked")}`;
  if (!/^[A-Za-z0-9:._/-]+$/.test(value) || value.length > 600) {
    throw new Error("Hermes task identity contains unsupported characters or is too long");
  }
  return value;
}

export function hermesIdentityEnvironment(task, input = process.env) {
  const environment = { ...input };
  for (const name of [
    "JARVIS_OMNIROUTE_CONTRACT_VERSION",
    "JARVIS_OMNIROUTE_MISSION_ID",
    "JARVIS_OMNIROUTE_TASK_ID",
    "JARVIS_OMNIROUTE_ATTEMPT_ID",
    "JARVIS_OMNIROUTE_REQUEST_ID",
    "JARVIS_OMNIROUTE_CORRELATION_ID",
  ])
    delete environment[name];
  const identity = {
    mission: task?.mission_id,
    task: task?.task_id,
    attempt: task?.attempt_id,
    request: task?.request_id,
    correlation: task?.correlation_id,
  };
  const values = Object.values(identity);
  if (
    values.every(
      (value) => typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/.test(value)
    ) &&
    identity.request === identity.correlation &&
    identity.correlation !== identity.attempt
  ) {
    environment.JARVIS_OMNIROUTE_CONTRACT_VERSION = "2";
    environment.JARVIS_OMNIROUTE_MISSION_ID = identity.mission;
    environment.JARVIS_OMNIROUTE_TASK_ID = identity.task;
    environment.JARVIS_OMNIROUTE_ATTEMPT_ID = identity.attempt;
    environment.JARVIS_OMNIROUTE_REQUEST_ID = identity.request;
    environment.JARVIS_OMNIROUTE_CORRELATION_ID = identity.correlation;
  }
  return environment;
}

function childEnvironment(task) {
  const environment = hermesIdentityEnvironment(task);
  delete environment.NODE_TEST_CONTEXT;
  delete environment.NODE_CHANNEL_FD;
  delete environment.NODE_CHANNEL_SERIALIZATION;
  return environment;
}

function supervisorPrompt(task) {
  return `Use only the terminal tool. Execute the task; do not guess or describe the answer.\nTASK: ${task.description}`;
}

export class HermesLocalAdapter {
  constructor({
    executable,
    args = [],
    timeoutMs = 300_000,
    queueTimeoutMs = 300_000,
    maxOutputBytes = 65_536,
    admissionRoot = null,
    endpointKey = "http://127.0.0.1:8080/v1",
    endpointCapacity = 1,
    profile = "supervisor-minimal",
  } = {}) {
    if (!executable) throw new Error("An explicit Hermes executable is required");
    this.executable = executable;
    this.args = [...args];
    this.timeoutMs = timeoutMs;
    this.queueTimeoutMs = queueTimeoutMs;
    this.maxOutputBytes = maxOutputBytes;
    this.admissionRoot = admissionRoot;
    this.endpointKey = endpointKey;
    this.endpointCapacity = endpointCapacity;
    this.profile = profile;
    this.tasks = new Map();
    this.attempts = new Map();
  }

  async assertAvailable() {
    try {
      await access(this.executable, constants.X_OK);
    } catch {
      throw new Error(`Hermes local executor unavailable: ${this.executable}`);
    }
  }

  async send_task(task) {
    await this.assertAvailable();
    const attemptKey = `${task.task_id}\0${task.attempt_id}`;
    const existing = this.attempts.get(attemptKey);
    if (existing) return existing;

    const handle = `hermes-local-${randomUUID()}`;
    const record = {
      child: null,
      status: "queued",
      output: "",
      error: "",
      failure: null,
      controller: new AbortController(),
      timeout: null,
    };
    const minimal = this.profile === "supervisor-minimal";
    const commandArgs = [
      ...this.args,
      "chat",
      ...(minimal ? ["--ignore-rules"] : []),
      "--quiet",
      "--max-turns",
      "4",
      "--format",
      "stream-json",
      "--source",
      boundedSource(task),
      "--query",
      minimal ? supervisorPrompt(task) : task.prompt,
    ];
    record.promise = this.executeTask(record, task, commandArgs);
    record.promise.catch(() => {});
    this.tasks.set(handle, record);
    this.attempts.set(attemptKey, handle);
    return handle;
  }

  async executeTask(record, task, commandArgs) {
    let lease = null;
    try {
      if (this.admissionRoot) {
        const gate = new EndpointAdmissionGate({
          root: this.admissionRoot,
          endpointKey: this.endpointKey,
          capacity: this.endpointCapacity,
          queueTimeoutMs: this.queueTimeoutMs,
        });
        lease = await gate.acquire(
          { task_id: task.task_id, attempt_id: task.attempt_id },
          { signal: record.controller.signal }
        );
      }
      if (record.status === "cancelled") throw new Error("Hermes task cancelled");
      record.status = "running";
      return await new Promise((resolve, reject) => {
        const child = spawn(this.executable, commandArgs, {
          shell: false,
          stdio: ["ignore", "pipe", "pipe"],
          env: childEnvironment(task),
        });
        record.child = child;
        const timeout = setTimeout(() => {
          if (record.status === "cancelled") return;
          record.failure = new Error("Hermes local executor timed out");
          record.failure.code = "INFERENCE_TIMEOUT";
          record.status = "failed";
          child.kill("SIGTERM");
        }, this.timeoutMs);
        record.timeout = timeout;
        const append = (target, chunk) => {
          const next = record[target] + chunk.toString("utf8");
          if (Buffer.byteLength(next) > this.maxOutputBytes) {
            record.failure = new Error("Hermes local executor output limit exceeded");
            record.status = "failed";
            child.kill("SIGTERM");
            return;
          }
          record[target] = next;
        };
        child.stdout.on("data", (chunk) => append("output", chunk));
        child.stderr.on("data", (chunk) => append("error", chunk));
        child.on("error", (error) => {
          clearTimeout(timeout);
          record.status = "failed";
          reject(new Error(`Hermes local executor failed: ${error.message}`));
        });
        child.on("close", (code, signal) => {
          clearTimeout(timeout);
          if (record.failure) {
            if (record.failure.code === "INFERENCE_TIMEOUT") {
              const partial = normalizePartialStreamJson(
                record.output,
                task,
                "Hermes local executor timed out after terminal evidence"
              );
              if (partial) {
                record.status = "complete";
                return resolve(partial);
              }
            }
            return reject(record.failure);
          }
          if (record.status === "cancelled") return reject(new Error("Hermes task cancelled"));
          if (code !== 0) {
            record.status = "failed";
            const diagnostic = (record.error || record.output).trim().slice(0, 500);
            return reject(
              new Error(`Hermes local executor exited ${code ?? signal}: ${diagnostic}`)
            );
          }
          try {
            const normalized = normalizeHermesTransportResult(record.output, task);
            record.status = "complete";
            resolve(normalized);
          } catch (error) {
            record.status = "failed";
            reject(error);
          }
        });
      });
    } finally {
      if (lease) await lease.release();
    }
  }

  async poll_status(handle) {
    const record = this.tasks.get(handle);
    return { status: record?.status ?? "unknown" };
  }

  async wait_for_result(handle) {
    const record = this.tasks.get(handle);
    if (!record) throw new Error("Unknown Hermes local adapter handle");
    return record.promise;
  }

  async cancel_task(handle) {
    const record = this.tasks.get(handle);
    if (!record) return { status: "unknown" };
    if (["complete", "failed", "cancelled"].includes(record.status)) {
      return { status: record.status };
    }
    record.status = "cancelled";
    record.controller.abort();
    clearTimeout(record.timeout);
    record.child?.kill("SIGTERM");
    return { status: "cancelled" };
  }

  async health_check() {
    try {
      await this.assertAvailable();
      const { stdout } = await execFileAsync(this.executable, [...this.args, "--version"], {
        shell: false,
        timeout: Math.min(this.timeoutMs, 10_000),
        maxBuffer: this.maxOutputBytes,
        encoding: "utf8",
        env: childEnvironment(),
      });
      return { status: "healthy", transport: "hermes-local", version: stdout.trim() };
    } catch (error) {
      return {
        status: "unhealthy",
        transport: "hermes-local",
        reason: String(error?.message ?? error).slice(0, 500),
      };
    }
  }
}
