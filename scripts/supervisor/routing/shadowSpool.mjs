import { randomUUID, createHash } from "node:crypto";
import { constants } from "node:fs";
import { lstat, mkdir, open, readdir, rename, unlink, link } from "node:fs/promises";
import path from "node:path";

import {
  mergeShadowRecord,
  normalizeShadowInput,
  normalizeShadowOutput,
  validateShadowRecord,
  SHADOW_RECORD_MAX_BYTES,
} from "./shadowAdvisory.mjs";

const MAX_QUEUE = 100;
const MAX_RESULTS = 500;
const MAX_ENVELOPE_BYTES = 24_576;
const MAX_PROTOCOL_BYTES = 32_768;
const LOCK_STALE_MS = 120_000;
const LOCK_WAIT_MS = 5_000;
const SAFE_HASH = /^[a-f0-9]{64}$/;
const SAFE_LEASE = /^[a-f0-9]{32}$/;

function keyFor(taskId, attemptId) {
  return createHash("sha256").update(`${taskId}\0${attemptId}`).digest("hex");
}

async function assertDirectory(directory, create = false) {
  const absolute = path.resolve(directory);
  const parsed = path.parse(absolute);
  let current = parsed.root;
  for (const part of absolute.slice(parsed.root.length).split(path.sep).filter(Boolean)) {
    current = path.join(current, part);
    try {
      const info = await lstat(current);
      if (info.isSymbolicLink() || !info.isDirectory())
        throw new Error(`Unsafe spool path boundary: ${current}`);
    } catch (error) {
      if (error?.code !== "ENOENT" || !create) throw error;
      await mkdir(current, { mode: 0o700 }).catch((mkdirError) => {
        if (mkdirError?.code !== "EEXIST") throw mkdirError;
      });
      const info = await lstat(current);
      if (info.isSymbolicLink() || !info.isDirectory())
        throw new Error(`Unsafe spool path boundary: ${current}`);
    }
  }
  return absolute;
}

async function assertRegularFile(file) {
  const info = await lstat(file);
  if (info.isSymbolicLink() || !info.isFile()) throw new Error(`Unsafe spool file: ${file}`);
  if (
    (typeof process.getuid === "function" && info.uid !== process.getuid()) ||
    (info.mode & 0o077) !== 0
  ) {
    throw new Error(`Unsafe spool file ownership or permissions: ${file}`);
  }
  return info;
}

async function restrictDirectory(directory) {
  const handle = await open(
    directory,
    constants.O_RDONLY | (constants.O_DIRECTORY ?? 0) | (constants.O_NOFOLLOW ?? 0)
  );
  try {
    const info = await handle.stat();
    if (typeof process.getuid === "function" && info.uid !== process.getuid())
      throw new Error(`Unsafe spool directory owner: ${directory}`);
    await handle.chmod(0o700);
  } finally {
    await handle.close();
  }
}

async function syncDirectory(directory) {
  try {
    const handle = await open(directory, constants.O_RDONLY | (constants.O_DIRECTORY ?? 0));
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch {
    // Some filesystems do not support fsync on directory descriptors.
  }
}

async function atomicCreateJson(directory, filename, value, maxBytes) {
  const destination = path.join(directory, filename);
  const serialized = `${JSON.stringify(value)}\n`;
  if (Buffer.byteLength(serialized) > maxBytes)
    throw new TypeError("Shadow spool record exceeds size limit");
  const temporary = path.join(directory, `.tmp-${process.pid}-${randomUUID().replaceAll("-", "")}`);
  const handle = await open(
    temporary,
    constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0),
    0o600
  );
  try {
    await handle.writeFile(serialized, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await link(temporary, destination);
    await syncDirectory(directory);
    return true;
  } catch (error) {
    if (error?.code === "EEXIST") return false;
    throw error;
  } finally {
    await unlink(temporary).catch(() => {});
  }
}

async function atomicReplaceJson(directory, filename, value, maxBytes) {
  const destination = path.join(directory, filename);
  const serialized = `${JSON.stringify(value)}\n`;
  if (Buffer.byteLength(serialized) > maxBytes)
    throw new TypeError("Shadow spool record exceeds size limit");
  const temporary = path.join(directory, `.tmp-${process.pid}-${randomUUID().replaceAll("-", "")}`);
  const handle = await open(
    temporary,
    constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0),
    0o600
  );
  try {
    await handle.writeFile(serialized, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await assertRegularFile(destination);
    await rename(temporary, destination);
    await syncDirectory(directory);
  } finally {
    await unlink(temporary).catch(() => {});
  }
}

async function readJson(file, maxBytes) {
  const handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const info = await handle.stat();
    if (
      !info.isFile() ||
      (typeof process.getuid === "function" && info.uid !== process.getuid()) ||
      (info.mode & 0o077) !== 0
    )
      throw new Error(`Unsafe spool file: ${file}`);
    if (info.size > maxBytes) throw new TypeError("Oversized shadow spool file");
    const raw = await handle.readFile("utf8");
    if (Buffer.byteLength(raw) > maxBytes) throw new TypeError("Oversized shadow spool file");
    return JSON.parse(raw);
  } finally {
    await handle.close();
  }
}

async function staleLockIsAbandoned(lockPath) {
  const initial = await assertRegularFile(lockPath);
  if (Date.now() - initial.mtimeMs <= LOCK_STALE_MS) return false;
  const handle = await open(lockPath, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const opened = await handle.stat();
    if (opened.dev !== initial.dev || opened.ino !== initial.ino) return false;
    const content = await handle.readFile("utf8");
    const pid = Number(content.split(/\r?\n/, 1)[0]);
    if (Number.isSafeInteger(pid) && pid > 0) {
      try {
        process.kill(pid, 0);
        return false;
      } catch (error) {
        if (error?.code !== "ESRCH") return false;
      }
    }
    const current = await lstat(lockPath).catch((error) =>
      error?.code === "ENOENT" ? null : Promise.reject(error)
    );
    return Boolean(
      current &&
      current.dev === opened.dev &&
      current.ino === opened.ino &&
      !current.isSymbolicLink()
    );
  } finally {
    await handle.close();
  }
}

async function safeJsonFiles(directory) {
  const names = (await readdir(directory))
    .filter((name) => /^[a-f0-9]{64}(?:\.[a-f0-9]{32})?\.json$/.test(name))
    .sort();
  const files = [];
  for (const name of names) {
    const filename = path.join(directory, name);
    const info = await lstat(filename);
    if (info.isSymbolicLink() || !info.isFile()) throw new Error(`Unsafe spool entry: ${filename}`);
    files.push({ name, filename, mtimeMs: info.mtimeMs });
  }
  return files;
}

export class DurableShadowSpool {
  constructor(root, { maxQueue = MAX_QUEUE, maxResults = MAX_RESULTS, leaseMs = 60_000 } = {}) {
    if (
      typeof root !== "string" ||
      !root ||
      root.includes("\0") ||
      root.split(/[\\/]+/).includes("..")
    ) {
      throw new TypeError("Invalid shadow spool path");
    }
    if (!Number.isInteger(maxQueue) || maxQueue < 1 || maxQueue > MAX_QUEUE)
      throw new TypeError("Invalid shadow queue limit");
    if (!Number.isInteger(maxResults) || maxResults < 1 || maxResults > MAX_RESULTS)
      throw new TypeError("Invalid shadow retention limit");
    if (!Number.isFinite(leaseMs) || leaseMs < 1) throw new TypeError("Invalid shadow claim lease");
    this.root = path.resolve(root);
    this.maxQueue = maxQueue;
    this.maxResults = maxResults;
    this.leaseMs = leaseMs;
    this.directories = Object.fromEntries(
      ["pending", "inflight", "records", "acknowledged"].map((name) => [
        name,
        path.join(this.root, name),
      ])
    );
    this.lockPath = path.join(this.root, ".spool.lock");
  }

  async ensureLayout() {
    await assertDirectory(this.root, true);
    await restrictDirectory(this.root);
    for (const directory of Object.values(this.directories)) {
      await assertDirectory(directory, true);
      await restrictDirectory(directory);
    }
  }

  async withLock(operation) {
    await this.ensureLayout();
    const started = Date.now();
    let lockHandle;
    while (!lockHandle) {
      try {
        const candidate = await open(
          this.lockPath,
          constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0),
          0o600
        );
        try {
          await candidate.writeFile(`${process.pid}\n${Date.now()}\n`);
          await candidate.sync();
          lockHandle = candidate;
        } catch (error) {
          await candidate.close().catch(() => {});
          await unlink(this.lockPath).catch(() => {});
          throw error;
        }
      } catch (error) {
        if (error?.code !== "EEXIST") throw error;
        await assertRegularFile(this.lockPath).catch((checkError) => {
          if (checkError?.code === "ENOENT") return null;
          throw checkError;
        });
        const abandoned = await staleLockIsAbandoned(this.lockPath).catch((checkError) => {
          if (checkError?.code === "ENOENT") return false;
          throw checkError;
        });
        if (abandoned) await unlink(this.lockPath).catch(() => {});
        if (Date.now() - started > LOCK_WAIT_MS)
          throw new Error("Timed out acquiring shadow spool lock");
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
    const heldInfo = await lockHandle.stat();
    try {
      return await operation();
    } finally {
      await lockHandle.close();
      const current = await lstat(this.lockPath).catch(() => null);
      if (
        current &&
        current.dev === heldInfo.dev &&
        current.ino === heldInfo.ino &&
        !current.isSymbolicLink()
      ) {
        await unlink(this.lockPath).catch(() => {});
      }
    }
  }

  async cleanupStaleTemps() {
    for (const directory of Object.values(this.directories)) {
      for (const name of await readdir(directory)) {
        if (!name.startsWith(".tmp-")) continue;
        const filename = path.join(directory, name);
        const info = await lstat(filename);
        if (info.isSymbolicLink() || !info.isFile())
          throw new Error(`Unsafe spool temporary: ${filename}`);
        if (Date.now() - info.mtimeMs > LOCK_STALE_MS) await unlink(filename);
      }
    }
  }

  async enqueue(inputValue, recordValue) {
    const input = normalizeShadowInput(inputValue);
    const record = validateShadowRecord(recordValue);
    if (
      record.task_id !== input.task_id ||
      record.attempt_id !== input.attempt_id ||
      record.request_id !== input.request_id ||
      record.correlation_id !== input.correlation_id
    ) {
      throw new TypeError("Shadow job identity mismatch");
    }
    const jobId = keyFor(input.task_id, input.attempt_id);
    return this.withLock(async () => {
      await this.cleanupStaleTemps();
      const filename = `${jobId}.json`;
      const recordPath = path.join(this.directories.records, filename);
      if (await pathExistsSafe(recordPath)) return true;
      if (await pathExistsSafe(path.join(this.directories.acknowledged, `${jobId}.json`)))
        return true;
      if (await pathExistsSafe(path.join(this.directories.pending, filename))) return true;
      if (
        (await safeJsonFiles(this.directories.inflight)).some((file) =>
          file.name.startsWith(`${jobId}.`)
        )
      )
        return true;
      const pending = await safeJsonFiles(this.directories.pending);
      const inflight = await safeJsonFiles(this.directories.inflight);
      if (pending.length + inflight.length >= this.maxQueue) return false;
      const envelope = { job_id: jobId, input, record, claimed_at: null };
      if (Buffer.byteLength(JSON.stringify(envelope)) > MAX_ENVELOPE_BYTES)
        throw new TypeError("Shadow spool job exceeds size limit");
      const created = await atomicCreateJson(
        this.directories.pending,
        filename,
        envelope,
        MAX_ENVELOPE_BYTES
      );
      if (!created) return true;
      try {
        const recordCreated = await atomicCreateJson(
          this.directories.records,
          filename,
          record,
          SHADOW_RECORD_MAX_BYTES
        );
        if (!recordCreated) {
          await unlink(path.join(this.directories.pending, filename)).catch(() => {});
        }
        return recordCreated;
      } catch (error) {
        await unlink(path.join(this.directories.pending, filename)).catch(() => {});
        throw error;
      }
    });
  }

  async claim(workerId = "worker") {
    const worker = String(workerId);
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(worker)) throw new TypeError("Invalid shadow worker ID");
    return this.withLock(async () => {
      await this.cleanupStaleTemps();
      const now = Date.now();
      const inflight = await safeJsonFiles(this.directories.inflight);
      for (const file of inflight) {
        const envelope = await readJson(file.filename, MAX_ENVELOPE_BYTES);
        const expired =
          !Number.isFinite(envelope.claimed_at) || now - envelope.claimed_at > this.leaseMs;
        if (!expired) continue;
        const baseName = `${envelope.job_id}.json`;
        const pendingPath = path.join(this.directories.pending, baseName);
        if (await pathExistsSafe(pendingPath)) await unlink(file.filename);
        else await rename(file.filename, pendingPath);
      }
      const pending = await safeJsonFiles(this.directories.pending);
      const activeClaims = await safeJsonFiles(this.directories.inflight);
      if (activeClaims.length) return null;
      for (const file of pending) {
        const envelope = await readJson(file.filename, MAX_ENVELOPE_BYTES);
        validateShadowRecord(envelope.record);
        const recordPath = path.join(this.directories.records, `${envelope.job_id}.json`);
        if (!(await pathExistsSafe(recordPath))) {
          await atomicCreateJson(
            this.directories.records,
            `${envelope.job_id}.json`,
            envelope.record,
            SHADOW_RECORD_MAX_BYTES
          );
        }
        const ackPath = path.join(this.directories.acknowledged, `${envelope.job_id}.json`);
        if (await pathExistsSafe(ackPath)) {
          await unlink(file.filename);
          await this.writeAck(envelope.job_id);
          continue;
        }
        const leaseId = randomUUID().replaceAll("-", "");
        const inflightName = `${envelope.job_id}.${leaseId}.json`;
        const inflightPath = path.join(this.directories.inflight, inflightName);
        try {
          await rename(file.filename, inflightPath);
        } catch (error) {
          if (error?.code === "ENOENT" || error?.code === "EEXIST") continue;
          throw error;
        }
        envelope.claimed_at = now;
        await atomicReplaceJson(
          this.directories.inflight,
          inflightName,
          envelope,
          MAX_ENVELOPE_BYTES
        );
        return {
          job_id: envelope.job_id,
          lease_id: leaseId,
          input: envelope.input,
          record: envelope.record,
        };
      }
      return null;
    });
  }

  async complete(claim, adviceValue) {
    if (!claim || !SAFE_HASH.test(claim.job_id) || !SAFE_LEASE.test(claim.lease_id))
      throw new TypeError("Invalid shadow claim");
    return this.withLock(async () => {
      const inflightName = `${claim.job_id}.${claim.lease_id}.json`;
      const inflightPath = path.join(this.directories.inflight, inflightName);
      const existingResultPath = path.join(this.directories.records, `${claim.job_id}.json`);
      const envelope = (await pathExistsSafe(inflightPath))
        ? await readJson(inflightPath, MAX_ENVELOPE_BYTES)
        : null;
      if (!envelope) {
        if (await pathExistsSafe(existingResultPath)) {
          const current = validateShadowRecord(
            await readJson(existingResultPath, SHADOW_RECORD_MAX_BYTES)
          );
          if (!claim.input)
            throw new TypeError("Late shadow advice is missing its original candidate set");
          const patch = normalizeAdvice(claim.input, adviceValue);
          const merged = mergeShadowRecord(current, patch);
          await atomicReplaceJson(
            this.directories.records,
            `${claim.job_id}.json`,
            merged,
            SHADOW_RECORD_MAX_BYTES
          );
          await this.writeAck(claim.job_id);
          return true;
        }
        return false;
      }
      if (envelope.job_id !== claim.job_id)
        throw new TypeError("Shadow claim job identity mismatch");
      const output = normalizeAdvice(envelope.input, adviceValue);
      const resultName = `${claim.job_id}.json`;
      const current = validateShadowRecord(
        await readJson(existingResultPath, SHADOW_RECORD_MAX_BYTES)
      );
      await atomicReplaceJson(
        this.directories.records,
        resultName,
        mergeShadowRecord(current, output),
        SHADOW_RECORD_MAX_BYTES
      );
      await this.writeAck(claim.job_id);
      await unlink(inflightPath).catch((error) => {
        if (error?.code !== "ENOENT") throw error;
      });
      await this.pruneResults();
      return true;
    });
  }

  async updateRecord(patchValue) {
    const patch = patchValue;
    const taskId = patch?.task_id;
    const attemptId = patch?.attempt_id;
    if (typeof taskId !== "string" || typeof attemptId !== "string")
      throw new TypeError("Shadow update requires identity");
    const jobId = keyFor(taskId, attemptId);
    return this.withLock(async () => {
      const resultName = `${jobId}.json`;
      const resultPath = path.join(this.directories.records, resultName);
      if (await pathExistsSafe(resultPath)) {
        const merged = mergeShadowRecord(
          await readJson(resultPath, SHADOW_RECORD_MAX_BYTES),
          patch
        );
        await atomicReplaceJson(
          this.directories.records,
          resultName,
          merged,
          SHADOW_RECORD_MAX_BYTES
        );
        return true;
      }
      const pendingPath = path.join(this.directories.pending, resultName);
      if (await pathExistsSafe(pendingPath)) {
        const envelope = await readJson(pendingPath, MAX_ENVELOPE_BYTES);
        envelope.record = mergeShadowRecord(envelope.record, patch);
        await atomicReplaceJson(this.directories.pending, resultName, envelope, MAX_ENVELOPE_BYTES);
        return true;
      }
      for (const file of await safeJsonFiles(this.directories.inflight)) {
        if (!file.name.startsWith(`${jobId}.`)) continue;
        const envelope = await readJson(file.filename, MAX_ENVELOPE_BYTES);
        envelope.record = mergeShadowRecord(envelope.record, patch);
        await atomicReplaceJson(this.directories.inflight, file.name, envelope, MAX_ENVELOPE_BYTES);
        return true;
      }
      return false;
    });
  }

  async listResults() {
    await this.ensureLayout();
    const files = await safeJsonFiles(this.directories.records);
    return Promise.all(
      files.map(async (file) =>
        validateShadowRecord(await readJson(file.filename, SHADOW_RECORD_MAX_BYTES))
      )
    );
  }

  async writeAck(jobId) {
    if (!SAFE_HASH.test(jobId)) throw new TypeError("Invalid shadow job ID");
    await atomicCreateJson(
      this.directories.acknowledged,
      `${jobId}.json`,
      { job_id: jobId, acknowledged_at: Date.now() },
      1024
    );
  }

  async pruneResults() {
    const files = await safeJsonFiles(this.directories.records);
    files.sort((a, b) => a.mtimeMs - b.mtimeMs);
    for (const file of files.slice(0, Math.max(0, files.length - this.maxResults))) {
      const jobId = file.name.slice(0, -5);
      const ackPath = path.join(this.directories.acknowledged, file.name);
      if (!(await pathExistsSafe(ackPath))) continue;
      await unlink(file.filename);
      await unlink(ackPath).catch((error) => {
        if (error?.code !== "ENOENT") throw error;
      });
    }
  }
}

async function pathExistsSafe(file) {
  try {
    await assertRegularFile(file);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

function normalizeAdvice(input, value) {
  const raw =
    value && Object.hasOwn(value, "selected")
      ? value
      : {
          task_id: value?.task_id,
          attempt_id: value?.attempt_id,
          selected: value?.laya_selected ?? null,
          abstain: value?.laya_abstain ?? true,
          confidence: value?.laya_confidence ?? null,
          latency_ms: value?.laya_latency_ms ?? null,
          peak_rss_bytes: value?.laya_peak_rss_bytes ?? null,
          fallback: value?.laya_fallback ?? true,
          fallback_reason: value?.laya_fallback_reason ?? null,
          error_category: value?.laya_error_category ?? null,
          adapter_version: value?.laya_adapter_version ?? null,
        };
  return normalizeShadowOutput(input, raw);
}

export class SshShadowSpoolClient {
  constructor({ host, ssh = "ssh", timeoutMs = 10_000, spawnProcess } = {}) {
    if (typeof host !== "string" || !/^[A-Za-z0-9_.@-]{1,255}$/.test(host) || host.startsWith("-"))
      throw new TypeError("Invalid SSH host alias");
    this.host = host;
    this.ssh = ssh;
    this.timeoutMs = timeoutMs;
    this.spawnProcess = spawnProcess;
  }

  async call(request) {
    const { spawn } = await import("node:child_process");
    const spawnProcess = this.spawnProcess ?? spawn;
    const child = spawnProcess(
      this.ssh,
      [
        "-T",
        "-oBatchMode=yes",
        "-oConnectTimeout=5",
        "-oClearAllForwardings=yes",
        "-oForwardAgent=no",
        "-oPermitLocalCommand=no",
        "-oRequestTTY=no",
        "-oStrictHostKeyChecking=yes",
        "-oIdentitiesOnly=yes",
        "--",
        this.host,
      ],
      { shell: false, stdio: ["pipe", "pipe", "pipe"] }
    );
    const requestBody = `${JSON.stringify(request)}\n`;
    if (Buffer.byteLength(requestBody) > MAX_PROTOCOL_BYTES)
      throw new TypeError("Shadow SSH request exceeds size limit");
    let stdout = "";
    let stderr = "";
    const collect = (target, chunk) => {
      const next = target + chunk.toString("utf8");
      if (Buffer.byteLength(next) > MAX_PROTOCOL_BYTES) child.kill("SIGKILL");
      return next;
    };
    child.stdout.on("data", (chunk) => {
      stdout = collect(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = collect(stderr, chunk);
    });
    const completion = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        child.kill("SIGTERM");
        reject(new Error("Shadow SSH request timed out"));
      }, this.timeoutMs);
      child.on("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      child.on("close", (code) => {
        clearTimeout(timer);
        if (code !== 0) return reject(new Error(`Restricted shadow SSH command failed (${code})`));
        try {
          resolve(JSON.parse(stdout));
        } catch {
          reject(new Error("Invalid shadow SSH response"));
        }
      });
    });
    child.stdin.end(requestBody);
    return completion;
  }

  async claim(workerId) {
    const response = await this.call({ operation: "claim", worker_id: workerId });
    if (response?.ok !== true) throw new Error("Shadow spool claim rejected");
    return response.claim ?? null;
  }

  async complete(claim, advice) {
    const response = await this.call({ operation: "complete", claim, advice });
    return response?.ok === true && response.acknowledged === true;
  }
}

export async function handleShadowSpoolProtocol(spool, request) {
  if (
    !request ||
    typeof request !== "object" ||
    Array.isArray(request) ||
    Buffer.byteLength(JSON.stringify(request)) > MAX_PROTOCOL_BYTES
  ) {
    throw new TypeError("Invalid shadow spool protocol request");
  }
  if (request.operation === "claim") {
    if (Object.keys(request).some((key) => !["operation", "worker_id"].includes(key)))
      throw new TypeError("Unexpected shadow claim field");
    return { ok: true, claim: await spool.claim(request.worker_id) };
  }
  if (request.operation === "complete") {
    if (Object.keys(request).some((key) => !["operation", "claim", "advice"].includes(key)))
      throw new TypeError("Unexpected shadow completion field");
    return { ok: true, acknowledged: await spool.complete(request.claim, request.advice) };
  }
  throw new TypeError("Unsupported shadow spool operation");
}
