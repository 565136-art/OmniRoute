#!/usr/bin/env node
import { readFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

import { CommandHermesAdapter } from "./adapters/commandAdapter.mjs";
import { FakeHermesAdapter } from "./adapters/fakeAdapter.mjs";
import { HermesLocalAdapter } from "./adapters/hermesLocalAdapter.mjs";
import { SupervisorEngine } from "./engine.mjs";
import { formatHermesPrompt } from "./resultContract.mjs";
import { createInitialState } from "./schema.mjs";
import { MissionStateStore } from "./stateStore.mjs";
import { ResourceLockManager } from "./resourceLocks.mjs";
import { SupervisorScheduler } from "./scheduler.mjs";
import { EvidenceCache } from "./evidenceCache.mjs";
import { VerifierHarness } from "./verifiers/index.mjs";
import { RoutingObservationStore } from "./routing/observationStore.mjs";
import { DurableShadowSpool, ShadowAdvisoryDispatcher } from "./routing/shadowAdvisory.mjs";

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const options = { args: [] };
  for (let index = 0; index < rest.length; index += 1) {
    const name = rest[index];
    if (name === "--dry-run") options.dryRun = true;
    else if (name === "--arg") options.args.push(rest[++index]);
    else if (name.startsWith("--")) options[name.slice(2).replaceAll("-", "_")] = rest[++index];
    else throw new Error(`Unexpected argument: ${name}`);
  }
  return { command, options };
}

function hermesAdapterOptions(options, stateRoot) {
  return {
    executable: options.executable,
    args: options.args,
    admissionRoot: path.join(stateRoot, "endpoint-admission"),
    endpointKey: options.endpoint_key ?? "http://127.0.0.1:8080/v1",
    endpointCapacity: Number(options.endpoint_capacity ?? process.env.LOCAL_ENDPOINT_CAPACITY ?? 1),
    queueTimeoutMs: Number(options.queue_timeout_ms ?? 300_000),
    timeoutMs: Number(options.inference_timeout_ms ?? 300_000),
  };
}

async function adapterFrom(options, stateRoot) {
  if (options.adapter === "fake") {
    if (!options.response_file) throw new Error("Fake adapter requires --response-file");
    return new FakeHermesAdapter([await readFile(options.response_file, "utf8")]);
  }
  if (options.adapter === "command") {
    if (!options.executable) throw new Error("Command adapter requires --executable");
    return new CommandHermesAdapter({ executable: options.executable, args: options.args });
  }
  if (options.adapter === "hermes-local") {
    if (!options.executable) throw new Error("Hermes-local adapter requires --executable");
    return new HermesLocalAdapter(hermesAdapterOptions(options, stateRoot));
  }
  throw new Error("An explicit --adapter fake|command|hermes-local is required");
}

async function dagAdapterFactory(options, stateRoot) {
  if (!options.adapter)
    throw new Error("An explicit --adapter fake|command|hermes-local is required");
  if (options.adapter === "fake") {
    if (!options.response_file) throw new Error("Fake adapter requires --response-file");
    const configured = JSON.parse(await readFile(options.response_file, "utf8"));
    return (lane) => {
      const value = configured[lane.task_id] ?? configured.default ?? configured;
      return new FakeHermesAdapter([typeof value === "string" ? value : JSON.stringify(value)]);
    };
  }
  if (options.adapter === "command") {
    if (!options.executable) throw new Error("Command adapter requires --executable");
    return () => new CommandHermesAdapter({ executable: options.executable, args: options.args });
  }
  if (options.adapter === "hermes-local") {
    if (!options.executable) throw new Error("Hermes-local adapter requires --executable");
    return () => new HermesLocalAdapter(hermesAdapterOptions(options, stateRoot));
  }
  throw new Error("An explicit --adapter fake|command|hermes-local is required");
}

function dagScheduler(stateRoot, mission, adapterFactory, maxConcurrency) {
  const cache = new EvidenceCache(stateRoot);
  return new SupervisorScheduler({
    root: stateRoot,
    missionId: mission,
    adapterFactory,
    lockManager: new ResourceLockManager(stateRoot),
    verifierHarness: new VerifierHarness({ cache }),
    maxConcurrency,
  });
}

async function routingFrom(options, stateRoot) {
  if (!options.routing_config) return null;
  const config = JSON.parse(await readFile(options.routing_config, "utf8"));
  const shadowSpool = new DurableShadowSpool(
    path.join(stateRoot, "empirical-routing", "shadow-spool"),
    { maxQueue: config.shadow?.max_queue ?? 100, maxResults: config.shadow?.max_results ?? 500 }
  );
  const shadowDispatcher = new ShadowAdvisoryDispatcher({
    enabled: config.shadow?.enabled === true,
    sampleRate: config.shadow?.sample_rate ?? 0.05,
    maxQueue: config.shadow?.max_queue ?? 100,
    transport: shadowSpool,
  });
  return {
    enabled: true,
    store: new RoutingObservationStore(stateRoot),
    candidates: config.candidates,
    conservative_fallback: config.conservative_fallback,
    minimum_samples: config.minimum_samples ?? 5,
    exploration_interval: config.exploration_interval ?? 10,
    exploration_sequence: config.exploration_sequence ?? 0,
    shadowDispatcher,
    shadowSpool,
  };
}

export async function main(argv = process.argv.slice(2)) {
  const { command, options } = parseArgs(argv);
  const stateRoot = path.resolve(options.state_root ?? ".supervisor-state");
  if (!options.mission) throw new Error("--mission is required");
  if (["dag-init", "dag-run", "dag-resume", "dag-status"].includes(command)) {
    const maxConcurrency = Number(
      options.max_concurrency ?? process.env.SUPERVISOR_MAX_CONCURRENCY ?? 3
    );
    const scheduler = dagScheduler(stateRoot, options.mission, null, maxConcurrency);
    if (command === "dag-init") {
      if (!options.config) throw new Error("dag-init requires --config");
      const config = JSON.parse(await readFile(options.config, "utf8"));
      const state = await scheduler.initialize({
        ...config,
        max_concurrency: config.max_concurrency ?? maxConcurrency,
      });
      process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
      return state;
    }
    if (command === "dag-status") {
      const state = await scheduler.load();
      process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
      return state;
    }
    if (options.dryRun) {
      const state = await scheduler.load();
      process.stdout.write(
        `${JSON.stringify({ mission_id: state.mission_id, tasks: Object.keys(state.lanes) })}\nSIMULATED_DAG_DISPATCH=AUTO_CONTINUE\n`
      );
      return state;
    }
    scheduler.adapterFactory = await dagAdapterFactory(options, stateRoot);
    scheduler.routing = await routingFrom(options, stateRoot);
    const state = command === "dag-resume" ? await scheduler.recover() : await scheduler.run();
    process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
    return state;
  }
  const store = new MissionStateStore(stateRoot, options.mission);

  if (command === "init") {
    if (!options.config) throw new Error("init requires --config");
    const config = JSON.parse(await readFile(options.config, "utf8"));
    const state = createInitialState({ ...config, mission_id: options.mission });
    await store.save(state);
    process.stdout.write(
      `${JSON.stringify({ mission_id: state.mission_id, status: state.status }, null, 2)}\n`
    );
    return state;
  }

  if (command === "status") {
    const state = await store.load();
    process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
    return state;
  }

  if (!["run", "resume"].includes(command))
    throw new Error("Command must be init, run, resume, or status");
  const state = await store.load();
  if (options.dryRun) {
    process.stdout.write(`${formatHermesPrompt(state)}\nSIMULATED_DECISION=AUTO_CONTINUE\n`);
    return state;
  }
  const engine = new SupervisorEngine({ store, adapter: await adapterFrom(options, stateRoot) });
  const result = command === "resume" ? await engine.recover() : await engine.runOnce();
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  return result;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
