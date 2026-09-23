function matchingStat(statistics, taskType, toolProfile, candidate) {
  return statistics.find(
    (item) =>
      item.task_type === taskType &&
      item.executor === candidate.executor &&
      (item.model ?? null) === (candidate.model ?? null) &&
      (item.provider ?? null) === (candidate.provider ?? null) &&
      (item.tool_profile ?? null) === (toolProfile ?? null)
  );
}

function eligible(candidate, requiredTools, requiredContextIds) {
  if (!candidate.available) return false;
  const tools = new Set(candidate.tools ?? []);
  const context = new Set(candidate.context_ids ?? []);
  return (
    requiredTools.every((item) => tools.has(item)) &&
    requiredContextIds.every((item) => context.has(item))
  );
}

const HEALTH_RANK = { DEGRADED: 0, PROBATION: 1, HEALTHY: 2 };

function boundedRatio(numerator, denominator) {
  if (!denominator) return 0;
  return Math.max(0, Math.min(1, numerator / denominator));
}

function scoreCandidate(stat, candidate, minimumSamples) {
  const samples = stat?.samples ?? 0;
  const successReliability = stat?.smoothed_success ?? 0.5;
  const verifierReliability = stat?.smoothed_verified ?? successReliability;
  const acceptanceReliability = stat?.smoothed_acceptance ?? verifierReliability;
  const reliability = Math.min(successReliability, verifierReliability, acceptanceReliability);
  const recentSamples = stat?.recent_samples ?? Math.min(samples, 5);
  const recentFailureStreak =
    stat?.recent_failure_streak ?? Math.min(stat?.recent_failures ?? 0, 5);
  const recentSuccessStreak = stat?.recent_success_streak ?? 0;
  const retryExhaustionRate = boundedRatio(stat?.retry_exhaustions ?? 0, samples);
  const recentRetryExhaustionRate = boundedRatio(
    stat?.recent_retry_exhaustions ?? stat?.retry_exhaustions ?? 0,
    recentSamples
  );
  const latency = stat?.recent_average_latency_ms ?? stat?.average_latency_ms ?? null;
  const retries = stat?.recent_average_retries ?? stat?.average_retries ?? 0;
  const latencyPenalty = latency === null ? 0 : Math.min(30, latency / 30_000);
  const retryPenalty = Math.min(30, (retries ?? 0) * 10 + retryExhaustionRate * 30);
  const cost = stat?.average_cost ?? candidate.estimated_cost ?? null;
  const costPenalty = cost === null ? 5 : Math.min(10, Math.max(0, cost));

  let healthState = "HEALTHY";
  if (samples < minimumSamples) healthState = "PROBATION";
  else {
    const degraded =
      recentFailureStreak >= 2 ||
      recentRetryExhaustionRate >= 0.4 ||
      retryExhaustionRate >= 0.25 ||
      reliability < 0.65 ||
      latencyPenalty >= 10 ||
      (retries ?? 0) >= 0.75;
    const sustainedRecovery =
      recentSuccessStreak >= 3 &&
      reliability >= 0.7 &&
      recentRetryExhaustionRate === 0 &&
      latencyPenalty < 10 &&
      (retries === null || retries < 0.75);
    if (sustainedRecovery) healthState = "HEALTHY";
    else if (degraded && recentSuccessStreak >= 2) healthState = "PROBATION";
    else if (degraded) healthState = "DEGRADED";
  }

  return {
    verified: verifierReliability,
    acceptance: acceptanceReliability,
    success: successReliability,
    reliability,
    health: HEALTH_RANK[healthState],
    health_state: healthState,
    retry_exhaustion_rate: retryExhaustionRate,
    retry: -retryPenalty,
    retry_penalty: retryPenalty,
    latency: -latencyPenalty,
    latency_penalty: latencyPenalty,
    cost: -costPenalty,
    cost_penalty: costPenalty,
    preference_score: reliability * 100 - retryPenalty - latencyPenalty - costPenalty,
  };
}

function compare(left, right) {
  for (const key of ["acceptance", "verified", "success", "health", "retry", "latency", "cost"]) {
    if (left.score[key] !== right.score[key]) return right.score[key] - left.score[key];
  }
  return left.candidate.executor.localeCompare(right.candidate.executor);
}

export function routeTask(input) {
  if (input.permission_decision === "FORBIDDEN") throw new Error("Routing forbidden by policy");
  if (input.permission_decision === "REQUIRES_USER_APPROVAL") {
    throw new Error("Routing requires user approval");
  }
  const minimumSamples = input.minimum_samples ?? 5;
  const available = input.candidates.filter((candidate) =>
    eligible(candidate, input.required_tools ?? [], input.required_context_ids ?? [])
  );
  if (!available.length) throw new Error("No eligible executor candidates");
  const scored = available.map((candidate) => {
    const stat = matchingStat(
      input.statistics ?? [],
      input.task_type,
      input.tool_profile,
      candidate
    );
    const samples = stat?.samples ?? 0;
    return {
      candidate,
      stat,
      samples,
      score: scoreCandidate(stat, candidate, minimumSamples),
    };
  });
  const ranked = [...scored].sort(compare);
  const measuredRanked = ranked.filter((entry) => entry.samples >= minimumSamples);
  const fallbackEntry = scored.find(
    (entry) => entry.candidate.executor === input.conservative_fallback
  );
  let selectedEntry;
  let reason;
  const sufficientlyMeasured = measuredRanked.length > 0;
  const shouldExplore =
    input.safe_read_only === true &&
    input.mutating !== true &&
    input.requires_approval !== true &&
    input.exploration_interval > 0 &&
    input.exploration_sequence > 0 &&
    input.exploration_sequence % input.exploration_interval === 0;
  if (shouldExplore) {
    selectedEntry = [...scored].sort(
      (left, right) =>
        left.samples - right.samples ||
        left.candidate.executor.localeCompare(right.candidate.executor)
    )[0];
    reason = "bounded exploration of eligible under-sampled executor";
  } else if (!sufficientlyMeasured || (fallbackEntry && fallbackEntry.samples < minimumSamples)) {
    selectedEntry = fallbackEntry ?? ranked[0];
    reason = "conservative fallback: insufficient empirical samples";
  } else {
    selectedEntry = measuredRanked[0];
    reason = "highest measured verified completion and acceptance reliability";
  }
  const empiricalSelection = ranked[0]?.candidate.executor ?? null;
  const stat = selectedEntry.stat;
  const selectedSnapshot = {
    ...selectedEntry.candidate,
    tools: [...(selectedEntry.candidate.tools ?? [])],
    context_ids: [...(selectedEntry.candidate.context_ids ?? [])],
  };
  return {
    selected: selectedSnapshot,
    scored_candidates: ranked.map((entry) => ({
      executor: entry.candidate.executor,
      samples: entry.samples,
      expected_success: entry.stat?.smoothed_success ?? null,
      expected_latency: entry.stat?.average_latency_ms ?? null,
      expected_cost: entry.stat?.average_cost ?? entry.candidate.estimated_cost ?? null,
      empirical_reliability: entry.score.reliability,
      health_state: entry.score.health_state,
      retry_exhaustion_rate: entry.score.retry_exhaustion_rate,
      retry_penalty: entry.score.retry_penalty,
      latency_penalty: entry.score.latency_penalty,
      cost_penalty: entry.score.cost_penalty,
      preference_score: entry.score.preference_score,
    })),
    explanation: {
      ROUTE_TASK_TYPE: input.task_type,
      ROUTE_CANDIDATES: available.map((item) => item.executor),
      LAYA_USED: "NO",
      LAYA_MODEL: null,
      LAYA_LATENCY_MS: null,
      LAYA_DECISION: [],
      LAYA_CONFIDENCE: null,
      LAYA_FALLBACK: true,
      LAYA_RECOMMENDATION: null,
      EMPIRICAL_SELECTION: empiricalSelection,
      FINAL_SELECTION: selectedEntry.candidate.executor,
      SELECTED_EXECUTOR: selectedEntry.candidate.executor,
      SELECTED_MODEL: selectedEntry.candidate.model ?? null,
      SELECTION_REASON: reason,
      EXPECTED_COST: stat?.average_cost ?? selectedEntry.candidate.estimated_cost ?? null,
      EXPECTED_LATENCY: stat?.average_latency_ms ?? null,
      EXPECTED_SUCCESS: stat?.smoothed_success ?? null,
      EMPIRICAL_RELIABILITY: selectedEntry.score.reliability,
      EXECUTOR_HEALTH_STATE: selectedEntry.score.health_state,
      RETRY_PENALTY: selectedEntry.score.retry_penalty,
      LATENCY_PENALTY: selectedEntry.score.latency_penalty,
      PREFERENCE_SCORE: selectedEntry.score.preference_score,
      FALLBACK_REASON: sufficientlyMeasured ? null : "insufficient-data",
    },
  };
}
