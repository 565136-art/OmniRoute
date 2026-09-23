export const TASK_CONTRACT_VERSION = 2 as const;

export interface TaskContractV1 {
  contract_version: 1 | 2;
  mission_id: string;
  task_id: string;
  correlation_id: string;
  attempt_id?: string;
  request_id?: string;
  parent_task_id?: string;
}

const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$/;
const LEGACY_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,199}$/;

function readId(headers: Headers, name: string, pattern = ID_PATTERN): string | null {
  const value = headers.get(name);
  if (value === null) return null;
  const normalized = value.trim();
  return pattern.test(normalized) ? normalized : null;
}

export function readTaskContract(headers: Headers): TaskContractV1 | null {
  const version = headers.get("x-omniroute-contract-version");
  if (version !== "1" && version !== String(TASK_CONTRACT_VERSION)) return null;
  const pattern = version === "1" ? LEGACY_ID_PATTERN : ID_PATTERN;
  const missionId = readId(headers, "x-omniroute-mission-id", pattern);
  const taskId = readId(headers, "x-omniroute-task-id", pattern);
  const correlationId = readId(headers, "x-omniroute-correlation-id", pattern);
  const parentHeader = headers.get("x-omniroute-parent-task-id");
  const parentTaskId =
    parentHeader === null ? null : readId(headers, "x-omniroute-parent-task-id", pattern);
  if (!missionId || !taskId || !correlationId) return null;
  if (parentHeader !== null && !parentTaskId) return null;
  if (version === "1") {
    if (taskId !== correlationId) return null;
    return {
      contract_version: 1,
      mission_id: missionId,
      task_id: taskId,
      correlation_id: correlationId,
      ...(parentTaskId ? { parent_task_id: parentTaskId } : {}),
    };
  }
  const attemptId = readId(headers, "x-omniroute-attempt-id", pattern);
  const requestId = readId(headers, "x-omniroute-request-id", pattern);
  if (!attemptId || !requestId || attemptId === correlationId || requestId !== correlationId)
    return null;
  return {
    contract_version: TASK_CONTRACT_VERSION,
    mission_id: missionId,
    task_id: taskId,
    attempt_id: attemptId,
    request_id: requestId,
    correlation_id: correlationId,
    ...(parentTaskId ? { parent_task_id: parentTaskId } : {}),
  };
}

export function resolveCallLogCorrelationId(headers: Headers, apiRequestId: string): string {
  return readTaskContract(headers)?.correlation_id ?? apiRequestId;
}
