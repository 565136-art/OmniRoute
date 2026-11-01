import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

test("external-output ingestion contract (isolated subprocess)", () => {
  const root = fileURLToPath(new URL("../../", import.meta.url));
  const childEnv: Record<string, string | undefined> = {
    ...process.env,
    DISABLE_SQLITE_AUTO_BACKUP: "true",
  };
  delete childEnv.NODE_TEST_CONTEXT;
  const result = spawnSync(
    process.execPath,
    [
      "--experimental-test-module-mocks",
      "--import",
      "tsx/esm",
      "--import",
      "./open-sse/utils/setupPolyfill.ts",
      "--import",
      "./tests/_setup/isolateDataDir.ts",
      "--import",
      "./tests/_setup/mock-inference.mjs",
      "--test",
      "--test-reporter=tap",
      "tests/fixtures/evals-outputs-ingestion.cases.ts",
    ],
    {
      cwd: root,
      env: childEnv,
      encoding: "utf8",
      timeout: 60000,
      maxBuffer: 4 * 1024 * 1024,
    }
  );
  assert.ifError(result.error);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  assert.match(result.stdout, /# tests 11(?:\r?\n|$)/, result.stdout);
  assert.match(result.stdout, /# pass 11(?:\r?\n|$)/, result.stdout);
  assert.match(result.stdout, /# fail 0(?:\r?\n|$)/, result.stdout);
});
