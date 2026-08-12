import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    const now = Date.now();
    let revision = 1;
    let configRevision = 0;
    let workItem: Record<string, unknown> | null = null;
    let connection: Record<string, unknown> | null = null;
    let account: Record<string, unknown> = { state: "not_authenticated" };
    let run: Record<string, unknown> | null = null;
    let approvals: Array<Record<string, unknown>> = [];
    let inputs: Array<Record<string, unknown>> = [];
    let timeline: Array<Record<string, unknown>> = [];
    const listeners = new Set<(event: { payload: unknown }) => void>();
    const emit = (kind: Record<string, unknown>) => {
      const payload = { work_item_id: workItem?.id, revision, kind };
      listeners.forEach((handler) => handler({ payload }));
    };
    const snapshot = () => ({ revision, work_item: { ...workItem, revision }, runs: run ? [run] : [], next_run_after: null });

    const invokeMock = async (command: string, args: Record<string, unknown> = {}): Promise<unknown> => {
        switch (command) {
          case "create_project": return undefined;
          case "list_connections":
            if (sessionStorage.getItem("startup-fail")) throw { code: "persistence_error", message: "persistence startup failed" };
            return {
              config_revision: configRevision,
              connections: connection ? [connection] : [],
              credential_migration: sessionStorage.getItem("migration-pending")
                ? { status: "pending", reason: "locked" }
                : { status: "ready" },
            };
          case "connection_put":
            connection = args.connection as Record<string, unknown>;
            configRevision += 1;
            return { config_revision: configRevision, connection };
          case "set_connection_credential": return { overwritten: true };
          case "account_status":
            if (sessionStorage.getItem("codex-missing")) throw { code: "unsupported", message: "Codex unavailable" };
            if (account.state === "login_in_progress") {
              account = { state: "authenticated", email: "yak@example.test", plan: "plus" };
            }
            return account;
          case "account_login_start":
            account = { state: "login_in_progress", login_id: "login-1", auth_url: "https://auth.example.test/login-1" };
            return account;
          case "account_logout": account = { state: "not_authenticated" }; return undefined;
          case "list_work_items": return { items: workItem ? [{ work_item: { ...workItem, revision }, revision }] : [], next_after: null };
          case "create_work_item":
            workItem = { id: "work-1", project_id: args.projectId, title: args.title, status: "active", parent_id: null, revision, created_at_ms: now, updated_at_ms: now };
            return snapshot();
          case "get_work_item_snapshot": return snapshot();
          case "get_work_item_timeline_page_at_revision":
          case "get_work_item_timeline_page": return { run_id: run?.id, work_item_revision: revision, items: timeline, next_after: null };
          case "get_run_approval_page": return { work_item_revision: revision, approvals, next_after: null };
          case "get_pending_user_input_page": return { work_item_revision: revision, inputs, next_after: null };
          case "start_run": {
            if (account.state !== "authenticated") throw { code: "not_authenticated", message: "Codex account is not authenticated" };
            run = { id: "run-1", connection_id: connection?.id, work_item_id: workItem?.id, status: "running", created_at_ms: now, ended_at_ms: null };
            revision += 2;
            timeline = [
              { id: "t1", connection_id: connection?.id, run_id: "run-1", revision: 1, kind: "message", body: "Planning the work", created_at_ms: now },
              { id: "t2", connection_id: connection?.id, run_id: "run-1", revision: 2, kind: "command_output", body: "Checked the workspace", created_at_ms: now + 1 },
              { id: "t3", connection_id: connection?.id, run_id: "run-1", revision: 3, kind: "message", body: "Ready for approval", created_at_ms: now + 2 },
            ];
            setTimeout(() => emit({ type: "timeline_batch_appended", run_id: "run-1", item_count: 3 }), 20);
            setTimeout(() => {
              revision += 1;
              approvals = [{ id: "approval-1", connection_id: connection?.id, run_id: "run-1", kind: "command", summary: "Apply the planned changes?", status: "pending", decision: null, requested_at_ms: now, response_started_at_ms: null, resolved_at_ms: null, voided_at_ms: null }];
              emit({ type: "approval_opened", run_id: "run-1", approval_id: "approval-1" });
            }, 60);
            return "run-1";
          }
          case "resolve_approval":
            approvals = approvals.map((approval) => ({ ...approval, status: "resolved", decision: args.decision }));
            revision += 1;
            emit({ type: "approval_resolved", run_id: "run-1", approval_id: args.approvalId });
            setTimeout(() => {
              revision += 1;
              inputs = [{ id: "input-1", run_id: "run-1", prompt: "Which environment should I target?" }];
              emit({ type: "user_input_opened", run_id: "run-1", request_id: "input-1", prompt: inputs[0].prompt });
            }, 20);
            return undefined;
          case "respond_user_input":
            inputs = [];
            revision += 1;
            timeline = [...timeline, { id: "t4", connection_id: connection?.id, run_id: "run-1", revision: 4, kind: "message", body: `Target: ${args.response}`, created_at_ms: now + 3 }];
            emit({ type: "user_input_responded", run_id: "run-1", request_id: args.requestId });
            return undefined;
          case "steer_run":
            revision += 1;
            timeline = [...timeline, { id: "t5", connection_id: connection?.id, run_id: "run-1", revision: 5, kind: "message", body: `Steered: ${args.message}`, created_at_ms: now + 4 }];
            emit({ type: "timeline_batch_appended", run_id: "run-1", item_count: 1 });
            return undefined;
          case "interrupt_run":
            if (run) run = { ...run, status: "outcome_unknown" };
            revision += 1;
            emit({ type: "run_outcome_unknown", run_id: "run-1", operation: "interrupt" });
            throw { code: "outcome_unknown", message: "operation outcome is uncertain", detail: "interrupt" };
          case "reconcile_run":
            if (run) run = { ...run, status: "interrupted", ended_at_ms: Date.now() };
            return run;
          default: throw new Error(`Unexpected command: ${command}`);
        }
    };
    window.__YAKSHED_MOCK__ = {
      listen: async <T>(_event: string, handler: (event: { payload: T }) => void) => {
        const wrapped = handler as (event: { payload: unknown }) => void;
        listeners.add(wrapped);
        return () => listeners.delete(wrapped);
      },
      invoke: <T>(command: string, args?: Record<string, unknown>) => invokeMock(command, args) as Promise<T>,
    };
  });
});

test("runs the product loop through approval, input, interrupt, and reconciliation", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("button", { name: /Connections/ }).click();
  await page.getByLabel("Name").fill("Scripted mock");
  await page.getByLabel("Model provider").fill("codex");
  await page.getByRole("button", { name: "Add connection" }).click();
  await expect(page.getByText("Codex is not authenticated for this connection.")).toBeVisible();
  await page.getByRole("button", { name: "Sign in with Codex" }).click();
  await expect(page.getByRole("link", { name: "Continue sign-in" })).toHaveAttribute("href", "https://auth.example.test/login-1");
  await page.getByRole("button", { name: "Refresh status" }).click();
  await expect(page.getByText("Signed in as yak@example.test · plus")).toBeVisible();
  await page.getByRole("button", { name: "Close connections" }).click();

  await page.getByLabel("New work item").fill("Ship the desktop shell");
  await page.getByRole("button", { name: "Create" }).click();
  await expect(page.getByRole("heading", { name: "Ship the desktop shell" })).toBeVisible();

  await page.getByLabel("Start a run").fill("Build and verify the shell");
  await page.getByRole("button", { name: "Start run" }).click();
  await expect(page.getByText("Batch received · 3 updates")).toBeVisible();
  await expect(page.getByRole("button", { name: /Ship the desktop shell/ })).toContainText("rev 3");
  await expect(page.getByText("Checked the workspace")).toBeVisible();

  await expect(page.getByRole("heading", { name: "Apply the planned changes?" })).toBeVisible();
  await page.getByRole("button", { name: "Approve" }).click();
  await expect(page.getByRole("heading", { name: "Which environment should I target?" })).toBeVisible();
  await page.getByLabel("Response").fill("local smoke");
  await page.getByRole("button", { name: "Send response" }).click();
  await expect(page.getByText("Target: local smoke")).toBeVisible();

  await page.getByLabel("Steer this run").fill("Keep the diff small");
  await page.getByRole("button", { name: "Steer" }).click();
  await expect(page.getByText("Steered: Keep the diff small")).toBeVisible();
  await page.getByRole("button", { name: "Interrupt" }).click();
  await expect(page.getByText("OUTCOME UNCERTAIN")).toBeVisible();
  await expect(page.getByText(/Interrupt outcome is unknown/)).toBeVisible();
  await page.getByRole("button", { name: "Reconcile" }).click();
  await expect(page.getByText("interrupted", { exact: true })).toBeVisible();

  await page.evaluate(() => sessionStorage.setItem("startup-fail", "1"));
  await page.reload();
  await expect(page.getByRole("heading", { name: "YakShed could not open." })).toBeVisible();
  await expect(page.getByText("STARTUP FAILED · persistence_error")).toBeVisible();
});

test("surfaces a locked keychain migration without failing startup", async ({ page }) => {
  await page.goto("/");
  await page.evaluate(() => sessionStorage.setItem("migration-pending", "1"));
  await page.reload();

  await expect(page.getByText("CREDENTIAL MIGRATION PENDING")).toBeVisible();
  await expect(page.getByText(/Keychain migration is locked/)).toBeVisible();
  await expect(page.getByText("STARTUP FAILED", { exact: false })).toHaveCount(0);
});

test("a missing Codex runtime leaves the account status unknown without failing startup", async ({ page }) => {
  await page.addInitScript(() => sessionStorage.setItem("codex-missing", "1"));
  await page.goto("/");
  await page.getByRole("button", { name: /Connections/ }).click();
  await page.getByLabel("Name").fill("Codex unavailable");
  await page.getByLabel("Model provider").fill("codex");
  await page.getByRole("button", { name: "Add connection" }).click();
  await expect(page.getByText("Codex account status is unknown.")).toBeVisible();
  await expect(page.getByText("YakShed could not open.")).not.toBeVisible();
});
