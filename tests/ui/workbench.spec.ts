import { expect, test } from "@playwright/test";

test.describe("YakShed workbench", () => {
  test("creates a task tree, edits notes and todos, and restores an archive", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("button", { name: /New task/ }).click();
    await page.getByLabel("Title", { exact: true }).fill("Plan the next release");
    await page.getByLabel("Labels").fill("planning");
    await page.getByRole("button", { name: "Create task", exact: true }).click();
    await expect(page.getByText("Plan the next release", { exact: true }).first()).toBeVisible();

    await page.keyboard.press("Shift+c");
    await expect(page.getByRole("dialog", { name: "Create task" })).toBeVisible();
    await page.getByLabel("Title", { exact: true }).fill("Check the release notes");
    await page.getByRole("button", { name: "Create task", exact: true }).click();
    await expect(page.getByText("Check the release notes", { exact: true }).first()).toBeVisible();

    await page.getByRole("button", { name: /close todo rail/i }).click();
    await page.getByRole("button", { name: /Todos/ }).click();
    await page.getByLabel("New todo").fill("Review the changelog");
    await page.getByLabel("New todo").press("Enter");
    await expect(page.getByText("Review the changelog", { exact: true })).toBeVisible();
    await page.getByRole("button", { name: "edit", exact: true }).click();
    await page.getByPlaceholder("Markdown notes for this task").fill("## Release\nKeep the first pass small.");
    await page.getByRole("button", { name: "done", exact: true }).click();
    await expect(page.getByRole("button", { name: /Keep the first pass small/ })).toBeVisible();

    await page.locator(".header-actions").getByRole("button", { name: /^Archive/ }).click();
    await expect(page.getByRole("status")).toContainText("archived");
    await page.getByRole("button", { name: /Undo/ }).click();
    await expect(page.getByText("Check the release notes", { exact: true }).first()).toBeVisible();
  });

  test("switches palettes, opens Reader, searches archived tasks, and survives reload", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("button", { name: /New task/ }).click();
    await page.getByLabel("Title", { exact: true }).fill("Reader persistence check");
    await page.getByRole("button", { name: "Create task", exact: true }).click();
    await page.getByRole("button", { name: "Open appearance palette" }).click();
    await expect(page.getByRole("dialog", { name: "YakShed settings" })).toBeVisible();
    await page.getByRole("button", { name: "Appearance", exact: true }).click();
    await page.getByRole("button", { name: /Ledger/ }).click();
    await page.getByRole("dialog", { name: "YakShed settings" }).getByRole("button", { name: /Close/ }).click();
    await page.locator(".header-actions").getByRole("button", { name: /^Reader/ }).click();
    await expect(page.getByRole("complementary", { name: "Reader" })).toBeVisible();
    await page.keyboard.press("/");
    await page.locator("#search-input").fill("Reader persistence");
    await page.locator("#search-input").press("Enter");
    await expect(page.getByText("Reader persistence check", { exact: true }).first()).toBeVisible();
    await page.reload();
    await expect(page.getByText("Reader persistence check", { exact: true }).first()).toBeVisible();
  });

  test("runs through approval, opens the resulting artifact, and can stop a run", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("button", { name: /New task/ }).click();
    await page.getByLabel("Title", { exact: true }).fill("Approval and stop journey");
    await page.getByRole("button", { name: "Create task", exact: true }).click();
    const persistedReader = page.getByRole("complementary", { name: "Reader" });
    if (await persistedReader.count()) await persistedReader.getByRole("button", { name: /close/ }).click();

    await page.getByRole("button", { name: "Open settings" }).click();
    await page.getByRole("dialog", { name: "YakShed settings" }).getByRole("button", { name: "Connections", exact: true }).click();
    await page.getByRole("button", { name: /Add connection/ }).click();
    await page.getByLabel("Connection name").fill("Fixture connection");
    await page.getByLabel("Adapter").selectOption({ label: "Deterministic demo" });
    await page.getByRole("button", { name: "Add connection", exact: true }).click();
    await page.getByRole("dialog", { name: "YakShed settings" }).getByRole("button", { name: /Close/ }).click();

    await page.getByLabel("Task prompt").fill("Inspect the fixture");
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await expect(page.getByText("Waiting for your approval", { exact: true })).toBeVisible({ timeout: 10_000 });
    await page.getByRole("button", { name: "Approve", exact: true }).click();
    await expect(page.getByRole("button", { name: "Approve", exact: true })).toHaveCount(0, { timeout: 10_000 });
    await expect(page.getByRole("button", { name: /OPEN IN READER/ })).toBeVisible({ timeout: 10_000 });

    await page.locator(".header-actions").getByRole("button", { name: /^Reader/ }).click();
    await expect(page.getByRole("complementary", { name: "Reader" })).toContainText("demo-report.txt");
    await page.getByRole("complementary", { name: "Reader" }).getByRole("button", { name: /close/ }).click();

    await page.keyboard.press("c");
    await page.getByRole("dialog", { name: "Create task" }).getByLabel("Title", { exact: true }).fill("Context switch task");
    await page.getByRole("dialog", { name: "Create task" }).getByRole("button", { name: "Create task", exact: true }).click();
    await expect(page.locator("button.workspace-button")).toHaveAttribute("title", "Choose workspace");
    await page.locator(".task-nav .task-main").filter({ hasText: "Approval and stop journey" }).click();
    await expect(page.locator("button.workspace-button")).toHaveAttribute("title", /workspace$/);
    await page.reload();
    const reloadedReader = page.getByRole("complementary", { name: "Reader" });
    if (await reloadedReader.count()) await reloadedReader.getByRole("button", { name: /close/ }).click();
    await page.locator(".task-nav .task-main").filter({ hasText: "Approval and stop journey" }).click();
    await expect(page.locator("button.workspace-button")).toHaveAttribute("title", /workspace$/);

    await page.getByLabel("Task prompt").fill("Stop this run");
    await page.getByRole("button", { name: "Run", exact: true }).click();
    await expect(page.getByRole("button", { name: "Stop", exact: true })).toBeVisible({ timeout: 10_000 });
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    await expect(page.getByText("Run interrupted.", { exact: true })).toBeVisible({ timeout: 10_000 });
  });
});
