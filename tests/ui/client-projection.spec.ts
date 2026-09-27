import { expect, test } from "@playwright/test";
import { eventText, latestTaskContext, projectTimeline, type Detail, type TimelineEvent } from "../../frontend/src/client";

test("projects assistant deltas per run and replaces the completed text", () => {
  const events: TimelineEvent[] = [
    { id: "a1", type: "assistant", run_id: "run-a", payload: { kind: "assistant", message_id: "message-1", phase: "delta", delta: "delta 1" } },
    { id: "a2", type: "assistant", run_id: "run-a", payload: { kind: "assistant", message_id: "message-1", phase: "delta", delta: " + delta 2" } },
    { id: "a3", type: "assistant", run_id: "run-a", payload: { kind: "assistant", message_id: "message-1", phase: "completed", text: "complete answer" } },
    { id: "b1", type: "assistant", run_id: "run-b", payload: { kind: "assistant", message_id: "message-1", phase: "delta", delta: "next run" } },
  ];

  const projected = projectTimeline(events);
  expect(projected).toHaveLength(2);
  expect(projected.map((event) => event.run_id)).toEqual(["run-a", "run-b"]);
  expect(eventText(projected[0])).toBe("complete answer");
  expect(eventText(projected[1])).toBe("next run");
});

test("restores the latest run workspace with its session connection", () => {
  const context = latestTaskContext({
    runs: [
      { id: "run-a", session_id: "session-a", requested_options: { workspace: "/repoA" } },
      { id: "run-b", session_id: "session-b", requested_options: { workspace: "/repoB" } },
    ],
    sessions: [
      { id: "session-a", connection_id: "connection-a", requested_options: { workspace: "/repoA" } },
      { id: "session-b", connection_id: "connection-b", requested_options: { workspace: "/repoB" } },
    ],
  } as Pick<Detail, "runs" | "sessions">);

  expect(context).toEqual({ workspace: "/repoB", connectionId: "connection-b" });
});

test("restores a child workspace reported by the session effective options", () => {
  const context = latestTaskContext({
    runs: [
      { id: "child-run", task_id: "child-task", session_id: "child-session", requested_options: {} },
    ],
    sessions: [
      {
        id: "child-session",
        connection_id: "connection-child",
        effective_options: { workspace: "/child-repo" },
        requested_options: { workspace: "/stale-requested-repo" },
      },
    ],
  } as Pick<Detail, "runs" | "sessions">);

  expect(context).toEqual({ workspace: "/child-repo", connectionId: "connection-child" });
});
