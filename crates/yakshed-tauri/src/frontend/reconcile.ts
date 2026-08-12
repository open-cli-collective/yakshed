import type { FrontendEvent, WorkItemSnapshot } from "./client";

export function needsSnapshot(localRevision: number, event: FrontendEvent): boolean {
  return event.revision > localRevision;
}

export async function reconcileSnapshot(
  current: WorkItemSnapshot,
  event: FrontendEvent,
  fetchSnapshot: () => Promise<WorkItemSnapshot>,
): Promise<WorkItemSnapshot> {
  if (!needsSnapshot(current.revision, event)) return current;
  const refreshed = await fetchSnapshot();
  return refreshed.revision >= current.revision ? refreshed : current;
}
