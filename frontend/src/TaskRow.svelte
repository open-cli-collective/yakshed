<script lang="ts">
  import type { Task } from "./client";

  export let row: { task: Task; depth: number };
  export let selectedId: string | null = null;
  export let focusZone: "threads" | "todos" = "threads";
  export let picked = false;
  export let labels: Array<{ name: string; color: string; own: boolean }> = [];
  export let hasChildren = false;
  export let folded = false;
  export let onopen: () => void = () => undefined;
  export let ontoggle: () => void = () => undefined;
  export let onfold: () => void = () => undefined;

  function statusName(task: Task): string {
    if (task.archived) return "archived";
    if (task.status === "active") return "working";
    if (task.status === "waiting") return "waiting";
    if (task.status === "done") return "done";
    return "inbox";
  }
</script>

<div class:selected={selectedId === row.task.id} class:focused={focusZone === "threads" && selectedId === row.task.id} class:archived={row.task.archived} class="task-row" style={`padding-left:${10 + row.depth * 17}px`}>
  <button class:checked={picked} class="task-pick" type="button" aria-label={`Select ${row.task.title}`} onclick={(event) => { event.stopPropagation(); ontoggle(); }}>{picked ? "✓" : ""}</button>
  <button class="fold-button" type="button" aria-label={folded ? "Unfold children" : "Fold children"} onclick={(event) => { event.stopPropagation(); onfold(); }}>{hasChildren ? (folded ? "▸" : "▾") : ""}</button>
  <button class={`task-main ${row.task.status}`} type="button" onclick={onopen}><span class="task-dot"></span><span class="task-copy"><strong>{row.task.title}</strong><small>{statusName(row.task)}</small></span>{#if labels[0]}<span class:inherited={!labels[0].own} class="mini-chip" style={`--chip-color:${labels[0].color}`}>{labels[0].name}</span>{/if}</button>
</div>
