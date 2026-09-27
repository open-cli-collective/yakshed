<script lang="ts">
  export let label = "";
  export let description = "";
  export let value = false;
  export let ontoggle: (value: boolean) => void = () => undefined;
</script>

<div class="setting-row"><div><strong>{label}</strong><small>{description}</small></div><button class:enabled={value} class="toggle" type="button" aria-label={label} aria-pressed={value} onclick={() => ontoggle(!value)}><span></span></button></div>
