import { svelte } from "@sveltejs/vite-plugin-svelte";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [svelte()],
  build: { outDir: "../yakshed-desktop/frontend", emptyOutDir: true },
  clearScreen: false,
  server: { strictPort: true },
});
