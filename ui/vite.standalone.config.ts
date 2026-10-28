import react from "@vitejs/plugin-react-swc";
import { resolve } from "node:path";
import { defineConfig } from "vite";
import cssInjectedByJsPlugin from "vite-plugin-css-injected-by-js";

// Standalone bundle (`pnpm build:standalone`): the ES-module app loaded by the page shell of
// `dag-healer serve`. Unlike vite.config.ts (the Airflow plugin's UMD library, with React from the
// host), nothing is external: React and ReactDOM are bundled, because the shell is
// a plain page with no host globals. File names carry no hash, so a rebuild gives a clean diff of the
// copy committed in src/dag_healer/api/static/standalone/ (the shell loads index.js?v=<version>, and
// relative imports resolve against the module URL). Run it after `pnpm build`, which empties dist/.
export default defineConfig({
  base: "./",
  build: {
    chunkSizeWarningLimit: 1600,
    emptyOutDir: true,
    outDir: "dist/standalone",
    rolldownOptions: {
      input: resolve("src", "standalone.tsx"),
      output: {
        assetFileNames: "assets/[name][extname]",
        chunkFileNames: "assets/[name].js",
        entryFileNames: "index.js",
        format: "es",
      },
    },
  },
  define: {
    global: "globalThis",
    "process.env": "{}",
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  plugins: [react(), cssInjectedByJsPlugin()],
  publicDir: false,
  resolve: { alias: { src: "/src" } },
});
