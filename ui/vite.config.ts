import react from "@vitejs/plugin-react-swc";
import { resolve } from "node:path";
import cssInjectedByJsPlugin from "vite-plugin-css-injected-by-js";
import dts from "vite-plugin-dts";
import { defineConfig } from "vitest/config";

// Airflow API server used by the dev server proxy (`pnpm dev`).
const airflowUrl = process.env.HEALER_DEV_AIRFLOW_URL ?? "http://localhost:8080";

// https://vitejs.dev/config/
export default defineConfig(({ command, mode }) => {
  const isLibraryBuild = command === "build";
  // Vitest applies `process.env.*` defines at runtime; forcing "production" there would load React's
  // production jsx-dev-runtime (no jsxDEV) and break every component test.
  const isTest = mode === "test";

  return {
    base: "./",
    build: isLibraryBuild
      ? {
          chunkSizeWarningLimit: 1600,
          lib: {
            entry: resolve("src", "main.tsx"),
            fileName: "main",
            formats: ["umd"],
            // UMD global for the module object. The Airflow host does NOT read it: it reads
            // globalThis.DagHealerNav / DagHealerDag / DagHealerTaskInstance, which src/main.tsx
            // assigns at evaluation time. Never use "AirflowPlugin" here (shared by all plugins).
            name: "DagHealer",
          },
          rollupOptions: {
            // Shared with the host Airflow UI, which exposes them on globalThis.
            external: ["react", "react-dom", "react-router-dom", "react/jsx-runtime"],
            output: {
              // Named exports only: the module object (globalThis.DagHealer) is informational.
              exports: "named",
              globals: {
                react: "React",
                "react-dom": "ReactDOM",
                "react-router-dom": "ReactRouterDOM",
                "react/jsx-runtime": "ReactJSXRuntime",
              },
            },
          },
        }
      : {
          // Development build configuration
          chunkSizeWarningLimit: 1600,
        },
    define: isTest
      ? { global: "globalThis" }
      : {
          global: "globalThis",
          "process.env": "{}",
          // Define process.env for browser compatibility
          "process.env.NODE_ENV": JSON.stringify("production"),
        },
    plugins: [
      react(),
      cssInjectedByJsPlugin(),
      ...(isLibraryBuild
        ? [
            dts({
              include: ["src/main.tsx", "src/global.d.ts"],
              insertTypesEntry: true,
              outDir: "dist",
              tsconfigPath: "./tsconfig.lib.json",
            }),
          ]
        : []),
    ],
    resolve: { alias: { src: "/src" } },
    server: {
      cors: true, // Only used by the dev server.
      proxy: {
        // The healer API and static files live on the Airflow API server. changeOrigin stays false so
        // the browser-facing host is preserved and the Airflow `_token` cookie keeps flowing.
        "/healer": {
          changeOrigin: false,
          target: airflowUrl,
        },
      },
    },
    test: {
      coverage: {
        include: ["src/**/*.ts", "src/**/*.tsx"],
      },
      css: true,
      environment: "happy-dom",
      globals: true,
      mockReset: true,
      passWithNoTests: true,
      restoreMocks: true,
      setupFiles: "./testsSetup.ts",
    },
  };
});
