/**
 * The two bundle entries share the views but not the standalone code: the Airflow plugin bundle
 * (main.tsx) must never reach src/standalone/ (token sign-in, sessionStorage, the Authorization
 * provider), so it stays small and never sends an Authorization header, which Airflow would read
 * before its own session cookie.
 */
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const SRC = dirname(fileURLToPath(import.meta.url));

// Static imports and re-exports (`import type` is erased by the build) and dynamic imports.
const IMPORT =
  /^\s*(?:import|export)(?!\s+type\s)[^"';]*?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)/gmu;

const EXTENSIONS = ["", ".ts", ".tsx", "/index.ts", "/index.tsx"];

/** The source file of an import of `from`, or undefined for a package. */
const resolveImport = (from: string, specifier: string): string | undefined => {
  let base: string;

  if (specifier.startsWith("src/")) {
    base = join(SRC, specifier.slice("src/".length));
  } else if (specifier.startsWith(".")) {
    base = resolve(dirname(from), specifier);
  } else {
    return undefined;
  }

  return EXTENSIONS.map((extension) => base + extension).find(
    (candidate) => existsSync(candidate) && /\.tsx?$/u.test(candidate),
  );
};

/** Every source file the entry reaches, relative to src/ (posix separators). */
const moduleGraph = (entry: string): Set<string> => {
  const seen = new Set<string>();
  const pending = [join(SRC, entry)];

  while (pending.length > 0) {
    const file = pending.pop() as string;
    const name = relative(SRC, file).split("\\").join("/");

    if (seen.has(name)) {
      continue;
    }
    seen.add(name);
    for (const match of readFileSync(file, "utf8").matchAll(IMPORT)) {
      const target = resolveImport(file, match[1] ?? match[2] ?? "");

      if (target !== undefined) {
        pending.push(target);
      }
    }
  }

  return seen;
};

const isStandalone = (name: string): boolean => name === "standalone.tsx" || name.startsWith("standalone/");

describe("bundle entries", () => {
  it("the Airflow plugin entry reaches the views but no standalone module", () => {
    const graph = moduleGraph("main.tsx");

    expect(graph).toContain("App.tsx");
    expect(graph).toContain("console/IncidentPage.tsx");
    expect(graph).toContain("vocabulary.ts");
    expect([...graph].filter(isStandalone)).toEqual([]);
  });

  it("the Airflow plugin entry never installs an authorization provider or touches sessionStorage", () => {
    for (const name of moduleGraph("main.tsx")) {
      const source = readFileSync(join(SRC, name), "utf8");

      expect(source, name).not.toMatch(/setAuthorizationProvider\(/u);
      expect(source, name).not.toMatch(/sessionStorage/u);
    }
  });

  it("the standalone entry renders the same App", () => {
    const graph = moduleGraph("standalone.tsx");

    expect(graph).toContain("App.tsx");
    expect(graph).toContain("standalone/StandaloneApp.tsx");
    expect(graph).toContain("standalone/auth.ts");
    expect(graph).not.toContain("main.tsx");
  });
});
