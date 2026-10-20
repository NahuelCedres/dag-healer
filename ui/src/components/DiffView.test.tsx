import { ChakraProvider } from "@chakra-ui/react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { diffStats, parseDiff } from "src/diff";
import { DIFF } from "src/test/fixtures";
import { localSystem } from "src/theme";

import { DiffView } from "./DiffView";

describe("parseDiff", () => {
  it("classifies file headers, hunks, additions, deletions and context", () => {
    const lines = parseDiff(DIFF);

    expect(lines.map((line) => line.kind)).toEqual([
      "file",
      "meta",
      "meta",
      "meta",
      "hunk",
      "context",
      "del",
      "add",
      "context",
    ]);
  });

  it("numbers old and new lines from the hunk header", () => {
    const lines = parseDiff(DIFF);

    expect(lines[5]).toMatchObject({ newNumber: 10, oldNumber: 10 });
    expect(lines[6]).toMatchObject({ kind: "del", oldNumber: 11 });
    expect(lines[6]?.newNumber).toBeUndefined();
    expect(lines[7]).toMatchObject({ kind: "add", newNumber: 11 });
    expect(lines[8]).toMatchObject({ newNumber: 12, oldNumber: 12 });
  });

  it("handles CRLF, new files and the no-newline marker", () => {
    const diff = [
      "diff --git a/tests/test_x.py b/tests/test_x.py",
      "new file mode 100644",
      "--- /dev/null",
      "+++ b/tests/test_x.py",
      "@@ -0,0 +1,2 @@",
      "+def test_x():",
      "+    assert True",
      "\\ No newline at end of file",
    ].join("\r\n");
    const lines = parseDiff(diff);

    expect(lines.map((line) => line.kind)).toEqual([
      "file",
      "meta",
      "meta",
      "meta",
      "hunk",
      "add",
      "add",
      "meta",
    ]);
    expect(lines[5]?.text).toBe("+def test_x():");
  });

  it("returns nothing for empty input", () => {
    expect(parseDiff("")).toEqual([]);
    expect(parseDiff(null)).toEqual([]);
  });
});

describe("diffStats", () => {
  it("counts files and changed lines", () => {
    expect(diffStats(DIFF)).toEqual({ additions: 1, deletions: 1, files: ["src/etl/customers.py"] });
  });

  it("collects every file of a multi-file diff once", () => {
    const diff = `${DIFF}diff --git a/tests/test_x.py b/tests/test_x.py\n--- /dev/null\n+++ b/tests/test_x.py\n@@ -0,0 +1 @@\n+x\n`;

    expect(diffStats(diff)).toEqual({
      additions: 2,
      deletions: 1,
      files: ["src/etl/customers.py", "tests/test_x.py"],
    });
  });
});

describe("DiffView", () => {
  const renderDiff = (diff: string | null) =>
    render(
      <ChakraProvider value={localSystem}>
        <DiffView diff={diff} />
      </ChakraProvider>,
    );

  it("renders colored +/- lines in a monospace region with a summary", () => {
    renderDiff(DIFF);
    const region = screen.getByTestId("diff-view");
    const added = region.querySelector('[data-kind="add"]') as HTMLElement;
    const removed = region.querySelector('[data-kind="del"]') as HTMLElement;

    expect(added).toHaveTextContent('+    return datetime.fromisoformat(value.replace("Z", "+00:00"))', {
      normalizeWhitespace: false,
    });
    expect(removed).toHaveTextContent('-    return datetime.strptime(value, "%Y-%m-%d")', {
      normalizeWhitespace: false,
    });
    // Added and removed lines get different styles (semantic success / error tokens).
    expect(added.className).not.toBe(removed.className);
    expect(added.className).not.toBe(
      (region.querySelector('[data-kind="context"]') as HTMLElement).className,
    );
    expect(screen.getByText("1 file changed")).toBeInTheDocument();
    expect(screen.getByText("+1")).toBeInTheDocument();
    expect(screen.getByText("-1")).toBeInTheDocument();
  });

  it("says when there is nothing to show", () => {
    renderDiff(null);
    expect(screen.getByText("No changes.")).toBeInTheDocument();
  });
});
