import { describe, expect, it } from "vitest";

import { DRAFT_YAML, PR_MD, PYTEST_OUTPUT } from "src/test/fixtures";

import { ciSection, markdownBlocks, pytestResults, yamlRuleRange } from "./texts";
import { relativeTime, shortAge } from "./time";

describe("pytestResults", () => {
  it("reads the runs before and after the fix", () => {
    expect(pytestResults(PYTEST_OUTPUT)).toEqual({
      after: { command: "pytest tests/test_customers_signup_date.py", passed: true, summary: "2 passed" },
      before: {
        command: "pytest tests/test_customers_signup_date.py",
        passed: false,
        summary: "1 failed, 1 passed",
      },
    });
  });

  it("is empty without output", () => {
    expect(pytestResults(null)).toEqual({});
    expect(pytestResults("1 passed")).toEqual({});
  });
});

describe("the CI section of PR.md", () => {
  it("is cut at the next heading, with Markdown escapes undone", () => {
    const ci = ciSection(PR_MD);

    expect(ci).toContain("Its commit carries [skip ci], so");
    expect(ci).not.toContain("Checklist");
    expect(ciSection("# Fix\n\nno CI section")).toBeUndefined();
  });

  it("keeps code blocks apart from the paragraphs", () => {
    expect(markdownBlocks(ciSection(PR_MD) ?? "")).toEqual([
      {
        text: "CI has not run for this change. Its commit carries [skip ci], so GitHub starts no workflow for it.",
      },
      { text: "After your review, start CI with an empty commit:" },
      { code: "git push --no-verify origin healer/customers_sync-transform-a1b2c3d4" },
    ]);
  });
});

describe("yamlRuleRange", () => {
  const lines = DRAFT_YAML.split("\n");
  const text = (range?: { first: number; last: number }) =>
    range === undefined ? undefined : lines.slice(range.first, range.last + 1).map((line) => line.trim());

  it("finds the rule of a column", () => {
    expect(text(yamlRuleRange(DRAFT_YAML, "inventory", "qty"))).toEqual([
      "- name: qty",
      "dtype: integer",
      "nullable: false",
      "min: 0.0",
      "max: 3100.0",
    ]);
  });

  it("finds a dataset-level key", () => {
    expect(text(yamlRuleRange(DRAFT_YAML, "inventory", undefined, "row_count"))).toEqual([
      "row_count:",
      "min: 15",
      "max: 100",
    ]);
    expect(text(yamlRuleRange(DRAFT_YAML, "inventory", undefined, "allow_extra_columns"))).toEqual([
      "allow_extra_columns: true",
    ]);
  });

  it("is undefined for a column the file does not hold", () => {
    expect(yamlRuleRange(DRAFT_YAML, "inventory", "quantity")).toBeUndefined();
  });
});

describe("times", () => {
  const now = Date.parse("2026-10-17T12:00:00Z");

  it("read as people say them", () => {
    expect(relativeTime("2026-10-17T11:59:40Z", now)).toBe("just now");
    expect(relativeTime("2026-10-17T11:56:00Z", now)).toBe("4 min ago");
    expect(relativeTime("2026-10-17T09:00:00Z", now)).toBe("3 h ago");
    expect(relativeTime("2026-10-16T09:00:00Z", now)).toBe("1 day ago");
    expect(relativeTime("2026-10-01T09:00:00Z", now)).toBe("Oct 1");
    expect(relativeTime(null, now)).toBe("");
    expect(shortAge("2026-10-17T11:56:00Z", now)).toBe("4m");
    expect(shortAge("2026-10-15T11:56:00Z", now)).toBe("2d");
  });
});
