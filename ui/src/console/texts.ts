/**
 * Readings of the texts the worker writes: the pytest output of a repair (before and after the
 * fix), the CI section of PR.md, and the lines of a contract file that hold one rule.
 */

export interface PytestRun {
  /** The command line, without the `$ `. */
  readonly command?: string;
  /** True when pytest exited with 0. */
  readonly passed: boolean;
  /** "1 failed, 1 passed" (the counts that are not zero), or the summary line as written. */
  readonly summary: string;
}

export interface PytestResults {
  readonly after?: PytestRun;
  readonly before?: PytestRun;
}

const SECTION = /^=== pytest (before|after) the fix/u;
const EXIT = /^exit code (-?\d+):\s*(.*)$/u;

const countsOf = (text: string): string => {
  const parts: Array<string> = [];

  for (const [, count, word] of text.matchAll(/(\d+) (passed|failed|errors?|skipped)/gu)) {
    if (count !== undefined && word !== undefined && Number(count) > 0) {
      parts.push(`${count} ${word}`);
    }
  }
  // "1 passed, 1 failed": failures first, as pytest prints them
  parts.sort((left, right) => Number(left.includes("passed")) - Number(right.includes("passed")));

  return parts.length > 0 ? parts.join(", ") : (text.split(" in ")[0]?.trim() ?? text);
};

/**
 * The before/after runs of a repair's pytest output (`combine_pytest_output`): each section opens
 * with `=== pytest before the fix ...` or `=== pytest after the fix ...`, then `$ <command>` and
 * `exit code <n>: <counts>`.
 */
export const pytestResults = (output: string | null | undefined): PytestResults => {
  const result: { after?: PytestRun; before?: PytestRun } = {};

  if (output === null || output === undefined) {
    return result;
  }
  let current: "after" | "before" | undefined;
  let command: string | undefined;

  for (const raw of output.split(/\r?\n/u)) {
    const line = raw.trim();
    const section = SECTION.exec(line);

    if (section !== null) {
      current = section[1] as "after" | "before";
      command = undefined;
      continue;
    }
    if (current === undefined) {
      continue;
    }
    if (line.startsWith("$ ") && command === undefined) {
      command = line.slice(2);
      continue;
    }
    const exit = EXIT.exec(line);

    if (exit !== null && result[current] === undefined) {
      result[current] = { command, passed: exit[1] === "0", summary: countsOf(exit[2] ?? "") };
    }
  }

  return result;
};

/** The body of the `## CI` section of a PR.md (until the next `## ` heading), or undefined. */
export const ciSection = (markdown: string | null | undefined): string | undefined => {
  if (markdown === null || markdown === undefined) {
    return undefined;
  }
  const lines = markdown.split(/\r?\n/u);
  const start = lines.findIndex((line) => /^##\s+CI\s*$/u.test(line));

  if (start === -1) {
    return undefined;
  }
  const rest = lines.slice(start + 1);
  const end = rest.findIndex((line) => /^#{1,2}\s/u.test(line));
  const body = (end === -1 ? rest : rest.slice(0, end)).join("\n").trim();

  return body.length > 0 ? body.replaceAll("\\[", "[").replaceAll("\\]", "]") : undefined;
};

export type MarkdownBlock = { readonly code: string } | { readonly text: string };

/**
 * The blocks of a Markdown section: fenced code blocks as they are, and paragraphs (blank-line
 * separated, inner line breaks joined). Enough for the sections of PR.md the console shows.
 */
export const markdownBlocks = (markdown: string): Array<MarkdownBlock> => {
  const blocks: Array<MarkdownBlock> = [];
  let paragraph: Array<string> = [];
  let code: Array<string> | undefined;
  const flush = () => {
    const text = paragraph.join(" ").replace(/\s+/gu, " ").trim();

    if (text.length > 0) {
      blocks.push({ text });
    }
    paragraph = [];
  };

  for (const line of markdown.split(/\r?\n/u)) {
    if (/^\s*```/u.test(line)) {
      if (code === undefined) {
        flush();
        code = [];
      } else {
        blocks.push({ code: code.join("\n") });
        code = undefined;
      }
    } else if (code !== undefined) {
      code.push(line);
    } else if (line.trim().length === 0) {
      flush();
    } else {
      paragraph.push(line.trim());
    }
  }
  if (code !== undefined) {
    blocks.push({ code: code.join("\n") });
  }
  flush();

  return blocks;
};

/** Lines [first, last] (0-based, inclusive) of a YAML block, or undefined. */
export interface LineRange {
  readonly first: number;
  readonly last: number;
}

const indentOf = (line: string): number => line.length - line.trimStart().length;

/** The block that starts at `start`: every following line indented deeper, or blank. */
const blockFrom = (lines: ReadonlyArray<string>, start: number): LineRange => {
  const base = indentOf(lines[start] ?? "");
  // A list item `- name: x` owns the lines indented past its dash.
  let last = start;

  for (let index = start + 1; index < lines.length; index += 1) {
    const line = lines[index] ?? "";

    if (line.trim().length === 0) {
      continue;
    }
    if (indentOf(line) <= base) {
      break;
    }
    last = index;
  }

  return { first: start, last };
};

const nameLine = (name: string): RegExp =>
  new RegExp(`^\\s*-?\\s*name:\\s*["']?${name.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}["']?\\s*(#.*)?$`, "u");

/**
 * The lines of a contract file that hold the rule of `column` in `dataset` (the `- name: <column>`
 * item and its keys), or the dataset's `row_count` / whole item without a column. Line-based, for
 * the files dag-healer writes; undefined when not found.
 */
export const yamlRuleRange = (
  text: string,
  dataset: string | undefined,
  column: string | null | undefined,
  key?: string,
): LineRange | undefined => {
  const lines = text.split(/\r?\n/u);
  let from = 0;
  let to = lines.length - 1;

  if (dataset !== undefined) {
    const start = lines.findIndex((line) => nameLine(dataset).test(line) && /^\s*-\s/u.test(line));

    if (start !== -1) {
      const block = blockFrom(lines, start);

      from = block.first;
      to = block.last;
      if (column === null || column === undefined) {
        if (key !== undefined) {
          const at = lines.findIndex(
            (line, index) => index >= from && index <= to && line.trim().startsWith(`${key}:`),
          );

          return at === -1 ? block : blockFrom(lines, at);
        }

        return block;
      }
    }
  }
  if (column === null || column === undefined) {
    return undefined;
  }
  const at = lines.findIndex((line, index) => index >= from && index <= to && nameLine(column).test(line));

  return at === -1 ? undefined : blockFrom(lines, at);
};
