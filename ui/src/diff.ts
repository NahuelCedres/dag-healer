/** Parsing of unified diffs (`git diff` output) for {@link DiffView}. */

export type DiffLineKind = "add" | "context" | "del" | "file" | "hunk" | "meta";

export interface DiffLine {
  kind: DiffLineKind;
  /** New-file line number for "add"/"context" lines. */
  newNumber?: number;
  /** Old-file line number for "del"/"context" lines. */
  oldNumber?: number;
  text: string;
}

export interface DiffStats {
  additions: number;
  deletions: number;
  files: Array<string>;
}

const HUNK_RE = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/u;

/**
 * Classify every line of a unified diff. Line numbers restart at each hunk header; the `---`/`+++`
 * file headers are "meta" lines only when they appear before the first hunk of a file.
 */
export const parseDiff = (text: string | null | undefined): Array<DiffLine> => {
  if (text === null || text === undefined || text.length === 0) {
    return [];
  }
  const lines = text.replace(/\r\n/gu, "\n").replace(/\n$/u, "").split("\n");
  const result: Array<DiffLine> = [];
  let inHunk = false;
  let oldLine = 0;
  let newLine = 0;

  for (const line of lines) {
    if (line.startsWith("diff --git ")) {
      inHunk = false;
      result.push({ kind: "file", text: line });
      continue;
    }
    const hunk = HUNK_RE.exec(line);

    if (hunk !== null) {
      inHunk = true;
      oldLine = Number(hunk[1]);
      newLine = Number(hunk[2]);
      result.push({ kind: "hunk", text: line });
      continue;
    }
    if (!inHunk || line.startsWith("\\ ")) {
      // File headers (index, ---, +++, modes, renames) and "\ No newline at end of file".
      result.push({ kind: "meta", text: line });
      continue;
    }
    if (line.startsWith("+")) {
      result.push({ kind: "add", newNumber: newLine, text: line });
      newLine += 1;
    } else if (line.startsWith("-")) {
      result.push({ kind: "del", oldNumber: oldLine, text: line });
      oldLine += 1;
    } else {
      result.push({ kind: "context", newNumber: newLine, oldNumber: oldLine, text: line });
      oldLine += 1;
      newLine += 1;
    }
  }

  return result;
};

const fileFromHeader = (line: string): string | undefined => {
  // diff --git a/src/x.py b/src/x.py
  const match = /^diff --git a\/(.+?) b\/(.+)$/u.exec(line);

  return match?.[2];
};

/** Files touched and the number of added / removed lines. */
export const diffStats = (text: string | null | undefined): DiffStats => {
  const stats: DiffStats = { additions: 0, deletions: 0, files: [] };
  for (const line of parseDiff(text)) {
    if (line.kind === "file") {
      const file = fileFromHeader(line.text);

      if (file !== undefined && !stats.files.includes(file)) {
        stats.files.push(file);
      }
    } else if (line.kind === "meta" && line.text.startsWith("+++ ")) {
      const file = line.text.slice(4).replace(/^b\//u, "").trim();

      if (file !== "/dev/null" && !stats.files.includes(file)) {
        stats.files.push(file);
      }
    } else if (line.kind === "add") {
      stats.additions += 1;
    } else if (line.kind === "del") {
      stats.deletions += 1;
    }
  }

  return stats;
};
