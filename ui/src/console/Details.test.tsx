import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { event } from "src/test/fixtures";
import { DEFAULT_VOCABULARY } from "src/vocabulary";

import { eventText, Timeline } from "./Details";
import { utcOffset } from "./time";
import { AnnounceProvider, CopyButton } from "./ui";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("the timeline", () => {
  it("says alerts, the incident's creation and a queued action in plain words", () => {
    expect(
      eventText(
        event("alert", "waiting_human alert: log sent", {
          event: "waiting_human",
          outcomes: [
            { channel: "log", error: null, status: "sent" },
            { channel: "email", error: "SMTP refused", status: "failed" },
          ],
        }),
        DEFAULT_VOCABULARY,
      ),
    ).toBe("wrote the “Needs you” alert to the worker's log; could not email the “Needs you” alert");
    expect(
      eventText(
        event("created", "Incident created from rest for d.t (run r, try 1, map_index -1)"),
        DEFAULT_VOCABULARY,
      ),
    ).toBe("opened the incident (found by checking Airflow regularly)");
    expect(
      eventText(event("command_queued", "ann requested retry task", { action: "retry_task" }, "ann"), {
        ...DEFAULT_VOCABULARY,
        retry_action: "Redrive execution",
      }),
    ).toBe("asked to redrive execution");
    expect(eventText(event("triage", "Automatic triage started"), DEFAULT_VOCABULARY)).toBeUndefined();
  });

  it("leaves out a status line that repeats the event before it", () => {
    render(
      <Timeline
        events={[
          event("classification", "Classified as code (llm): the model decides."),
          event("status", "Classified as code (confidence 0.78, rule llm)"),
          event("repair", "Repair failed: no failing test after 3 attempts"),
          event("status", "Repair failed: no failing test after 3 attempts"),
          event("status", "Waiting for a human decision"),
        ]}
      />,
    );

    expect(within(screen.getByTestId("timeline")).getAllByRole("listitem")).toHaveLength(3);
  });
});

describe("times", () => {
  it("name the browser's offset from UTC", () => {
    const at = (minutes: number) => {
      const date = new Date("2026-10-17T12:00:00Z");

      vi.spyOn(date, "getTimezoneOffset").mockReturnValue(minutes);

      return date;
    };

    expect(utcOffset(at(180))).toBe("UTC-3");
    expect(utcOffset(at(-330))).toBe("UTC+5:30");
    expect(utcOffset(at(0))).toBe("UTC");
  });
});

describe("the copy button", () => {
  const copyIn = (copied: boolean) => {
    vi.stubGlobal("navigator", { ...globalThis.navigator, clipboard: undefined });
    const command = vi.fn(() => copied);

    Object.defineProperty(document, "execCommand", { configurable: true, value: command });
    render(
      <AnnounceProvider>
        <CopyButton label="PR.md" text={"# A long PR.md\n\nwith many lines"} />
      </AnnounceProvider>,
    );
    const button = screen.getByRole("button", { name: "Copy PR.md" });

    fireEvent.click(button);

    return { button, command };
  };

  it("copies without the Clipboard API and keeps the focus", async () => {
    const { button, command } = copyIn(true);

    expect(command).toHaveBeenCalledWith("copy");
    expect(document.activeElement).toBe(button);
    await waitFor(() => expect(screen.getByTestId("live-region")).toHaveTextContent("PR.md copied"));
  });

  it("says how to copy when the browser refuses, never the text itself", async () => {
    copyIn(false);

    await waitFor(() =>
      expect(screen.getByTestId("live-region")).toHaveTextContent(
        "Copying is blocked in this browser: select the PR.md and press Ctrl+C (⌘C on a Mac).",
      ),
    );
    expect(screen.getByTestId("live-region")).not.toHaveTextContent("A long PR.md");
    expect(document.querySelector("textarea")).toBeNull();
  });
});
