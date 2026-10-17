import { describe, expect, it } from "vitest";

import { contextFromObjects, parseAirflowPath, resolveContext } from "./pluginContext";

describe("parseAirflowPath", () => {
  it("parses a DAG path", () => {
    expect(parseAirflowPath("/dags/sales_daily_etl")).toEqual({ dagId: "sales_daily_etl" });
  });

  it("parses a plugin tab under a task instance", () => {
    expect(
      parseAirflowPath(
        "/dags/customers_sync/runs/manual__2026-10-17T12%3A00%3A00%2B00%3A00/tasks/load/plugin/healer-ti",
      ),
    ).toEqual({ dagId: "customers_sync", runId: "manual__2026-10-17T12:00:00+00:00", taskId: "load" });
  });

  it("parses a mapped task instance", () => {
    expect(parseAirflowPath("/dags/d/runs/r1/tasks/t/mapped/3/plugin/healer-ti")).toEqual({
      dagId: "d",
      mapIndex: "3",
      runId: "r1",
      taskId: "t",
    });
  });

  it("parses a task without a run", () => {
    expect(parseAirflowPath("/dags/d/tasks/t")).toEqual({ dagId: "d", taskId: "t" });
  });

  it("ignores task group routes and non-numeric map indexes", () => {
    expect(parseAirflowPath("/dags/d/runs/r/tasks/group/g1")).toEqual({ dagId: "d", runId: "r" });
    expect(parseAirflowPath("/dags/d/runs/r/tasks/t/mapped/abc")).toEqual({
      dagId: "d",
      runId: "r",
      taskId: "t",
    });
  });

  it("finds the dags segment after a base path prefix", () => {
    expect(parseAirflowPath("/airflow/team-a/dags/d/runs/r", "http://host/airflow/team-a/")).toEqual({
      dagId: "d",
      runId: "r",
    });
    // Without a base URI the segment is still found anywhere in the path.
    expect(parseAirflowPath("/airflow/dags/d")).toEqual({ dagId: "d" });
  });

  it("does not confuse a base path named dags with the route", () => {
    expect(parseAirflowPath("/dags/dags/my_dag/runs/r", "http://host/dags/")).toEqual({
      dagId: "my_dag",
      runId: "r",
    });
  });

  it("returns an empty context outside DAG routes", () => {
    expect(parseAirflowPath("/plugin/healer")).toEqual({});
    expect(parseAirflowPath("/dags")).toEqual({});
    expect(parseAirflowPath("")).toEqual({});
  });

  it("keeps malformed escapes as-is", () => {
    expect(parseAirflowPath("/dags/bad%E0%A4%A")).toEqual({ dagId: "bad%E0%A4%A" });
  });
});

describe("contextFromObjects", () => {
  it("reads REST-shaped task instance objects", () => {
    expect(
      contextFromObjects({
        taskInstance: { dag_id: "d", dag_run_id: "r", map_index: -1, task_id: "t" },
      }),
    ).toEqual({ dagId: "d", mapIndex: "-1", runId: "r", taskId: "t" });
  });

  it("falls back to dagRun and dag objects", () => {
    expect(contextFromObjects({ dag: { dag_id: "d" }, dagRun: { dag_run_id: "r" } })).toEqual({
      dagId: "d",
      runId: "r",
    });
    expect(contextFromObjects({ dag: "not-an-object", taskInstance: null })).toEqual({});
  });
});

describe("resolveContext", () => {
  const location = { baseUri: "http://host/", pathname: "/dags/url_dag/runs/url_run/tasks/url_task" };

  it("prefers explicit props over the URL", () => {
    expect(resolveContext({ dagId: "url_dag", taskId: "prop_task" }, location)).toEqual({
      dagId: "url_dag",
      runId: "url_run",
      taskId: "prop_task",
    });
  });

  it("falls back to the URL when props are missing", () => {
    expect(resolveContext({}, location)).toEqual({ dagId: "url_dag", runId: "url_run", taskId: "url_task" });
  });

  it("never mixes values from a different DAG", () => {
    expect(resolveContext({ dagId: "other" }, location)).toEqual({ dagId: "other" });
  });

  it("normalizes numeric map indexes and ignores empty strings", () => {
    expect(resolveContext({ dagId: "d", mapIndex: 2, runId: "" }, { pathname: "/" })).toEqual({
      dagId: "d",
      mapIndex: "2",
    });
  });

  it("uses window.location by default", () => {
    window.history.pushState({}, "", "/dags/from_window/runs/r9");
    expect(resolveContext({})).toMatchObject({ dagId: "from_window", runId: "r9" });
    window.history.pushState({}, "", "/");
  });
});
