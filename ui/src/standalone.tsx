/**
 * Entry point of the standalone bundle (`static/standalone/index.js`), loaded as an ES module by the
 * page shell of `dag-healer serve` (`dag_healer.hosting.shell`).
 *
 * Unlike the Airflow plugin bundle (`main.tsx`: UMD, React from the host), this bundle includes
 * React and ReactDOM, because the shell is a plain page with no host globals. The shell
 * serves the plugin's URL shapes, so the view is picked from the path and the same App renders it,
 * with the orchestrator's words from `GET /me`. The shell names the auth backend in
 * `<meta name="dag-healer-auth">`; with "token", the API token the user signs in with is sent as
 * `Authorization: Bearer` on every API call.
 */
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { StandaloneApp } from "./standalone/StandaloneApp";
import { authBackendFromDocument, installTokenAuthorization, TOKEN_BACKEND } from "./standalone/auth";
import { standaloneMode } from "./standalone/mode";

const authBackend = authBackendFromDocument();

// Before any request: the views fetch as soon as they mount.
if (authBackend === TOKEN_BACKEND) {
  installTokenAuthorization();
}

const root = document.querySelector<HTMLDivElement>("#root");

if (root !== null) {
  createRoot(root).render(
    <StrictMode>
      <StandaloneApp
        authBackend={authBackend}
        mode={standaloneMode(globalThis.location.pathname, document.baseURI)}
      />
    </StrictMode>,
  );
}
