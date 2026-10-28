"""The dag-healer agent: incident context, deterministic policy, LLM classification and repairs.

Submodules (import them directly; this package imports nothing heavy on purpose):

* ``dag_healer.agent.context`` - gathers the evidence of one incident (logs, code, contract,
  masked sample).
* ``dag_healer.agent.policy`` - the deterministic gates that decide DATA / CODE / AMBIGUOUS.
* ``dag_healer.agent.classify`` - combines the policy with the LLM classification.
* ``dag_healer.agent.llm`` / ``dag_healer.agent.bedrock`` / ``dag_healer.agent.fake`` - LLM
  providers (the Claude API, Claude on Amazon Bedrock, offline fake).
* ``dag_healer.agent.budget`` - token budget per incident.
* ``dag_healer.agent.repair`` / ``dag_healer.agent.tests_gen`` - the CODE path (test, fix, PR).
* ``dag_healer.agent.prompts`` - prompt templates (Markdown files shipped as package data).
"""

from __future__ import annotations
