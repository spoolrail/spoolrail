---
name: maintain-package-overview
description: Use when creating, reviewing, rewriting, or otherwise editing .ai/docs/overview.md.
---

# Maintain Package Overview

Work exclusively on `.ai/docs/overview.md`. Inspect other repository files only as read-only evidence.

Treat the overview as mandatory upfront context, not a catalog. Keep only durable protected outcomes, non-obvious boundaries, cross-component compatibility costs, and rationale that normal investigation would not reliably reveal.

Before retaining or adding a rule, inspect the authoritative documentation, configuration guidance, public interfaces, tests, and relevant implementation. If a capable agent working in that area can and should discover the fact there, leave it there. A new feature does not need an overview rule by default.

Keep the smallest rule whose absence could cause a concrete mistake before discovery, and phrase it no more broadly than that mistake. Prefer tightening or merging an existing rule. Record hidden rationale without restating discoverable behavior, and keep the wording valid across plausible refactors.

Reread the overview as standalone input. Delete any rule that is discoverable, redundant, implementation-bound, or does not materially change a decision.
