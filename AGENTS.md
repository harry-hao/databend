# Repository Guidelines

This file is the entry point for repository-specific working rules. Keep it focused on overall direction; read the matching detail documents in `agents/` before making changes in that area.

## Core Workflow
- Build context from the codebase first. Databend is a multi-crate Rust workspace, so understand the affected module boundaries before editing.
- Validate incrementally. Run the smallest relevant checks early, and apply the verification standard that matches the task type.
- Treat tests as part of the change. Planner, executor, storage, and behavior changes should come with regression coverage.
- Keep contributions reviewable. Commits should stay scoped, and pull requests should follow the repository's expected collaboration workflow.

## Task Types
- Implementation tasks: the result is intended to stay in the branch, be reviewed, or be submitted. Follow the normal quality bar for edits, validation, tests, commits, and PR readiness.
- Exploration tasks: the result is mainly temporary, used for investigation, debugging, measurement, or option evaluation, and is not intended to be submitted as-is. Prioritize speed and useful conclusions over polish.
- If the output you plan to keep is only notes, logs, ad hoc scripts, or temporary experiments, treat it as exploration work.
- If the output includes code, tests, or docs that are expected to remain in the branch for review or submission, treat it as implementation work.
- For exploration tasks that do not produce submit-worthy changes, avoid spending time on low-value checks, exhaustive formatting passes, or broad test runs unless they are needed to answer the question correctly.
- If a task starts as exploration and turns into a real code change that should be kept, switch back to the implementation-task standard before handoff.

## Detail Index
- [`agents/repository-structure.md`](agents/repository-structure.md) for workspace layout and where code, tests, tooling, and fixtures live.
- [`agents/development-commands.md`](agents/development-commands.md) for setup, build, run, test, format, and lint commands.
- [`agents/coding-style.md`](agents/coding-style.md) for Rust, Python, shell, naming, error handling, and observability conventions.
- [`agents/debug-and-validation.md`](agents/debug-and-validation.md) for clippy expectations and testing strategy.
- [`agents/commit-and-pr.md`](agents/commit-and-pr.md) for commit format, PR requirements, and the Databend PR template example.
- If you discover high-value information that would materially help development or testing but cannot be found through this Detail Index and the linked documents beneath it, call that gap out explicitly instead of assuming the current documentation path is sufficient.

## Default Rule
If guidance in a detail file is relevant to the task, follow it. Determine the task type first, then apply the matching validation and testing rules. Exploration-task guidance is an explicit exception to the default implementation-task quality bar.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:7510c1e2 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
