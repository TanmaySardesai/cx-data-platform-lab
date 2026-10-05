# Lab 08 — CI/CD with GitHub Actions + AI coding tools (Fri Part C, ~45 min)

The workflow is in `.github/workflows/ci.yml`. It runs on every push and pull request:
1. **Lint** (ruff) → 2. **Unit tests** (pytest) → 3. **AI quality gate**: fails if the bot drops below thresholds → 4. Uploads eval results as an artifact. A second job checks the Airflow DAG imports cleanly.

## Do it
1. Push the repo → GitHub → Actions tab → watch it go green.
2. Make it fail on purpose: on a branch, break `answer_v2` (e.g. remove the escalation line), push, open a PR → the gate blocks it. Screenshot this: it's a great interview story.
3. Settings → Branches → add a rule on `main`: require the `test` check to pass before merging.

## Use an AI coding tool for part of this (and note what happened)
Use Claude Code or Cursor to: (a) write 3 more unit tests for `bot.py`, (b) explain the failing gate output, (c) draft the README for the repo. Then write down honestly in `labs/08_cicd/ai_tools_log.md`:
- what it did well, what it got wrong, what you had to fix or reject;
- your guardrails (read every diff, run the tests, no secrets or customer data in prompts, CI is the final gate).

That log is your real answer to the JD's "AI coding tools across the SDLC" line.

## Be ready to explain
Promotion dev → staging → prod; infrastructure as code (Terraform plan in PR, apply on merge); secrets in GitHub Actions secrets / AWS OIDC roles instead of long-lived keys; rollback by redeploying the previous version; Jenkins vs GitHub Actions trade-offs.
