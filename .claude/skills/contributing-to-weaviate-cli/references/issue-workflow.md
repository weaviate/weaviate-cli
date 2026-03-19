# Issue Workflow Reference

How agents should create, track, and close GitHub Issues when working on weaviate-cli.

## When to Create an Issue

Create a GitHub Issue **before starting implementation** when:

- The user asks to add a new feature or command
- The user asks to fix a bug
- The user asks to improve or refactor existing functionality
- The task involves non-trivial changes (3+ files, new patterns, architectural decisions)

**Do NOT create an issue for:**
- Trivial one-line fixes (typos, formatting)
- Documentation-only updates (skill files, CLAUDE.md)
- Agent/Claude-specific changes (`.claude/` configs, settings, skill improvements, workflow tweaks)
- Exploratory research or investigation
- Tasks where the user explicitly says not to create one

**Rule of thumb:** If the change only affects how agents interact with the repo (skills,
settings, prompts) and does not change the CLI tool itself, skip the issue.

## Creating an Issue

Always target the `weaviate/weaviate-cli` repository explicitly using `--repo`.

### Feature Issues

```bash
gh issue create \
  --repo weaviate/weaviate-cli \
  --title "Add \`weaviate-cli create widget\` command" \
  --label "enhancement" \
  --label "draft" \
  --body "$(cat <<'EOF'
## Context
<!-- Why is this needed? -->

Brief description of the motivation and use case.

## Scope
<!-- What specifically needs to change? -->

- Add `create widget` command with `--name`, `--size`, `--enabled` flags
- Add `WidgetManager` for business logic
- Add `--json` output support

## Files Involved

- `weaviate_cli/defaults.py` — add `CreateWidgetDefaults` dataclass
- `weaviate_cli/commands/create.py` — add Click command
- `weaviate_cli/managers/widget_manager.py` — new manager
- `test/unittests/test_managers/test_widget_manager.py` — unit tests

## Acceptance Criteria

- [ ] Command works with default arguments
- [ ] `--json` flag produces structured output
- [ ] Unit tests cover happy path and error cases
- [ ] `make lint` passes
- [ ] Operating skill documentation updated

## Constraints

- Follow existing defaults.py dataclass pattern
- Follow command → manager architecture
- Must support Python 3.9+

---
🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### Bug Issues

```bash
gh issue create \
  --repo weaviate/weaviate-cli \
  --title "Fix: collection get fails with special characters" \
  --label "bug" \
  --label "draft" \
  --body "$(cat <<'EOF'
## Description

Brief description of the bug.

## Steps to Reproduce

1. Run `weaviate-cli create collection --collection "Test-Collection"`
2. Run `weaviate-cli get collection --collection "Test-Collection"`
3. Observe error: `KeyError: 'Test-Collection'`

## Expected Behavior

The command should return the collection details.

## Actual Behavior

Crashes with `KeyError`.

## Root Cause

<!-- Fill in after investigation -->

## Files Involved

- `weaviate_cli/managers/collection_manager.py`

## Acceptance Criteria

- [ ] The command handles special characters correctly
- [ ] Unit test added for the edge case
- [ ] `make lint` passes

---
🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

## The `draft` Label

GitHub Issues don't have a native "draft" state (only PRs do). We use the `draft` label to indicate
that an issue is being actively worked on by an agent and is not yet ready for review.

**Label lifecycle:**
1. **`draft`**: Issue created, work in progress
2. Remove `draft` when the PR is ready for review
3. Issue gets closed automatically when the linked PR is merged

**First-time setup:** If the `draft` label doesn't exist in the repo, create it:
```bash
gh label create "draft" --repo weaviate/weaviate-cli --description "Work in progress, not ready for review" --color "E4E669"
```

## Linking Issues to PRs

When creating a PR for an issue, use the `Closes #N` syntax in the PR body:

```bash
gh pr create \
  --repo weaviate/weaviate-cli \
  --title "Add create widget command" \
  --body "$(cat <<'EOF'
## Summary
- Add `weaviate-cli create widget` command with --name, --size, --enabled flags
- Add WidgetManager with JSON output support
- Add unit tests

Closes #42

## Test plan
- [ ] `make test` passes
- [ ] `make lint` passes
- [ ] Manual test: `weaviate-cli create widget --json`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

This automatically closes the issue when the PR is merged.

## Updating Issue Progress

As work progresses, update the issue to reflect status. Check off acceptance criteria
using the GitHub API:

```bash
# Add a comment with progress update
gh issue comment 42 --repo weaviate/weaviate-cli --body "Implementation complete. PR #43 created."

# Remove the draft label when PR is ready
gh issue edit 42 --repo weaviate/weaviate-cli --remove-label "draft"

# Close manually if needed (normally auto-closed by PR merge)
gh issue close 42 --repo weaviate/weaviate-cli
```

## Complete Agent Workflow

Here's the full sequence an agent should follow:

```
1. User requests a feature or bug fix
2. INVESTIGATE: Read relevant code, understand the problem
3. CREATE ISSUE: `gh issue create --repo weaviate/weaviate-cli ...` with `draft` label
4. PLAN: Enter plan mode, design the implementation
5. IMPLEMENT: Write code following the contributing skill patterns
6. TEST: Run `make test` and `make lint`
7. CREATE PR: Link to the issue with `Closes #N`
8. UPDATE ISSUE: Remove `draft` label, add comment with PR link
```

### Example: Full Feature Implementation

```bash
# Step 3: Create the tracking issue
gh issue create \
  --repo weaviate/weaviate-cli \
  --title "Add \`weaviate-cli get replication\` command" \
  --label "enhancement" \
  --label "draft" \
  --body "$(cat <<'EOF'
## Context
Users need to check replication status for collections.

## Scope
- Add `get replication` command
- Show replication factor, async status, and per-shard details

## Files Involved
- `weaviate_cli/defaults.py`
- `weaviate_cli/commands/get.py`
- `weaviate_cli/managers/cluster_manager.py`
- `test/unittests/test_managers/test_cluster_manager.py`

## Acceptance Criteria
- [ ] Command shows replication config for a collection
- [ ] `--json` output supported
- [ ] Unit tests added
- [ ] `make lint` passes

---
🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"

# ... implement, test, etc ...

# Step 7: Create PR linked to issue
gh pr create \
  --repo weaviate/weaviate-cli \
  --title "Add get replication command" \
  --body "$(cat <<'EOF'
## Summary
- Add `weaviate-cli get replication` command
- Shows replication factor and per-shard async replication status

Closes #42

## Test plan
- [ ] `make test` passes
- [ ] `make lint` passes

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"

# Step 8: Update issue
gh issue edit 42 --repo weaviate/weaviate-cli --remove-label "draft"
gh issue comment 42 --repo weaviate/weaviate-cli --body "PR #43 ready for review."
```

## Issue Naming Conventions

**Features:**
- `Add \`weaviate-cli <group> <command>\` command` — for new commands
- `Add --<flag> to \`weaviate-cli <group> <command>\`` — for new options
- `Support <capability> in weaviate-cli` — for broader features

**Bugs:**
- `Fix: <brief description of the bug>` — for bug fixes

**Improvements:**
- `Improve <area>: <brief description>` — for refactoring or enhancements
