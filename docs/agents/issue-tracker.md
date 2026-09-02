# Issue tracker: GitHub

Issues and specs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies. Re-read and apply the user's writing guidelines fully, if available.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v`, which `gh` does automatically when run inside a clone. This clone points at `assetcorp/sirannon-db`.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo treats external PRs as feature requests; `/triage` reads this flag.)_

When set to `yes`, PRs run through the same labels and states as issues, using the `gh pr` equivalents:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>` for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments` then keep only `authorAssociation` of `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, or `NONE` (drop `OWNER`/`MEMBER`/`COLLABORATOR`).
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label`/`--remove-label`, `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be either. Resolve it with `gh pr view 42`, and fall back to `gh issue view 42`.

## Project board

Every issue also appears on the user project 'Feature release' at `https://github.com/users/assetcorp/projects/2`, and each item there carries four fields. Set all four when you create the issue, set the type label on the issue itself, and move Status as the work moves. Read the project id, the field ids, and the option ids with `gh project field-list 2 --owner assetcorp --format json`, and read an item's id with `gh project item-list 2 --owner assetcorp --format json`. Add an issue to the board with `gh project item-add 2 --owner assetcorp --url <issue-url>`.

**Status** is a project field with five options. Backlog holds an issue that is created and not yet specified, or specified with an open blocker. Ready holds an issue that is specified and whose every blocker is closed. In progress holds an issue with an assignee and a branch. In review holds an issue with an open pull request. Done holds the closed issue. Set it with `gh project item-edit --project-id <project-id> --id <item-id> --field-id <status-field-id> --single-select-option-id <option-id>`.

**Type** is one of three labels on the issue, because a user account carries no organisation issue types. `bug` is behaviour that contradicts a promise the code or a contract makes. `enhancement` is new behaviour a caller can see. `task` is everything else, such as a benchmark change, a refactor, or a document.

**Priority** is a project field with three options. P0 loses data, breaks the on-disk format or the wire protocol, or blocks the next release. P1 must close before its train closes. P2 waits for the next free window.

**Size** is a project field that measures the change's reach. XS is one file. S is one directory in one package. M is several directories in one package with their tests. L crosses packages. XL changes the on-disk format, the wire protocol, or a contract document.

**Estimate** is a project number field from 0 to 5 that measures how many things must be true at once, independent of size. 0 is a document only. 1 closes in one fresh window. 2 needs tests against the built engine or a live container. 3 needs several windows. 4 changes the engine and a benchmark suite in one train. 5 needs a design ruling before it starts.

The project also carries an Iteration field and two date fields, which the owner sets by hand, so leave them unset.

**Labels stay separate.** The triage labels say who does the work, the type label says what kind of work it is, and Status says where the work is.

## When a skill says "publish to the issue tracker"

Create a GitHub issue, set its type label, add it to the project board, and set its Status, Priority, Size, and Estimate there.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Wayfinding operations

Used by `/wayfinder`. The **map** is a single issue with **child** issues as tickets.

- **Map**: a single issue labelled `wayfinder:map`, holding the Notes / Decisions-so-far / Fog body. `gh issue create --label wayfinder:map`.
- **Child ticket**: an issue linked to the map as a GitHub sub-issue (`gh api` on the sub-issues endpoint). Where sub-issues aren't enabled, add the child to a task list in the map body and put `Part of #<map>` at the top of the child body. Labels: `wayfinder:<type>` (`research`/`prototype`/`grilling`/`task`). Once claimed, the ticket is assigned to the driving dev.
- **Blocking**: GitHub's **native issue dependencies**, which are the canonical, UI-visible representation. Add an edge with `gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`, where `<blocker-db-id>` is the blocker's numeric **database id** (`gh api repos/<owner>/<repo>/issues/<n> --jq .id`, _not_ the `#number` or `node_id`). GitHub reports `issue_dependencies_summary.blocked_by`, counting open blockers only, which is the live gate. Where dependencies aren't available, fall back to a `Blocked by: #<n>, #<n>` line at the top of the child body. A ticket is unblocked when every blocker is closed.
- **Frontier query**: list the map's open children (`gh issue list --state open`, scoped to the map's sub-issues / task list), drop any with an open blocker (`issue_dependencies_summary.blocked_by > 0`, or an open issue in the `Blocked by` line) or an assignee; first in map order wins.
- **Claim**: `gh issue edit <n> --add-assignee @me`, the session's first write.
- **Resolve**: `gh issue comment <n> --body "<answer>"`, then `gh issue close <n>`, then append a context pointer (gist + link) to the map's Decisions-so-far.
