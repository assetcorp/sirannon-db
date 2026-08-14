# Triage Labels

The skills speak in terms of five canonical triage roles. This repo uses the canonical name as the label string, so each role maps to itself.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

All five labels exist on `assetcorp/sirannon-db`. GitHub's default `question` label was renamed to `needs-info`, because the two carry the same meaning and a second label would have split the same state across two names.

When a skill mentions a role (e.g. 'apply the AFK-ready triage label'), use the corresponding label string from this table.

Edit the right-hand column to match whatever vocabulary you actually use.
