# Working in webhook-relay

See the workspace AGENTS.md (../AGENTS.md) for shared guidance.

## Sharing a checkout with other sessions

Several Claude or Codex sessions (local, desktop, Remote Control, subagents) can work in this same folder at the same time. Their uncommitted work is not yours.

- **Before staging**, run `git status` and `git diff --stat`. Anything you did not change in this session belongs to someone else: do not stage, commit, reformat, stash, reset or delete it.
- **Stage by explicit path** (`git add path/to/file ...`). In a shared checkout never use `git add -A`, `git add .`, `git commit -a`, `git stash`, `git reset --hard`, `git checkout -- .` or `git clean`. Look at untracked files for credentials (kubeconfig, `.env`, keys) before any commit.
- **Pull before committing, fetch tags before releasing.** `git pull --rebase`, then `git fetch --tags`. Read `git log <last tag>..HEAD` so a release holds only what you mean to ship, and tag the exact commit (`git tag -a vX.Y.Z <sha>`), never "whatever HEAD is".
- **Before a deploy, read what the environment pins now** (homelab `kubernetes/apps/km-service/values/<env>/`): another session may have released since you last looked. Pull the deploy repository, change only your own line(s), and leave other applications' tags alone.
- **Parallel runs need their own names**: a scratch database, port and temp folder per session or subagent.
- **Say what you did at the end of your turn**: what you committed, and what you deliberately left alone. If the tree holds work you did not write, say so instead of taking it over.
