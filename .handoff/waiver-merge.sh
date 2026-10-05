#!/bin/bash
# waiver-merge.sh <pr> <full-head-sha>
# Temporarily removes the required "go" check, merges the PR at exactly that head,
# then restores the full protection and verifies it matches the snapshot. Always restores.
set -u
PR=$1; SHA=$2
S=${WAIVER_STATE_DIR:-$(mktemp -d)}
R=repos/well-prado/new-blok/branches/main/protection
FULL='{"required_status_checks":{"strict":true,"checks":[{"context":"go","app_id":15368}]},"enforce_admins":true,"required_pull_request_reviews":{"dismiss_stale_reviews":false,"require_code_owner_reviews":false,"require_last_push_approval":false,"required_approving_review_count":0},"restrictions":null,"required_linear_history":false,"allow_force_pushes":false,"allow_deletions":false,"block_creations":false,"required_conversation_resolution":false,"lock_branch":false,"allow_fork_syncing":false}'
WAIVED=$(echo "$FULL" | jq -c '.required_status_checks = null')
restore() {
  echo "$FULL" | gh api -X PUT $R --input - >/dev/null
  gh api $R > $S/protection-after.json
  if diff <(jq -S 'del(.. | .url?, .contexts_url?)' $S/protection-before.json) <(jq -S 'del(.. | .url?, .contexts_url?)' $S/protection-after.json); then echo "PROTECTION IDENTICAL"; else echo "PROTECTION MISMATCH"; exit 9; fi
}
[ "$(gh api repos/well-prado/new-blok/actions/workflows/372722919 --jq .state)" = disabled_manually ] || { echo "CI not disabled; abort"; exit 3; }
gh api $R > $S/protection-before.json || { echo "snapshot failed; abort"; exit 4; }
[ "$(jq -c '.required_status_checks.checks' $S/protection-before.json)" = '[{"context":"go","app_id":15368}]' ] || { echo "protection not in expected full state before merge; abort"; exit 5; }
trap restore EXIT
[ "$(gh api repos/well-prado/new-blok/actions/workflows/372722919 --jq .state)" = disabled_manually ] || { echo "CI not disabled; abort"; exit 3; }
echo "$WAIVED" | gh api -X PUT $R --input - >/dev/null && echo waived
gh pr merge "$PR" --repo well-prado/new-blok --merge --match-head-commit "$SHA"
echo "merge-exit=$?"
