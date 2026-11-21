#!/usr/bin/env bash
# Open an SSM shell on the requested host so the student can register its runner.
# The one-time GitHub registration token is entered only in the remote interactive shell;
# it is never passed through SSM Run Command, printed, or written to this repo.
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

role=${1:-app}
if [[ ! "$role" =~ ^(app|platform|llm)$ ]]; then
  echo "usage: scripts/register-runner.sh [app|platform|llm]" >&2
  exit 2
fi

for command in aws gh terraform; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 2; }
done

root="$(cd "$(dirname "$0")/.." && pwd)"
repo=$(cd "$root" && gh repo view --json nameWithOwner --jq .nameWithOwner)
instance_id=$(terraform -chdir="$root/infra/live" output -raw "${role}_instance_id")
label="lab-${role}"

cat <<EOF
Connecting to the AWS ${role} host for GitHub runner setup.

In GitHub, open https://github.com/${repo}/settings/actions/runners/new, choose Linux / ARM64,
and copy the short-lived registration token (do not share or save it). Then, in this SSM shell:

  sudo -u runner -H bash
  cd /opt/actions-runner
  curl -fsSLO https://github.com/actions/runner/releases/download/v2.338.0/actions-runner-linux-arm64-2.338.0.tar.gz
  tar xzf actions-runner-linux-arm64-2.338.0.tar.gz
  read -rsp 'One-time registration token: ' RUNNER_TOKEN; echo
  ./config.sh --url https://github.com/${repo} --token "\$RUNNER_TOKEN" --name ${role} --labels ${label} --work _work --unattended
  unset RUNNER_TOKEN
  exit
  sudo /opt/actions-runner/svc.sh install runner
  sudo /opt/actions-runner/svc.sh start

The token is read without echo and is not stored in this repo or passed through SSM Run Command.
EOF

exec aws ssm start-session --target "$instance_id"
