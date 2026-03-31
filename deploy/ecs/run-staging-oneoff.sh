#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <image-uri> <db-password-ssm-arn> <gcp-key-b64-ssm-arn>"
  exit 1
fi

IMAGE_URI="$1"
DB_PASSWORD_SSM_ARN="$2"
GCP_KEY_B64_SSM_ARN="$3"

TMP_TASKDEF="$(mktemp)"
trap 'rm -f "$TMP_TASKDEF"' EXIT

sed \
  -e "s#__IMAGE_URI__#${IMAGE_URI}#g" \
  -e "s#__DB_PASSWORD_SSM_ARN__#${DB_PASSWORD_SSM_ARN}#g" \
  -e "s#__GCP_KEY_B64_SSM_ARN__#${GCP_KEY_B64_SSM_ARN}#g" \
  /Users/mazdak/Code/cdsync/deploy/ecs/cdsync-staging-oneoff-taskdef.template.json > "$TMP_TASKDEF"

TASK_DEF_ARN="$(
  aws ecs register-task-definition \
    --region us-east-1 \
    --cli-input-json "file://${TMP_TASKDEF}" \
    --query 'taskDefinition.taskDefinitionArn' \
    --output text
)"

echo "registered task definition: ${TASK_DEF_ARN}"

aws ecs run-task \
  --region us-east-1 \
  --cluster cluster-staging \
  --launch-type FARGATE \
  --task-definition "${TASK_DEF_ARN}" \
  --network-configuration 'awsvpcConfiguration={subnets=[subnet-0847f8d9415013ba2,subnet-0a790db993d728821],securityGroups=[sg-06d149ded90962461],assignPublicIp=DISABLED}' \
  --query 'tasks[0].taskArn' \
  --output text
