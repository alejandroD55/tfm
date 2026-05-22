#!/usr/bin/env bash
# Lanza el pipeline desde parallel_analysis (sentiment + indicators → bayesian → report).
# Requiere que ingestion, news_filter y macro_context ya se hayan ejecutado para BATCH_DATE.
#
# Uso:
#   ./scripts/start_pipeline_from_parallel.sh 2026-05-21
#   ./scripts/start_pipeline_from_parallel.sh 2026-05-21 eu-north-1
set -euo pipefail

BATCH_DATE="${1:?Falta batch_date (YYYY-MM-DD)}"
REGION="${2:-eu-north-1}"
SM_NAME="tfm-pipeline-resume"
ROLE_ARN="${SFN_ROLE_ARN:-}"

DEF_FILE="$(cd "$(dirname "$0")/.." && pwd)/stepfunctions_definition_resume.json"
INPUT=$(mktemp)
trap 'rm -f "$INPUT"' EXIT

RUN_ID="resume-${BATCH_DATE}-$(date +%s)"
cat >"$INPUT" <<EOF
{
  "batch_date": "${BATCH_DATE}",
  "trigger_type": "manual",
  "pipeline_context": {
    "batch_date": "${BATCH_DATE}",
    "run_id": "${RUN_ID}",
    "execution_name": "resume-parallel",
    "request": {
      "batch_date": "${BATCH_DATE}",
      "trigger_type": "manual"
    }
  }
}
EOF

# Crear o actualizar state machine de reanudación
if [ -z "$ROLE_ARN" ]; then
  echo "Define SFN_ROLE_ARN (rol que Step Functions usa para invocar Lambdas), ej.:"
  echo "  export SFN_ROLE_ARN=arn:aws:iam::586723123656:role/tfm-stepfunctions-role"
  exit 1
fi

EXISTING=$(aws stepfunctions list-state-machines --region "$REGION" \
  --query "stateMachines[?name=='${SM_NAME}'].stateMachineArn" --output text)

if [ -n "$EXISTING" ] && [ "$EXISTING" != "None" ]; then
  echo "Actualizando ${SM_NAME}..."
  aws stepfunctions update-state-machine \
    --state-machine-arn "$EXISTING" \
    --definition "file://${DEF_FILE}" \
    --region "$REGION" >/dev/null
  SM_ARN="$EXISTING"
else
  echo "Creando ${SM_NAME}..."
  SM_ARN=$(aws stepfunctions create-state-machine \
    --name "$SM_NAME" \
    --definition "file://${DEF_FILE}" \
    --role-arn "$ROLE_ARN" \
    --region "$REGION" \
    --query 'stateMachineArn' --output text)
fi

echo "Iniciando ejecución (desde parallel_analysis) batch_date=${BATCH_DATE}..."
EXEC_ARN=$(aws stepfunctions start-execution \
  --state-machine-arn "$SM_ARN" \
  --name "resume-${BATCH_DATE}-$(date +%H%M%S)" \
  --input "file://${INPUT}" \
  --region "$REGION" \
  --query 'executionArn' --output text)

echo "ExecutionArn: ${EXEC_ARN}"
echo "Consola: https://${REGION}.console.aws.amazon.com/states/home?region=${REGION}#/executions/details/$(echo "$EXEC_ARN" | sed 's/.*execution://')"
