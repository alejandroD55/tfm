#!/usr/bin/env bash
# Reactiva Lambdas en estado Inactive (ImageAccessDenied) re-aplicando la imagen ECR actual.
# Uso: ./scripts/reactivate_lambda_images.sh [REGION]
set -euo pipefail

REGION="${1:-eu-north-1}"
FUNCS=(
  lambda_ingestion
  lambda_macro_ingestion
  lambda_macro_context
  lambda_news_filter
  lambda_sentiment
  lambda_indicators
  lambda_bayesian
  lambda_report
)

echo "Región: ${REGION}"
for fn in "${FUNCS[@]}"; do
  STATE=$(aws lambda get-function-configuration --function-name "$fn" --region "$REGION" \
    --query 'State' --output text 2>/dev/null || echo "MISSING")
  if [ "$STATE" = "MISSING" ]; then
    echo "[SKIP] $fn — no existe"
    continue
  fi
  if [ "$STATE" = "Active" ]; then
    echo "[OK]   $fn — ya Active"
    continue
  fi
  IMG=$(aws lambda get-function --function-name "$fn" --region "$REGION" \
    --query 'Code.ImageUri' --output text)
  echo "[FIX]  $fn — $STATE → update-function-code ($IMG)"
  aws lambda update-function-code --function-name "$fn" --region "$REGION" --image-uri "$IMG" >/dev/null
  aws lambda wait function-updated --function-name "$fn" --region "$REGION"
  NEW=$(aws lambda get-function-configuration --function-name "$fn" --region "$REGION" \
    --query 'State' --output text)
  echo "       → $NEW"
done
echo "Listo."
