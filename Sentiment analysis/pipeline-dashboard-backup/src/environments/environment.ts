export const environment = {
  production: false,

  // ──────────────────────────────────────────────────────────────────
  // API Gateway — se rellena al ejecutar deploy_api.sh
  // El script imprime estos valores al final del despliegue.
  // ──────────────────────────────────────────────────────────────────
  // En EKS, nginx proxea /api/ → pod tfm-api-service. No necesitas URL absoluta.
  // En desarrollo local, puedes usar: 'http://localhost:8000' y arrancar main.py con uvicorn.
  apiGatewayUrl: '/api',
  apiKey:        'REEMPLAZA_CON_TU_API_KEY',   // el mismo que pases a deploy_k8s.sh

  // ──────────────────────────────────────────────────────────────────
  // Credenciales locales del dashboard (no AWS)
  // Cambia user/password por los que quieras usar en la demo
  // ──────────────────────────────────────────────────────────────────
  dashboardAuth: {
    username: 'admin',
    password: 'tfm2024',
  },

  // ──────────────────────────────────────────────────────────────────
  // Polling — intervalo de refresco automático (ms)
  // ──────────────────────────────────────────────────────────────────
  pollingIntervalMs: 60_000,
};
