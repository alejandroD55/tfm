export const environment = {
  production: false,

  // ──────────────────────────────────────────────────────────────────
  // API Gateway — se rellena al ejecutar deploy_api.sh
  // El script imprime estos valores al final del despliegue.
  // ──────────────────────────────────────────────────────────────────
  // En EKS, nginx proxea /api/ → pod tfm-api-service. No necesitas URL absoluta.
  // En docker compose local, nginx proxea /api/ → tfm-api:8000.
  // En ng serve, proxy.conf.json redirige /api/ → http://localhost:8000.
  apiGatewayUrl: '/api',
  apiKey:        'dev-local-key',   // coincide con DASHBOARD_API_KEY en docker-compose / .env

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
