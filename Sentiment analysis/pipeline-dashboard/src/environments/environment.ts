export const environment = {
  production: false,

  // ──────────────────────────────────────────────────────────────────
  // API Gateway — se rellena al ejecutar deploy_api.sh
  // El script imprime estos valores al final del despliegue.
  // ──────────────────────────────────────────────────────────────────
  // Producción/K8s: nginx proxea /api → API. Desarrollo: environment.development.ts usa :8000.
  // Desarrollo local: apunta directamente a la API en puerto 8000
  apiGatewayUrl: 'http://localhost:8000',
  apiKey:        '25aded11b15417a5580f631e432efad66848df1fa2f620e94d26d6b588486431',

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
