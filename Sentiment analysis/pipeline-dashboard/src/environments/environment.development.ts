/**
 * Desarrollo local: ng serve --configuration development (npm run start:dev).
 * Requiere API en http://localhost:8000 (scripts/run_api_local.sh desde tfm/).
 * apiKey debe coincidir con DASHBOARD_API_KEY en tfm/.env
 */
export const environment = {
  production: false,
  // API FastAPI (docker compose / run_api_local.sh) — rutas en raíz: /reports, /trace, ...
  apiGatewayUrl: 'http://localhost:8000',
  apiKey: '25aded11b15417a5580f631e432efad66848df1fa2f620e94d26d6b588486431',
  dashboardAuth: {
    username: 'admin',
    password: 'tfm2024',
  },
  pollingIntervalMs: 60_000,
};
