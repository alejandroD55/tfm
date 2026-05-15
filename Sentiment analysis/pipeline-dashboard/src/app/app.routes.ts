import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: 'dashboard',
    loadComponent: () =>
      import('./features/dashboard/dashboard.component').then(m => m.DashboardComponent),
  },
  {
    path: 'signals',
    loadComponent: () =>
      import('./features/signals/signals.component').then(m => m.SignalsComponent),
  },
  {
    path: 'backtesting',
    loadComponent: () =>
      import('./features/backtesting/backtesting.component').then(m => m.BacktestingComponent),
  },
  {
    path: 'pipeline',
    loadComponent: () =>
      import('./features/pipeline/pipeline.component').then(m => m.PipelineComponent),
  },
  {
    path: 'audit',
    loadComponent: () =>
      import('./features/audit/audit.component').then(m => m.AuditComponent),
  },
    {
      path: 'explorer',
      loadComponent: () =>
        import('./features/ticker-explorer/ticker-explorer.component').then(m => m.TickerExplorerComponent),
    },
    {
      path: 'watchlist',
      loadComponent: () =>
        import('./features/watchlist/watchlist.component').then(m => m.WatchlistComponent),
    },
  {
    path: '',
    redirectTo: 'dashboard',
    pathMatch: 'full',
  },
  {
    path: '**',
    redirectTo: 'dashboard',
  },
];
