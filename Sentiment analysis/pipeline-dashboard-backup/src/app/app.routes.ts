import { Routes } from '@angular/router';
import { authGuard } from './core/guards/auth.guard';
import { MainLayoutComponent } from './shared/layout/main-layout.component';

export const routes: Routes = [
  {
    path: 'login',
    loadComponent: () =>
      import('./features/login/login.component').then(m => m.LoginComponent),
  },
  {
    path: '',
    component: MainLayoutComponent,
    canActivate: [authGuard],
    children: [
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
        path: 's3-explorer',
        loadComponent: () =>
          import('./features/s3-explorer/s3-explorer.component').then(m => m.S3ExplorerComponent),
      },
      {
        path: '',
        redirectTo: 'dashboard',
        pathMatch: 'full',
      },
    ],
  },
  {
    path: '**',
    redirectTo: 'dashboard',
  },
];
