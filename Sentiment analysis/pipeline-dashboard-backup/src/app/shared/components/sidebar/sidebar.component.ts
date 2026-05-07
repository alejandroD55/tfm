import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatListModule } from '@angular/material/list';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDividerModule } from '@angular/material/divider';

interface NavItem {
  label: string;
  icon: string;
  route: string;
  description: string;
}

@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [CommonModule, RouterModule, MatListModule, MatIconModule, MatTooltipModule, MatDividerModule],
  template: `
    <div class="sidebar">
      <div class="sidebar-section-label">ANÁLISIS</div>
      <nav mat-nav-list>
        @for (item of mainNav; track item.route) {
          <a
            mat-list-item
            [routerLink]="item.route"
            routerLinkActive="active-link"
            [matTooltip]="item.description"
            matTooltipPosition="right"
            class="nav-item"
          >
            <mat-icon matListItemIcon>{{ item.icon }}</mat-icon>
            <span matListItemTitle>{{ item.label }}</span>
          </a>
        }
      </nav>

      <div class="sidebar-divider"></div>
      <div class="sidebar-section-label">INFRAESTRUCTURA</div>
      <nav mat-nav-list>
        @for (item of infraNav; track item.route) {
          <a
            mat-list-item
            [routerLink]="item.route"
            routerLinkActive="active-link"
            [matTooltip]="item.description"
            matTooltipPosition="right"
            class="nav-item"
          >
            <mat-icon matListItemIcon>{{ item.icon }}</mat-icon>
            <span matListItemTitle>{{ item.label }}</span>
          </a>
        }
      </nav>

      <div class="sidebar-footer">
        <div class="footer-info">
          <mat-icon>memory</mat-icon>
          <div>
            <div class="footer-title">TFM Trading System</div>
            <div class="footer-sub">FinBERT + Red Bayesiana</div>
          </div>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .sidebar {
      width: 240px;
      height: 100%;
      background: #1a237e;
      display: flex;
      flex-direction: column;
      padding-top: 8px;
    }
    .sidebar-section-label {
      padding: 8px 20px 4px;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 1.5px;
      color: rgba(255,255,255,.35);
    }
    mat-nav-list { padding-top: 0; }
    .nav-item {
      color: rgba(255,255,255,.7) !important;
      margin: 2px 10px;
      border-radius: 8px;
      transition: background .2s, color .2s;
    }
    .nav-item:hover {
      background: rgba(255,255,255,.12) !important;
      color: #fff !important;
    }
    .nav-item.active-link {
      background: rgba(255,255,255,.2) !important;
      color: #80cbc4 !important;
    }
    mat-icon[matListItemIcon] { color: inherit !important; }
    span[matListItemTitle]    { color: inherit !important; font-weight: 500; font-size: 14px; }
    .sidebar-divider {
      height: 1px;
      background: rgba(255,255,255,.1);
      margin: 8px 16px;
    }
    .sidebar-footer {
      margin-top: auto;
      padding: 12px 16px;
      border-top: 1px solid rgba(255,255,255,.1);
    }
    .footer-info {
      display: flex; align-items: center; gap: 10px;
      mat-icon { color: rgba(255,255,255,.35); font-size: 20px; }
    }
    .footer-title { font-size: 12px; color: rgba(255,255,255,.5); font-weight: 600; }
    .footer-sub   { font-size: 10px; color: rgba(255,255,255,.3); margin-top: 1px; }
  `],
})
export class SidebarComponent {
  mainNav: NavItem[] = [
    {
      label: 'Portfolio',
      icon: 'dashboard',
      route: '/dashboard',
      description: 'Resumen general: KPIs, señales activas y retornos',
    },
    {
      label: 'Señales Bayesianas',
      icon: 'psychology',
      route: '/signals',
      description: 'BUY/SELL/HOLD con cadena de decisión explicable',
    },
    {
      label: 'Backtesting',
      icon: 'analytics',
      route: '/backtesting',
      description: 'Estrategia vs Buy&Hold · Sharpe · Drawdown · Win rate',
    },
  ];

  infraNav: NavItem[] = [
    {
      label: 'Pipeline',
      icon: 'account_tree',
      route: '/pipeline',
      description: 'Estado de las 5 Lambdas y batches diarios',
    },
    {
      label: 'Explorador S3',
      icon: 'storage',
      route: '/s3-explorer',
      description: 'Navegar los buckets tfm-unir-datalake y tfm-unir-config',
    },
  ];
}
