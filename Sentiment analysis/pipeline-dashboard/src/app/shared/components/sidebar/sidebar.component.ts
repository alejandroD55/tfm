import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';

interface NavItem {
  label: string;
  icon: string;
  route: string;
  description: string;
  badge?: string;
}

@Component({
  selector: 'app-sidebar',
  standalone: true,
  imports: [CommonModule, RouterModule, MatIconModule, MatTooltipModule],
  template: `
    <div class="sb">

      <div class="sb-brand">
        <div class="sb-brand-mark">
          <div class="sb-brand-glow"></div>
          <span class="sb-brand-letter">F×B</span>
        </div>
        <div class="sb-brand-text">
          <div class="sb-brand-title">Aurora</div>
          <div class="sb-brand-sub">FinBERT × Bayesian</div>
        </div>
      </div>

      <div class="sb-status">
        <span class="sb-led"></span>
        <span class="sb-status-text">Pipeline operativo</span>
      </div>

      <div class="sb-section">
        <div class="sb-section-label">Análisis</div>
        <nav class="sb-nav">
          @for (item of mainNav; track item.route) {
            <a class="sb-link"
               [routerLink]="item.route"
               routerLinkActive="active"
               [matTooltip]="item.description"
               matTooltipPosition="right">
              <mat-icon class="sb-link-icon">{{ item.icon }}</mat-icon>
              <span class="sb-link-label">{{ item.label }}</span>
              @if (item.badge) {
                <span class="sb-badge">{{ item.badge }}</span>
              }
              <span class="sb-link-arrow">
                <mat-icon>chevron_right</mat-icon>
              </span>
            </a>
          }
        </nav>
      </div>

      <div class="sb-divider"></div>

      <div class="sb-section">
        <div class="sb-section-label">Infraestructura</div>
        <nav class="sb-nav">
          @for (item of infraNav; track item.route) {
            <a class="sb-link"
               [routerLink]="item.route"
               routerLinkActive="active"
               [matTooltip]="item.description"
               matTooltipPosition="right">
              <mat-icon class="sb-link-icon">{{ item.icon }}</mat-icon>
              <span class="sb-link-label">{{ item.label }}</span>
              <span class="sb-link-arrow">
                <mat-icon>chevron_right</mat-icon>
              </span>
            </a>
          }
        </nav>
      </div>

      <div class="sb-footer">
        <div class="sb-foot-row">
          <mat-icon>verified</mat-icon>
          <div>
            <div class="sb-foot-title">TFM Trading System</div>
            <div class="sb-foot-sub">v1.0 · AWS Lambda + EKS</div>
          </div>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .sb {
      height: 100%;
      display: flex; flex-direction: column;
      color: var(--side-fg);
      padding: 20px 14px 16px;
      gap: 14px;
    }

    /* Brand */
    .sb-brand {
      display: flex; align-items: center; gap: 12px;
      padding: 6px 8px 4px;
    }
    .sb-brand-mark {
      position: relative;
      width: 40px; height: 40px;
      border-radius: 12px;
      background: linear-gradient(135deg, #2563EB 0%, #06B6D4 100%);
      display: flex; align-items: center; justify-content: center;
      box-shadow: 0 6px 14px rgba(37, 99, 235, .35), inset 0 1px 0 rgba(255,255,255,.25);
    }
    .sb-brand-glow {
      position: absolute; inset: -2px;
      border-radius: 14px;
      background: conic-gradient(from 220deg, rgba(37,99,235,.6), rgba(6,182,212,.6), rgba(124,58,237,.4), rgba(37,99,235,.6));
      filter: blur(10px); opacity: .55;
      z-index: -1;
    }
    .sb-brand-letter {
      font-weight: 800; color: #fff;
      font-size: 16px; letter-spacing: -.02em;
    }
    .sb-brand-text { line-height: 1.15; }
    .sb-brand-title {
      font-weight: 700; font-size: 15px;
      letter-spacing: -.01em; color: #fff;
    }
    .sb-brand-sub {
      font-size: 11px;
      color: var(--side-fg-dim);
      font-weight: 500;
      letter-spacing: .02em;
    }

    /* Status pill */
    .sb-status {
      display: inline-flex; align-items: center; gap: 8px;
      padding: 6px 12px; margin: 0 4px;
      background: rgba(34, 197, 94, .08);
      border: 1px solid rgba(34, 197, 94, .25);
      color: #86EFAC;
      border-radius: var(--r-pill);
      font-size: 11px; font-weight: 600;
      letter-spacing: .02em;
      width: fit-content;
    }
    .sb-led {
      width: 7px; height: 7px; border-radius: 50%;
      background: var(--success-500);
      box-shadow: 0 0 0 3px rgba(34,197,94,.25);
      animation: sb-pulse 2.2s ease-in-out infinite;
    }
    @keyframes sb-pulse {
      0%,100% { box-shadow: 0 0 0 3px rgba(34,197,94,.25); }
      50%     { box-shadow: 0 0 0 6px rgba(34,197,94,.10); }
    }

    /* Section + nav */
    .sb-section { display: flex; flex-direction: column; gap: 4px; }
    .sb-section-label {
      padding: 4px 12px;
      font-size: 10px; font-weight: 700;
      letter-spacing: .15em; text-transform: uppercase;
      color: var(--side-fg-muted);
    }
    .sb-nav { display: flex; flex-direction: column; gap: 2px; }

    .sb-link {
      position: relative;
      display: flex; align-items: center; gap: 12px;
      padding: 10px 12px;
      border-radius: var(--r-sm);
      color: var(--side-fg-dim);
      font-size: 13px; font-weight: 500;
      cursor: pointer;
      transition: background .18s, color .18s, transform .18s;
    }
    .sb-link::before {
      content: ''; position: absolute;
      left: -14px; top: 8px; bottom: 8px; width: 3px;
      background: transparent;
      border-radius: 0 3px 3px 0;
      transition: background .18s;
    }
    .sb-link:hover {
      background: rgba(255, 255, 255, .04);
      color: #fff;
    }
    .sb-link.active {
      background: var(--side-active);
      color: #fff;
    }
    .sb-link.active::before { background: var(--side-active-bar); }
    .sb-link-icon { color: inherit; font-size: 20px; height: 20px; width: 20px; }
    .sb-link-label { flex: 1; }
    .sb-link-arrow {
      opacity: 0; transform: translateX(-4px);
      transition: opacity .18s, transform .18s;
    }
    .sb-link-arrow mat-icon { font-size: 16px; height: 16px; width: 16px; color: inherit; }
    .sb-link.active .sb-link-arrow,
    .sb-link:hover .sb-link-arrow {
      opacity: .8; transform: translateX(0);
    }
    .sb-badge {
      font-size: 10px; font-weight: 700;
      padding: 2px 8px;
      background: var(--brand-600);
      color: #fff;
      border-radius: var(--r-pill);
      letter-spacing: .04em;
    }

    .sb-divider {
      height: 1px;
      background: linear-gradient(to right, transparent, rgba(255,255,255,.08), transparent);
      margin: 4px 4px;
    }

    /* Footer */
    .sb-footer {
      margin-top: auto;
      padding-top: 12px;
      border-top: 1px solid rgba(255,255,255,.06);
    }
    .sb-foot-row {
      display: flex; align-items: center; gap: 10px;
      padding: 6px 8px;
      mat-icon {
        color: var(--brand-400); font-size: 22px;
        height: 22px; width: 22px;
      }
    }
    .sb-foot-title { font-size: 12px; font-weight: 600; color: rgba(255,255,255,.78); }
    .sb-foot-sub   { font-size: 10px; color: var(--side-fg-muted); margin-top: 1px; letter-spacing: .02em; }

    /* Compact mode */
    @media (max-width: 1100px) {
      .sb { padding: 16px 8px; align-items: center; }
      .sb-brand-text, .sb-brand-sub, .sb-section-label,
      .sb-link-label, .sb-link-arrow, .sb-badge,
      .sb-status-text, .sb-foot-title, .sb-foot-sub { display: none; }
      .sb-status { padding: 6px; }
      .sb-link { justify-content: center; padding: 10px; }
      .sb-foot-row { justify-content: center; }
    }
  `],
})
export class SidebarComponent {
  mainNav: NavItem[] = [
    {
      label: 'Portfolio',
      icon: 'space_dashboard',
      route: '/dashboard',
      description: 'Resumen general · KPIs · señales activas',
    },
    {
      label: 'Señales',
      icon: 'psychology',
      route: '/signals',
      description: 'BUY / SELL / HOLD · cadena de decisión bayesiana',
      badge: 'AI',
    },
    {
      label: 'Backtesting',
      icon: 'insights',
      route: '/backtesting',
      description: 'Sharpe · Drawdown · Alpha vs Buy & Hold',
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
      label: 'Auditoría',
      icon: 'manage_search',
      route: '/audit',
      description: 'Trazabilidad completa · CPT · decisiones implícitas · sentimiento multi-headline',
      badge: 'NEW',
    }
  ];
}