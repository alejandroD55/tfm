import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';

interface NavItem {
  label:       string;
  icon:        string;
  route:       string;
  description: string;
  badge?:      string;
}

@Component({
  selector:    'app-sidebar',
  standalone:  true,
  imports:     [CommonModule, RouterModule, MatIconModule, MatTooltipModule],
  templateUrl: './sidebar.component.html',
  styleUrl:    './sidebar.component.scss',
})
export class SidebarComponent {

  mainNav: NavItem[] = [
    {
      label:       'Portfolio',
      icon:        'space_dashboard',
      route:       '/dashboard',
      description: 'Resumen general · KPIs · señales activas',
    },
    {
      label:       'Señales',
      icon:        'psychology',
      route:       '/signals',
      description: 'BUY / SELL / HOLD · cadena de decisión bayesiana',
      badge:       'AI',
    },
    {
      label:       'Backtesting',
      icon:        'insights',
      route:       '/backtesting',
      description: 'Sharpe · Drawdown · Alpha vs Buy & Hold',
    },
    {
      label:       'Explorador',
      icon:        'search',
      route:       '/explorer',
      description: 'Noticias raw · OHLCV · decisión por ticker · lanzar pipeline',
      badge:       'NEW',
    },
  ];

  infraNav: NavItem[] = [
    {
      label:       'Pipeline',
      icon:        'account_tree',
      route:       '/pipeline',
      description: 'Estado de las 5 Lambdas y batches diarios',
    },
    {
      label:       'Auditoría',
      icon:        'manage_search',
      route:       '/audit',
      description: 'Trazabilidad completa · CPT · decisiones implícitas',
      badge:       'NEW',
    },
  ];
}
