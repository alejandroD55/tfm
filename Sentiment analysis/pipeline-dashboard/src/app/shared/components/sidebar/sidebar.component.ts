import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { PipelineContextService } from '../../../core/services/pipeline-context.service';

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
  imports:     [CommonModule, FormsModule, RouterModule, MatIconModule, MatTooltipModule],
  templateUrl: './sidebar.component.html',
  styleUrl:    './sidebar.component.scss',
})
export class SidebarComponent {
  readonly pipelineCtx = inject(PipelineContextService);

  onPipelineChange(id: string): void {
    this.pipelineCtx.selectPipelineById(id);
  }

  mainNav: NavItem[] = [
    {
      label:       'Portfolio',
      icon:        'space_dashboard',
      route:       '/dashboard',
      description: 'Exposición actual por activo · KPIs de rendimiento · resumen ejecutivo',
    },
    {
      label:       'Análisis de Sentimiento',
      icon:        'psychology',
      route:       '/signals',
      description: 'Por qué cada activo tiene ese % de exposición · cadena de decisión bayesiana · noticias',
      badge:       'AI',
    },
    {
      label:       'Backtesting',
      icon:        'insights',
      route:       '/backtesting',
      description: 'Sharpe · Drawdown · Alpha vs Buy & Hold · rendimiento histórico validado',
    },
  ];

  infraNav: NavItem[] = [
    {
      label:       'Auditoría del Modelo',
      icon:        'manage_search',
      route:       '/audit',
      description: 'Trazabilidad completa · CPT bayesiana · decisiones implícitas · explicabilidad',
    },
    {
      label:       'Pipeline',
      icon:        'account_tree',
      route:       '/pipeline',
      description: 'Estado interno de las etapas de procesamiento de datos',
    },
  ];
}
