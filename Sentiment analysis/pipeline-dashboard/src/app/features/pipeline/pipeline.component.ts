import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { LegendPosition, NgxChartsModule } from '@swimlane/ngx-charts';
import { forkJoin, switchMap, of, Subject, takeUntil } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { PipelineContextService } from '../../core/services/pipeline-context.service';
import { DailyReport, PipelineHealth, BatchStatus } from '../../core/models/report.model';
import { ChartDataPoint } from '../../core/models/pipeline.model';

interface BatchSummary {
  date: string;
  status: string;
  tickers_expected: number;
  tickers_with_recommendations: number;
  headlines_scored: number;
  coverage_ratio: number;
  stage_kpis: Record<string, any>;
}

@Component({
  selector: 'app-pipeline',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatButtonModule, MatIconModule,
    MatProgressSpinnerModule, MatTooltipModule, MatExpansionModule, NgxChartsModule,
  ],
  templateUrl: './pipeline.component.html',
  styleUrl: './pipeline.component.scss',
})
export class PipelineComponent implements OnInit, OnDestroy {
  private reportSvc = inject(ReportService);
  private pipelineCtx = inject(PipelineContextService);
  private destroy$ = new Subject<void>();

  legendBelow = LegendPosition.Below;

  loading = true;
  batches: BatchSummary[] = [];
  latestHealth: PipelineHealth | null = null;
  totalDates = 0;

  // Selector de rango de fechas
  pageSize = 30;
  readonly pageSizeOptions = [
    { value: 14,   label: 'Últimas 2 semanas' },
    { value: 30,   label: 'Último mes' },
    { value: 90,   label: 'Últimos 3 meses' },
    { value: 9999, label: 'Todo el histórico' },
  ];

  // Gráficos
  statusPieChart: ChartDataPoint[] = [];
  coverageChart: ChartDataPoint[] = [];
  headlinesChart: ChartDataPoint[] = [];
  signalsChart: ChartDataPoint[] = [];

  // Paletas de color
  coverageScheme: any = { domain: ['#06B6D4'] }; // Cyan
  headlinesScheme: any = { domain: ['#8B5CF6'] }; // Violeta
  signalsScheme: any = { domain: ['#3B82F6'] }; // Azul

  customStatusColors = (name: string) => {
    if (name === 'Éxito') return '#22C55E';
    return '#EF4444'; // Error
  };

  // KPI Logic
  get completedCount() { return this.batches.filter(b => b.status === 'COMPLETED' || b.status === 'STARTED').length; }
  get failedCount()    { return this.batches.filter(b => b.status === 'FAILED').length; }
  get avgCoverage()    {
    if (!this.batches.length) return 0;
    return (this.batches.reduce((s, b) => s + b.coverage_ratio, 0) / this.batches.length) * 100;
  }

  ngOnInit() {
    this.pipelineCtx.pipelineChanged$.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.reportSvc.clearCache();
      this.refresh();
    });
    this.loadData();
  }

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  refresh()  { this.loading = true; this.batches = []; this.loadData(); }

  onPageSizeChange() { this.loading = true; this.batches = []; this.loadData(); }

  private loadData() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        if (!dates.length) { this.loading = false; return of([]); }
        this.totalDates = dates.length;
        const toLoad = dates.slice(0, this.pageSize);
        return forkJoin(toLoad.map(d => this.reportSvc.loadReport(d.date)));
      })
    ).subscribe({
      next: (reports: any[]) => {
        this.batches = reports.map((r: DailyReport) => {
          
          // --- MAGIA: Inyectamos los KPIs de la Lambda 5 artificialmente ---
          const stage_kpis = { ...r.pipeline_health.stage_kpis };
          stage_kpis['report'] = {
            tickers_reported: r.summary?.total_tickers || r.pipeline_health.tickers_expected,
            total_closed_trades: r.summary?.total_closed_trades || 0
          };

          return {
            date: r.report_date,
            status: r.pipeline_health.batch_status,
            tickers_expected: r.pipeline_health.tickers_expected,
            tickers_with_recommendations: r.pipeline_health.tickers_with_recommendations,
            headlines_scored: r.pipeline_health.headlines_scored,
            coverage_ratio: r.pipeline_health.coverage_ratio,
            stage_kpis: stage_kpis, // Ahora incluye la Lambda 5
          };
        });

        this.latestHealth = reports[0]?.pipeline_health ?? null;
        this.buildCharts();
        this.loading = false;
      },
      error: () => { this.loading = false; },
    });
  }

  private buildCharts() {
    // 1. Gráfico de Estados (Tarta)
    const counts: Record<string, number> = { COMPLETED: 0, FAILED: 0 };
    for (const b of this.batches) {
      if (b.status === 'COMPLETED' || b.status === 'STARTED') counts['COMPLETED']++;
      else counts['FAILED']++;
    }
    this.statusPieChart = [
      { name: 'Éxito', value: counts['COMPLETED'] },
      { name: 'Error', value: counts['FAILED'] }
    ].filter(i => i.value > 0);

    // 2. Gráfico de Cobertura (Barras) -> Invertimos para cronología correcta
    const sortedBatches = [...this.batches].reverse();

    this.coverageChart = sortedBatches.map(b => ({
      name: b.date.slice(5),
      value: +(b.coverage_ratio * 100).toFixed(1),
    }));

    // 3. Gráfico de Volumen de Noticias
    this.headlinesChart = sortedBatches.map(b => ({
      name: b.date.slice(5),
      value: b.headlines_scored,
    }));

  // 4. Gráfico de Recomendaciones Emitidas
    this.signalsChart = sortedBatches.map(b => ({
      name: b.date.slice(5),
      value: b.tickers_with_recommendations,
    }));
  }

  // Utilidades
  getStatusClass(s: string) {
    return (s === 'COMPLETED' || s === 'STARTED') ? 'completed' : 'failed';
  }

  statusIcon(s: string) {
    return (s === 'COMPLETED' || s === 'STARTED') ? 'check_circle' : 'error';
  }
  
  translateStatus(s: string) {
    return (s === 'COMPLETED' || s === 'STARTED') ? 'COMPLETADO' : 'ERROR';
  }

  stageIcon(key: string) {
    const m: Record<string, string> = {
      ingestion: 'download', sentiment: 'sentiment_satisfied',
      indicators: 'show_chart', bayesian: 'account_tree', report: 'summarize',
    };
    return m[key] ?? 'memory';
  }

  stageLabel(key: string) {
    const m: Record<string, string> = {
      ingestion: 'λ1 Ingesta API', sentiment: 'λ2 Modelo FinBERT',
      indicators: 'λ3 Índices Técnicos', bayesian: 'λ4 Inferencia Bayesiana', report: 'λ5 Reporte Fin.',
    };
    return m[key] ?? key;
  }
  
  translateKey(key: string): string {
    const dict: Record<string, string> = {
      'headlines_total': 'Noticias Totales',
      'ohlcv_rows_total': 'Filas de Precios',
      'tickers_expected': 'ETFs Esperados',
      'tickers_with_news': 'ETFs con Noticias',
      'tickers_with_ohlcv': 'ETFs con Precios',
      'tickers_in_ohlcv': 'ETFs Procesados',
      'tickers_with_indicators': 'Cálculos de RSI/SMA',
      'tickers_in_news': 'ETFs Analizados (NLP)',
      'headlines_skipped': 'Noticias Descartadas',
      'headlines_processed': 'Noticias Clasificadas',
      'trace_s3_key': 'Archivo Trace S3',
      'model_version': 'Versión del Modelo',
      'tickers_skipped': 'ETFs Omitidos',
      'signals_generated': 'Recomendaciones Generadas',
      'tickers_with_sentiment': 'Cruces NLP Exitósos',
      'tickers_reported': 'Activos Reportados',      // Nueva Key
      'total_closed_trades': 'Operaciones Cerradas'  // Nueva Key
    };
    return dict[key] || key.replace(/_/g, ' ');
  }

  // Fuerza el orden estricto: λ1, λ2, λ3, λ4, λ5
  getStages(kpis: Record<string, any>) {
    if (!kpis) return [];
    const orderedKeys = ['ingestion', 'sentiment', 'indicators', 'bayesian', 'report'];
    const result = [];
    
    for (const key of orderedKeys) {
      if (kpis[key]) {
        result.push({ key, value: kpis[key] });
      }
    }
    
    // Por seguridad, si aparece una lambda no registrada
    for (const key of Object.keys(kpis)) {
      if (!orderedKeys.includes(key)) {
        result.push({ key, value: kpis[key] });
      }
    }
    
    return result;
  }

  objectEntries(obj: Record<string, any>): [string, any][] {
    return Object.entries(obj);
  }
}
