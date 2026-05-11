import { Component, OnInit, AfterViewInit, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatSortModule, MatSort } from '@angular/material/sort';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { switchMap } from 'rxjs';
import { ReportService } from '../../core/services/report.service';
import { DailyReport, TickerView, ReportDateEntry } from '../../core/models/report.model';
import { ChartDataPoint, ChartSeries } from '../../core/models/pipeline.model';

@Component({
  selector: 'app-backtesting',
  standalone: true,
  imports: [
    CommonModule, FormsModule, MatTableModule, MatSortModule,
    MatButtonModule, MatIconModule, MatProgressSpinnerModule,
    MatTooltipModule, MatExpansionModule, NgxChartsModule,
  ],
  templateUrl: './backtesting.component.html',
  styleUrl: './backtesting.component.scss',
})
export class BacktestingComponent implements OnInit, AfterViewInit {
  private reportSvc = inject(ReportService);

  @ViewChild(MatSort) sort!: MatSort;

  loading = true;
  availableDates: ReportDateEntry[] = [];
  selectedDate = '';
  tickerViews: TickerView[] = [];
  summary: any = { total_tickers: 0, avg_cumulative_return: 0, avg_sharpe_ratio: 0, avg_max_drawdown: 0, total_closed_trades: 0 };

  returnChart: ChartSeries[] = [];
  sharpeChart: ChartDataPoint[] = [];
  drawdownChart: ChartDataPoint[] = [];
  alphaChart: ChartDataPoint[] = [];

  tableSource = new MatTableDataSource<TickerView>();
  tableCols = ['ticker', 'signal', 'return', 'bh', 'alpha', 'sharpe', 'drawdown', 'equity', 'trades', 'winrate', 'pf'];

  compareScheme: any  = { domain: ['#2563EB', '#94A3B8'] }; // Azul corporativo (IA) vs Gris neutro (B&H)
  drawdownScheme: any = { domain: ['#EF4444'] }; // Rojo
  
  // Coloreado dinámico para Alpha (Verde si gana al mercado, Rojo si pierde)
  customColorsAlpha = (name: string) => {
    const item = this.alphaChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#22C55E' : '#EF4444'; 
  };

  // Coloreado dinámico para Sharpe (Verde/Azul si es positivo, Rojo si es negativo)
  customColorsSharpe = (name: string) => {
    const item = this.sharpeChart.find(d => d.name === name);
    return (item && item.value >= 0) ? '#06B6D4' : '#EF4444'; 
  };

  get winnersCount() { return this.tickerViews.filter(t => t.cumulative_return > 0).length; }

  ngOnInit() {
    this.reportSvc.listAvailableDates().pipe(
      switchMap(dates => {
        this.availableDates = dates;
        if (!dates.length) { this.loading = false; return []; }
        this.selectedDate = dates[0].date;
        return this.reportSvc.loadReport(this.selectedDate);
      })
    ).subscribe({
      next: (r: any) => { if (r) this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  // Activa la ordenación de las columnas de la tabla
  ngAfterViewInit() {
    this.tableSource.sort = this.sort;
  }

  onDateChange(date: string) {
    this.loading = true;
    this.reportSvc.loadReport(date).subscribe({
      next: r => { this.processReport(r); this.loading = false; },
      error: () => { this.loading = false; },
    });
  }

  private processReport(report: DailyReport) {
    this.summary = report.summary;
    this.tickerViews = this.reportSvc.buildTickerViews(report);
    this.tableSource.data = this.tickerViews;
    
    // Reconecta la ordenación
    if (this.sort) {
      this.tableSource.sort = this.sort;
    }

    // MAPEO FORZADO PARA EL GRÁFICO AGRUPADO: Eje X = ETF, Eje Y = IA vs Mercado
    this.returnChart = this.tickerViews.map(t => ({
      name: t.ticker,
      series: [
        { name: 'Estrategia IA', value: t.cumulative_return * 100 },
        { name: 'Mercado (Buy & Hold)', value: t.buy_hold_return * 100 }
      ]
    }));

    this.sharpeChart   = this.reportSvc.sharpeChart(this.tickerViews);
    this.drawdownChart = this.reportSvc.drawdownChart(this.tickerViews);
    this.alphaChart    = this.reportSvc.alphaChart(this.tickerViews);
  }

  qualityLabel(s: number) {
    if (s >= 2) return 'Excelente';
    if (s >= 1) return 'Bueno';
    if (s >= 0) return 'Aceptable';
    return 'Deficiente';
  }
}
