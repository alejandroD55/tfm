import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { MatExpansionModule } from '@angular/material/expansion';
import { NgxChartsModule } from '@swimlane/ngx-charts';
import { catchError, debounceTime, interval, of, Subject, switchMap, takeWhile } from 'rxjs';
import { ApiService, InstrumentResult, InstrumentProfile, PipelineStageStatus } from '../../core/services/api.service';
import { TraceService } from '../../core/services/trace.service';
import { ReportService } from '../../core/services/report.service';
import { ReportDateEntry } from '../../core/models/report.model';
import { TickerTrace } from '../../core/models/trace.model';

interface OhlcvRow {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}
interface Article {
  headline: string;
  url?: string;
  datetime?: number;
  source?: string;
  sentiment?: string;
  confidence?: number;
}

@Component({
  selector: 'app-ticker-explorer',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    MatIconModule,
    MatButtonModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatChipsModule,
    MatExpansionModule,
    NgxChartsModule,
  ],
  templateUrl: './ticker-explorer.component.html',
  styleUrl: './ticker-explorer.component.scss',
})
export class TickerExplorerComponent implements OnInit, OnDestroy {
  private api       = inject(ApiService);
  private traceSvc  = inject(TraceService);
  private reportSvc = inject(ReportService);

  // ─── Instrument search state ──────────────────────────────────────
  instrumentQuery      = '';
  instrumentFilter     = '';           // '', 'ETF', 'FUND', 'STOCK'
  instrumentResults:   InstrumentResult[]  = [];
  instrumentSearching  = false;
  selectedInstrument:  InstrumentResult | null  = null;
  instrumentProfile:   InstrumentProfile | null = null;
  private searchSubject = new Subject<string>();

  // ─── Ticker explorer state ────────────────────────────────────────
  tickerInput   = '';
  currentTicker = '';
  selectedDate  = '';
  loading       = false;

  availableTickers: string[] = [];
  filteredTickers:  string[] = [];
  availableDates:   ReportDateEntry[] = [];

  ohlcvData:     OhlcvRow[] = [];
  ohlcvChartData: any[]     = [];
  articles:      Article[]  = [];
  tickerTrace:   TickerTrace | null = null;

  pipelineRunning = false;
  pipelineExecution: any = null;
  pipelineTicker = '';
  pipelineElapsedSec = 0;
  pipelineStages: PipelineStageStatus[] = [
    { name: 'ingestion', status: 'PENDING' },
    { name: 'parallel', status: 'PENDING' },
    { name: 'bayesian', status: 'PENDING' },
    { name: 'report', status: 'PENDING' },
  ];
  pipelineProgressPct = 0;
  private pipelineTimer: ReturnType<typeof setInterval> | null = null;

  lineScheme: any = { domain: ['#3b82f6'] };
  chartView: [number, number] = [800, 260];

  // ─── Init ─────────────────────────────────────────────────────────
  ngOnInit() {
    // Cargar fechas y tickers disponibles en paralelo
    this.reportSvc.listAvailableDates().subscribe((dates) => {
      this.availableDates = dates;
      if (dates.length) this.selectedDate = dates[0].date;
    });

    this.api.getTickers().subscribe({
      next: (resp) => {
        this.availableTickers = resp.tickers;
        this.filteredTickers  = resp.tickers;
      },
      error: () => {},
    });

    // Búsqueda de instrumentos con debounce de 400ms
    this.searchSubject.pipe(
      debounceTime(400),
      switchMap(q => {
        if (q.length < 2) { this.instrumentResults = []; this.instrumentSearching = false; return of(null); }
        this.instrumentSearching = true;
        return this.api.searchInstruments(q, this.instrumentFilter).pipe(
          catchError(() => of(null))
        );
      })
    ).subscribe(resp => {
      this.instrumentSearching = false;
      if (resp) this.instrumentResults = resp.results;
    });
  }

  ngOnDestroy() {
    this.searchSubject.complete();
    this.stopPipelineTimer();
  }

  // ─── Instrument search ────────────────────────────────────────────
  onInstrumentSearch(value: string) {
    this.instrumentQuery = value;
    this.searchSubject.next(value);
  }

  triggerInstrumentSearch() {
    if (this.instrumentQuery.length >= 2) {
      this.instrumentSearching = true;
      this.api.searchInstruments(this.instrumentQuery, this.instrumentFilter)
        .pipe(catchError(() => of(null)))
        .subscribe(resp => {
          this.instrumentSearching = false;
          if (resp) this.instrumentResults = resp.results;
        });
    }
  }

  setInstrumentFilter(filter: string) {
    this.instrumentFilter = filter;
    if (this.instrumentQuery.length >= 2) this.triggerInstrumentSearch();
  }

  selectInstrument(r: InstrumentResult) {
    if (this.selectedInstrument?.symbol === r.symbol) {
      this.selectedInstrument = null;
      this.instrumentProfile  = null;
      return;
    }
    this.selectedInstrument = r;
    this.instrumentProfile  = null;
    this.api.getInstrumentProfile(r.symbol).pipe(catchError(() => of(null)))
      .subscribe(profile => { this.instrumentProfile = profile; });
  }

  loadInstrumentInExplorer(r: InstrumentResult) {
    this.tickerInput = r.displaySymbol || r.symbol;
    this.selectInstrument(r);
    this.loadTicker();
    // Scroll suave al explorador
    setTimeout(() => {
      document.querySelector('.search-section')?.scrollIntoView({ behavior: 'smooth' });
    }, 100);
  }

  runPipelineForInstrument(r: InstrumentResult) {
    this.tickerInput = r.displaySymbol || r.symbol;
    this.triggerPipeline();
  }

  // ─── Ticker input ─────────────────────────────────────────────────
  onTickerInput(value: string) {
    const q = value.toUpperCase();
    this.filteredTickers = this.availableTickers.filter((t) => t.startsWith(q));
  }

  selectTicker(ticker: string) {
    this.tickerInput = ticker;
    this.loadTicker();
  }

  // ─── Cargar datos del ticker ──────────────────────────────────────
  loadTicker() {
    const ticker = this.tickerInput.trim().toUpperCase();
    if (!ticker || !this.selectedDate) return;

    this.currentTicker = ticker;
    this.loading = true;
    this.ohlcvData = [];
    this.articles = [];
    this.tickerTrace = null;

    // Cargar en paralelo: OHLCV + Noticias + Trace
    Promise.all([
      this.loadOhlcv(ticker),
      this.loadNews(ticker),
      this.loadTrace(ticker),
    ]).finally(() => {
      this.loading = false;
    });
  }

  private loadOhlcv(ticker: string): Promise<void> {
    return new Promise((resolve) => {
      this.api
        .getRawOhlcv(this.selectedDate, ticker)
        .pipe(catchError(() => of(null)))
        .subscribe((resp) => {
          if (resp?.data) {
            this.ohlcvData = resp.data;
            this.ohlcvChartData = [
              {
                name: ticker,
                series: resp.data.map((r: OhlcvRow) => ({
                  name: r.date,
                  value: r.close,
                })),
              },
            ];
          }
          resolve();
        });
    });
  }

  private loadNews(ticker: string): Promise<void> {
    return new Promise((resolve) => {
      this.api
        .getRawNews(this.selectedDate, ticker)
        .pipe(catchError(() => of(null)))
        .subscribe((resp) => {
          this.articles = resp?.articles ?? [];
          resolve();
        });
    });
  }

  private loadTrace(ticker: string): Promise<void> {
    return new Promise((resolve) => {
      this.api
        .getTickerTrace(this.selectedDate, ticker)
        .pipe(catchError(() => of(null)))
        .subscribe((resp) => {
          this.tickerTrace = resp?.trace ?? null;
          // Enriquecer artículos con sentiment del trace si está disponible
          if (this.tickerTrace?.sentiment_detail?.headlines_sample) {
            const sample = this.tickerTrace.sentiment_detail.headlines_sample;
            this.articles = this.articles.map((art) => {
              const match = sample.find((s: any) =>
                art.headline?.includes(s.headline?.slice(0, 40)),
              );
              return match
                ? {
                    ...art,
                    sentiment: match.sentiment,
                    confidence: match.confidence,
                  }
                : art;
            });
          }
          resolve();
        });
    });
  }

  // ─── Pipeline trigger ─────────────────────────────────────────────
  triggerPipeline() {
    const ticker = this.tickerInput.trim().toUpperCase();
    if (!ticker || this.pipelineRunning) return;

    this.pipelineTicker = ticker;
    this.currentTicker = ticker;
    this.pipelineRunning = true;
    this.pipelineExecution = null;
    this.pipelineElapsedSec = 0;
    this.pipelineProgressPct = 0;
    this.pipelineStages = this.pipelineStages.map((s) => ({ ...s, status: 'PENDING' }));
    this.startPipelineTimer();

    this.api.runPipeline({ ticker, batch_date: this.selectedDate }).subscribe({
      next: (exec) => {
        this.pipelineExecution = exec;
        this.pollPipelineStatus(exec.executionArn);
      },
      error: (err) => {
        this.pipelineRunning = false;
        this.stopPipelineTimer();
        this.pipelineExecution = {
          status: 'FAILED',
          message: err.error?.detail ?? 'Error al lanzar',
        };
      },
    });
  }

  private pollPipelineStatus(arn: string) {
    interval(5000)
      .pipe(
        switchMap(() =>
          this.api.getPipelineStatus(arn).pipe(catchError(() => of(null))),
        ),
        takeWhile((s) => s?.status === 'RUNNING', true),
      )
      .subscribe((status) => {
        if (status) {
          this.pipelineExecution = status;
          if (status.stages?.length) {
            this.pipelineStages = status.stages;
          }
          if (typeof status.progressPct === 'number') {
            this.pipelineProgressPct = status.progressPct;
          }
        }
        if (status?.status !== 'RUNNING') {
          this.pipelineRunning = false;
          this.stopPipelineTimer();
          if (status?.status === 'SUCCEEDED') {
            this.pipelineProgressPct = 100;
          }
          if (status?.status === 'SUCCEEDED') {
            // Auto-recarga de datos al completar para evitar que el usuario
            // tenga que pulsar "Explorar" manualmente.
            this.loadTicker();
            setTimeout(() => {
              document.querySelector('.ticker-banner')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 120);
          }
        }
      });
  }

  private startPipelineTimer() {
    this.stopPipelineTimer();
    this.pipelineTimer = setInterval(() => {
      this.pipelineElapsedSec += 1;
    }, 1000);
  }

  private stopPipelineTimer() {
    if (this.pipelineTimer) {
      clearInterval(this.pipelineTimer);
      this.pipelineTimer = null;
    }
  }

  formatElapsed(seconds: number): string {
    const mm = Math.floor(seconds / 60).toString().padStart(2, '0');
    const ss = Math.floor(seconds % 60).toString().padStart(2, '0');
    return `${mm}:${ss}`;
  }

  pipelineStatusText(status?: string): string {
    const s = (status || '').toUpperCase();
    if (s === 'RUNNING') return 'Pipeline en ejecución';
    if (s === 'SUCCEEDED') return 'Pipeline completado';
    if (s === 'FAILED') return 'Pipeline fallido';
    if (s === 'ABORTED') return 'Pipeline abortado';
    return 'Lanzando ejecución...';
  }

  stageLabel(name: string): string {
    const map: Record<string, string> = {
      ingestion: 'Ingestión',
      parallel: 'Sentimiento + Indicadores',
      bayesian: 'Bayesiano',
      report: 'Reporte',
    };
    return map[name] || name;
  }

  pipelineStatusIcon(status: string): string {
    return (
      {
        RUNNING: 'pending',
        SUCCEEDED: 'check_circle',
        FAILED: 'error',
        ABORTED: 'cancel',
      }[status] ?? 'help'
    );
  }

  // ─── Helpers de presentación ──────────────────────────────────────
  getTraceStates() {
    const d = this.tickerTrace?.discretization;
    if (!d) return [];
    return [
      {
        label: 'Sentimiento',
        value: d.sentiment_state,
        cls: d.sentiment_state,
      },
      { label: 'RSI', value: d.rsi_state, cls: d.rsi_state },
      { label: 'Tendencia', value: d.trend_state, cls: d.trend_state },
      {
        label: 'Volatilidad',
        value: d.volatility_state,
        cls: `vol-${d.volatility_state}`,
      },
    ];
  }

  getSentimentDist() {
    const dist = this.tickerTrace?.sentiment_detail?.distribution;
    if (!dist) return [];
    return Object.entries(dist).map(([key, v]: [string, any]) => ({
      key,
      count: v.count,
      pct: v.pct,
    }));
  }
}
