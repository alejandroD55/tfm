import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatChipsModule } from '@angular/material/chips';
import { catchError, debounceTime, of, Subject, switchMap } from 'rxjs';
import {
  ApiService,
  InstrumentResult,
  WatchlistCoverageRow,
} from '../../core/services/api.service';
import { ReportService } from '../../core/services/report.service';

@Component({
  selector: 'app-watchlist',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    RouterModule,
    MatIconModule,
    MatButtonModule,
    MatProgressSpinnerModule,
    MatTooltipModule,
    MatChipsModule,
  ],
  templateUrl: './watchlist.component.html',
  styleUrl: './watchlist.component.scss',
})
export class WatchlistComponent implements OnInit {
  private api = inject(ApiService);
  private reportSvc = inject(ReportService);

  loading = true;
  saving = false;
  coverageLoading = false;
  pipelineRunning = false;
  pipelineMessage = '';

  name = 'Cartera de seguimiento';
  tickers: string[] = [];
  newTicker = '';
  selectedDate = '';

  coverageRows: WatchlistCoverageRow[] = [];
  coverageComplete = 0;
  coverageTotal = 0;
  coverageRatio = 0;

  searchQuery = '';
  searchResults: InstrumentResult[] = [];
  searching = false;
  private search$ = new Subject<string>();

  ngOnInit() {
    this.reportSvc.listAvailableDates().subscribe((dates) => {
      if (dates.length) this.selectedDate = dates[0].date;
      else this.selectedDate = new Date().toISOString().slice(0, 10);
      this.loadCoverage();
    });
    this.loadWatchlist();

    this.search$.pipe(
      debounceTime(350),
      switchMap((q) => {
        if (q.length < 2) {
          this.searchResults = [];
          this.searching = false;
          return of(null);
        }
        this.searching = true;
        return this.api.searchInstruments(q, 'ETF').pipe(catchError(() => of(null)));
      }),
    ).subscribe((resp) => {
      this.searching = false;
      if (resp) this.searchResults = resp.results;
    });
  }

  loadWatchlist() {
    this.loading = true;
    this.api.getWatchlist().subscribe({
      next: (w) => {
        this.name = w.name || this.name;
        this.tickers = w.tickers || [];
        this.loading = false;
        this.loadCoverage();
      },
      error: () => { this.loading = false; },
    });
  }

  loadCoverage() {
    if (!this.selectedDate) return;
    this.coverageLoading = true;
    this.api.getWatchlistCoverage(this.selectedDate).subscribe({
      next: (c) => {
        this.coverageRows = c.tickers;
        this.coverageComplete = c.complete;
        this.coverageTotal = c.total;
        this.coverageRatio = c.coverage_ratio;
        this.coverageLoading = false;
      },
      error: () => { this.coverageLoading = false; },
    });
  }

  onSearchInput() {
    this.search$.next(this.searchQuery.trim());
  }

  addFromSearch(r: InstrumentResult) {
    this.addTicker(r.symbol);
    this.searchQuery = '';
    this.searchResults = [];
  }

  addTicker(raw?: string) {
    const sym = (raw || this.newTicker).trim().toUpperCase();
    if (!sym) return;
    this.saving = true;
    this.api.addWatchlistTicker(sym).subscribe({
      next: (r) => {
        this.tickers = r.tickers;
        this.newTicker = '';
        this.saving = false;
        this.loadCoverage();
      },
      error: () => { this.saving = false; },
    });
  }

  removeTicker(sym: string) {
    this.saving = true;
    this.api.removeWatchlistTicker(sym).subscribe({
      next: (r) => {
        this.tickers = r.tickers;
        this.saving = false;
        this.loadCoverage();
      },
      error: () => { this.saving = false; },
    });
  }

  importDefaults() {
    const seed = ['SPY', 'QQQ', 'SMH', 'XLE', 'XLP', 'XLF', 'XLV', 'IWM', 'TLT', 'GLD'];
    this.saving = true;
    this.api.putWatchlist({ tickers: seed, name: this.name }).subscribe({
      next: (r) => {
        this.tickers = r.tickers;
        this.saving = false;
        this.loadCoverage();
      },
      error: () => { this.saving = false; },
    });
  }

  runPipeline(full: boolean) {
    this.pipelineRunning = true;
    this.pipelineMessage = '';
    this.api.runWatchlistPipeline({
      batch_date: this.selectedDate,
      only_missing: !full,
    }).subscribe({
      next: (r) => {
        this.pipelineRunning = false;
        this.pipelineMessage = r.message || r.status;
        if (r.status !== 'SKIPPED') {
          setTimeout(() => this.loadCoverage(), 8000);
        }
      },
      error: (e) => {
        this.pipelineRunning = false;
        this.pipelineMessage = e?.error?.detail || 'Error al lanzar pipeline';
      },
    });
  }

  rowStatus(row: WatchlistCoverageRow): string {
    if (row.complete) return 'ok';
    if (row.has_trace) return 'partial';
    if (row.has_raw_news) return 'warn';
    return 'missing';
  }
}
