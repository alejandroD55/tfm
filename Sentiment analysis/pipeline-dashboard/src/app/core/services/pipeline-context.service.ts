import { Injectable, signal } from '@angular/core';
import { Observable, Subject, map, tap, catchError, switchMap, of } from 'rxjs';
import { ApiService } from './api.service';
import { PipelineRun } from '../models/pipeline-run.model';
import {
  formatPipelineMetaSubtitle,
  formatPipelineRangeLabel,
} from '../utils/pipeline-display.util';

const STORAGE_KEY = 'tfm-selected-pipeline-id';

@Injectable({ providedIn: 'root' })
export class PipelineContextService {
  readonly pipelines = signal<PipelineRun[]>([]);
  readonly selectedPipeline = signal<PipelineRun | null>(null);
  readonly loading = signal(false);

  /** Emite cuando el usuario cambia de pipeline (recargar vistas). */
  private readonly changed = new Subject<PipelineRun | null>();
  readonly pipelineChanged$ = this.changed.asObservable();

  constructor(private api: ApiService) {}

  loadPipelines(): Observable<void> {
    this.loading.set(true);
    return this.api.listPipelines().pipe(
      map(resp => (resp.pipelines ?? []).map(p => this._mapPipeline(p))),
      // Fallback: si la API no tiene /pipelines o devuelve vacío,
      // construimos un pipeline "Historial completo" desde /reports
      switchMap(list => list.length > 0
        ? of(list)
        : this._buildFallbackPipeline()
      ),
      catchError(() => this._buildFallbackPipeline()),
      tap(list => {
        this.pipelines.set(list);
        const saved = sessionStorage.getItem(STORAGE_KEY);
        const selected = list.find(p => p.id === saved) ?? list[0] ?? null;
        this.selectedPipeline.set(selected);
        if (selected) sessionStorage.setItem(STORAGE_KEY, selected.id);
        this.loading.set(false);
      }),
      map(() => void 0),
    );
  }

  private _mapPipeline(p: any): PipelineRun {
    return {
      id:               p.id,
      label:            p.label ?? formatPipelineRangeLabel(p.start_date, p.end_date),
      startDate:        p.start_date,
      endDate:          p.end_date,
      reportCount:      p.report_count ?? 0,
      initialCapital:   p.initial_capital ?? 10_000,
      firstReportDate:  p.first_report_date,
      lastReportDate:   p.last_report_date,
      type:             p.type ?? 'independent',
      sourcePipelines:  p.source_pipelines ?? [],
    };
  }

  /** Construye un pipeline "Historial completo" desde las fechas disponibles en /reports */
  private _buildFallbackPipeline(): Observable<PipelineRun[]> {
    return this.api.listReports().pipe(
      map(resp => {
        const dates = (resp.dates ?? []).map((d: any) => d.date).sort();
        if (!dates.length) return [];
        const start = dates[0];
        const end   = dates[dates.length - 1];
        const run: PipelineRun = {
          id:            `${start}_${end}`,
          label:         formatPipelineRangeLabel(start, end),
          startDate:     start,
          endDate:       end,
          reportCount:   dates.length,
          initialCapital:10_000,
          firstReportDate: start,
          lastReportDate:  end,
          type:          'independent',
        };
        return [run];
      }),
      catchError(() => of([]))
    );
  }

  selectPipelineById(id: string): void {
    const p = this.pipelines().find(x => x.id === id);
    if (!p) return;
    this.selectedPipeline.set(p);
    sessionStorage.setItem(STORAGE_KEY, p.id);
    this.changed.next(p);
  }

  /** Último día con informe dentro del pipeline (snapshot backtesting). */
  pipelineEndDate(): string | null {
    const p = this.selectedPipeline();
    if (!p) return null;
    return p.lastReportDate ?? p.endDate;
  }

  dateFilter(): { start?: string; end?: string } {
    const p = this.selectedPipeline();
    if (!p) return {};
    return { start: p.startDate, end: p.endDate };
  }

  /** Etiqueta legible del rango (selector y cabeceras de vista). */
  rangeLabel(run: PipelineRun | null = this.selectedPipeline()): string {
    if (!run) return '';
    if (run.label) return run.label;
    return formatPipelineRangeLabel(run.startDate, run.endDate);
  }

  /** Subtítulo bajo el selector (días hábiles y capital). */
  metaSubtitle(run: PipelineRun | null = this.selectedPipeline()): string {
    if (!run) return '';
    return formatPipelineMetaSubtitle(run.reportCount, run.initialCapital);
  }
}
