import { Injectable, signal } from '@angular/core';
import { Observable, Subject, map, tap } from 'rxjs';
import { ApiService } from './api.service';
import { PipelineRun } from '../models/pipeline-run.model';

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
      map(resp =>
        (resp.pipelines ?? []).map(p => ({
          id: p.id,
          label: p.label,
          startDate: p.start_date,
          endDate: p.end_date,
          reportCount: p.report_count ?? 0,
          initialCapital: p.initial_capital ?? 10_000,
          firstReportDate: p.first_report_date,
          lastReportDate: p.last_report_date,
        } as PipelineRun))
      ),
      tap(list => {
        this.pipelines.set(list);
        const saved = sessionStorage.getItem(STORAGE_KEY);
        const selected =
          list.find(p => p.id === saved) ?? list[0] ?? null;
        this.selectedPipeline.set(selected);
        if (selected) {
          sessionStorage.setItem(STORAGE_KEY, selected.id);
        }
        this.loading.set(false);
      }),
      map(() => void 0),
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
}
