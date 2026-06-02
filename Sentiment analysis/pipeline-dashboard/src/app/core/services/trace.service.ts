import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { ApiService } from './api.service';
import { BayesianTrace, TickerTrace, ModelConfigResponse } from '../models/trace.model';

@Injectable({ providedIn: 'root' })
export class TraceService {

  constructor(private api: ApiService) {}

  // ─── Endpoints wrapping ApiService ────────────────────────────────

  getTrace(date: string): Observable<BayesianTrace> {
    return this.api.getTrace(date);
  }

  getTickerTrace(date: string, ticker: string): Observable<any> {
    return this.api.getTickerTrace(date, ticker);
  }

  getModelConfig(date?: string): Observable<ModelConfigResponse> {
    return this.api.getModelConfig(date);
  }

  getSentimentDetail(date: string, ticker: string): Observable<any> {
    return this.api.getSentimentDetail(date, ticker);
  }

  getIndicatorsDetail(date: string, ticker: string): Observable<any> {
    return this.api.getIndicatorsDetail(date, ticker);
  }

  // ─── Helpers de presentación ──────────────────────────────────────

  /** Fuerza de la recomendación según distancia al punto de indiferencia (0.5) */
  signalStrength(probUp: number): 'strong' | 'moderate' | 'weak' {
    const dist = Math.abs(probUp - 0.5);
    if (dist > 0.25) return 'strong';
    if (dist > 0.15) return 'moderate';
    return 'weak';
  }

  /** Clase CSS de color según el estado discretizado */
  stateColorClass(node: string, state: string): string {
    const positive = ['bullish', 'oversold', 'uptrend', 'low'];
    const negative = ['bearish', 'overbought', 'downtrend', 'high'];
    if (positive.includes(state)) return 'positive';
    if (negative.includes(state)) return 'negative';
    return 'neutral';
  }

  /** Parsea la CPT de 36 valores en una tabla de filas legibles */
  parseCptMatrix(values_P_up: number[]): {
    sentiment: string; rsi: string; trend: string; volatility: string; prob_up: number;
  }[] {
    const sentiments   = ['bullish', 'bearish', 'neutral'];
    const rsis         = ['oversold', 'neutral', 'overbought'];
    const trends       = ['uptrend', 'downtrend'];
    const volatilities = ['low', 'high'];
    const rows: any[]  = [];
    let i = 0;
    for (const s of sentiments) {
      for (const r of rsis) {
        for (const t of trends) {
          for (const v of volatilities) {
            rows.push({ sentiment: s, rsi: r, trend: t, volatility: v,
                        prob_up: values_P_up[i] });
            i++;
          }
        }
      }
    }
    return rows;
  }

  /** Estado "positivo" para colores del prior */
  isPositiveState(state: string): boolean {
    return ['bullish', 'oversold', 'uptrend'].includes(state);
  }

  isNegativeState(state: string): boolean {
    return ['bearish', 'overbought', 'downtrend'].includes(state);
  }
}
