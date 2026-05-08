/**
 * ApiService — cliente HTTP hacia la API FastAPI
 * ===============================================
 * Punto único de acceso al backend. Añade x-api-key automáticamente.
 *
 * Endpoints disponibles:
 *   /health
 *   /reports  /reports/{date}
 *   /trace/{date}  /trace/{date}/{ticker}
 *   /model
 *   /sentiment/{date}/{ticker}
 *   /indicators/{date}/{ticker}
 *   /files  /files/presign  /stats
 */
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';
import { BayesianTrace, ModelConfigResponse } from '../models/trace.model';

// ─── DTOs de respuesta ────────────────────────────────────────────────────────

export interface ReportDateEntry {
  date:         string;
  s3Key:        string;
  lastModified: string;
  sizeBytes:    number;
  has_trace?:   boolean;
}

export interface FileItem {
  key:           string;
  name:          string;
  isFolder:      boolean;
  size:          number;
  lastModified?: string;
  etag?:         string;
  storageClass?: string;
}

export interface FileListResponse {
  items:                  FileItem[];
  prefix:                 string;
  isTruncated:            boolean;
  nextContinuationToken?: string;
}

export interface BucketStats {
  bucket:      string;
  totalFiles:  number;
  totalBytes:  number;
  lastUpdated: string | null;
  breakdown:   { prefix: string; fileCount: number; sizeBytes: number }[];
}

// ─── Service ──────────────────────────────────────────────────────────────────

@Injectable({ providedIn: 'root' })
export class ApiService {

  readonly baseUrl = environment.apiGatewayUrl;
  readonly authHeaders = new HttpHeaders({
    'x-api-key':    environment.apiKey,
    'Content-Type': 'application/json',
  });

  constructor(readonly http: HttpClient) {}

  // ─── Sistema ──────────────────────────────────────────────────────
  health(): Observable<{ status: string; timestamp: string; version: string }> {
    return this.http.get<any>(`${this.baseUrl}/health`);
  }

  // ─── Reports ──────────────────────────────────────────────────────
  listReports(): Observable<{ dates: ReportDateEntry[]; total: number }> {
    return this.http.get<any>(`${this.baseUrl}/reports`,
      { headers: this.authHeaders });
  }

  getReport<T>(date: string): Observable<T> {
    return this.http.get<T>(`${this.baseUrl}/reports/${date}`,
      { headers: this.authHeaders });
  }

  // ─── Trazabilidad bayesiana (nuevo) ───────────────────────────────

  /** Traza bayesiana completa de una fecha */
  getTrace(date: string): Observable<BayesianTrace> {
    return this.http.get<BayesianTrace>(`${this.baseUrl}/trace/${date}`,
      { headers: this.authHeaders });
  }

  /** Traza bayesiana de un ticker concreto */
  getTickerTrace(date: string, ticker: string): Observable<any> {
    return this.http.get<any>(`${this.baseUrl}/trace/${date}/${ticker.toUpperCase()}`,
      { headers: this.authHeaders });
  }

  /** Configuración completa del modelo bayesiano */
  getModelConfig(date?: string): Observable<ModelConfigResponse> {
    const params = date ? new HttpParams().set('date', date) : new HttpParams();
    return this.http.get<ModelConfigResponse>(`${this.baseUrl}/model`,
      { headers: this.authHeaders, params });
  }

  /** Distribución completa de sentimientos FinBERT para un ticker */
  getSentimentDetail(date: string, ticker: string): Observable<any> {
    return this.http.get<any>(
      `${this.baseUrl}/sentiment/${date}/${ticker.toUpperCase()}`,
      { headers: this.authHeaders });
  }

  /** Valores raw de indicadores técnicos y reglas de discretización */
  getIndicatorsDetail(date: string, ticker: string): Observable<any> {
    return this.http.get<any>(
      `${this.baseUrl}/indicators/${date}/${ticker.toUpperCase()}`,
      { headers: this.authHeaders });
  }

  // ─── Files ────────────────────────────────────────────────────────
  listFiles(prefix: string, maxKeys = 200): Observable<FileListResponse> {
    const params = new HttpParams()
      .set('prefix', prefix)
      .set('maxKeys', maxKeys.toString());
    return this.http.get<FileListResponse>(`${this.baseUrl}/files`,
      { headers: this.authHeaders, params });
  }

  getPresignedUrl(key: string): Observable<{ url: string; expiresInSeconds: number }> {
    const params = new HttpParams().set('key', key);
    return this.http.get<any>(`${this.baseUrl}/files/presign`,
      { headers: this.authHeaders, params });
  }

  // ─── Stats ────────────────────────────────────────────────────────
  getBucketStats(): Observable<BucketStats> {
    return this.http.get<BucketStats>(`${this.baseUrl}/stats`,
      { headers: this.authHeaders });
  }
}
