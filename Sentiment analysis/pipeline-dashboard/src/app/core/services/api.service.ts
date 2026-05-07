/**
 * ApiService — cliente HTTP hacia API Gateway
 * ============================================
 * Todas las llamadas al backend pasan por aquí.
 * Añade automáticamente la cabecera x-api-key.
 */
import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../../environments/environment';

export interface ReportDateEntry {
  date: string;
  s3Key: string;
  lastModified: string;
  sizeBytes: number;
}

export interface FileItem {
  key: string;
  name: string;
  isFolder: boolean;
  size: number;
  lastModified?: string;
  etag?: string;
  storageClass?: string;
}

export interface FileListResponse {
  items: FileItem[];
  prefix: string;
  isTruncated: boolean;
  nextContinuationToken?: string;
}

export interface BucketStats {
  bucket: string;
  totalFiles: number;
  totalBytes: number;
  lastUpdated: string | null;
  breakdown: { prefix: string; fileCount: number; sizeBytes: number }[];
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private readonly base = environment.apiGatewayUrl;
  private readonly headers = new HttpHeaders({
    'x-api-key': environment.apiKey,
    'Content-Type': 'application/json',
  });

  constructor(private http: HttpClient) {}

  // ─── Health ───────────────────────────────────────────────────────
  health(): Observable<{ status: string; timestamp: string }> {
    return this.http.get<any>(`${this.base}/health`);
  }

  // ─── Reports ──────────────────────────────────────────────────────
  listReports(): Observable<{ dates: ReportDateEntry[]; total: number }> {
    return this.http.get<any>(`${this.base}/reports`, { headers: this.headers });
  }

  getReport<T>(date: string): Observable<T> {
    return this.http.get<T>(`${this.base}/reports/${date}`, { headers: this.headers });
  }

  // ─── Files ────────────────────────────────────────────────────────
  listFiles(prefix: string, maxKeys = 200): Observable<FileListResponse> {
    const params = new HttpParams()
      .set('prefix', prefix)
      .set('maxKeys', maxKeys.toString());
    return this.http.get<FileListResponse>(`${this.base}/files`, {
      headers: this.headers, params,
    });
  }

  getPresignedUrl(key: string): Observable<{ url: string; expiresInSeconds: number }> {
    const params = new HttpParams().set('key', key);
    return this.http.get<any>(`${this.base}/files/presign`, {
      headers: this.headers, params,
    });
  }

  // ─── Stats ────────────────────────────────────────────────────────
  getBucketStats(): Observable<BucketStats> {
    return this.http.get<BucketStats>(`${this.base}/stats`, { headers: this.headers });
  }
}
