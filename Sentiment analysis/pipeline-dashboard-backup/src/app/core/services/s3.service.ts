/**
 * S3Service — fachada sobre ApiService
 * =====================================
 * Mantiene la misma interfaz que antes para no romper
 * los componentes que ya la usan (S3ExplorerComponent).
 * Ahora delega todas las llamadas al ApiService (HTTP + API Gateway).
 */
import { Injectable } from '@angular/core';
import { Observable, map } from 'rxjs';
import { ApiService } from './api.service';
import { S3FileItem, S3BucketStats } from '../models/s3-file.model';

@Injectable({ providedIn: 'root' })
export class S3Service {

  constructor(private api: ApiService) {}

  // ─── Listar archivos bajo un prefijo ──────────────────────────────
  listFiles(prefix: string, maxKeys = 200): Observable<S3FileItem[]> {
    return this.api.listFiles(prefix, maxKeys).pipe(
      map(resp => resp.items.map(item => ({
        key:          item.key,
        name:         item.name,
        size:         item.size,
        lastModified: item.lastModified ? new Date(item.lastModified) : new Date(),
        etag:         item.etag,
        storageClass: item.storageClass,
        isFolder:     item.isFolder,
      } as S3FileItem)))
    );
  }

  // ─── Leer un JSON de S3 (vía /reports/{date}) ─────────────────────
  readJsonFile<T>(key: string): Observable<T> {
    // Si es un report.json → usamos el endpoint optimizado
    const match = key.match(/results\/(\d{4}-\d{2}-\d{2})\/report\.json/);
    if (match) {
      return this.api.getReport<T>(match[1]);
    }
    // Para otros JSON: listamos y descargamos vía presigned URL
    // (caso de uso menos común; el S3Explorer lo gestiona por su cuenta)
    throw new Error(`readJsonFile: ruta no soportada vía API: ${key}`);
  }

  // ─── URL prefirmada ───────────────────────────────────────────────
  getPresignedUrl(key: string): Observable<string> {
    return this.api.getPresignedUrl(key).pipe(map(r => r.url));
  }

  // ─── Estadísticas del bucket ──────────────────────────────────────
  getBucketStats(): Observable<S3BucketStats> {
    return this.api.getBucketStats().pipe(
      map(stats => ({
        totalFiles:    stats.totalFiles,
        totalSizeBytes: stats.totalBytes,
        lastUpdated:   stats.lastUpdated ? new Date(stats.lastUpdated) : new Date(),
        folderBreakdown: stats.breakdown.map(b => ({
          prefix:    b.prefix,
          sizeBytes: b.sizeBytes,
          fileCount: b.fileCount,
        })),
      } as S3BucketStats))
    );
  }
}
