/**
 * AuthService — autenticación local (sin Cognito)
 * ================================================
 * Valida usuario/contraseña contra environment.ts.
 * Las credenciales nunca salen del navegador.
 */
import { Injectable, signal } from '@angular/core';
import { Router } from '@angular/router';
import { environment } from '../../../environments/environment';

export interface AppUser {
  username: string;
}

const SESSION_KEY = 'tfm_dashboard_auth';

@Injectable({ providedIn: 'root' })
export class AuthService {
  readonly currentUser  = signal<AppUser | null>(null);
  readonly isAuthenticated = signal<boolean>(false);
  readonly isLoading    = signal<boolean>(false);

  constructor(private router: Router) {
    this.restoreSession();
  }

  // ─── Restaurar sesión de sessionStorage ──────────────────────────
  private restoreSession(): void {
    const stored = sessionStorage.getItem(SESSION_KEY);
    if (stored) {
      try {
        const user = JSON.parse(stored) as AppUser;
        this.currentUser.set(user);
        this.isAuthenticated.set(true);
      } catch {
        sessionStorage.removeItem(SESSION_KEY);
      }
    }
  }

  // ─── Login ───────────────────────────────────────────────────────
  async login(username: string, password: string): Promise<void> {
    this.isLoading.set(true);
    // Pequeño delay para simular llamada de red (mejor UX)
    await new Promise(r => setTimeout(r, 400));

    const { username: validUser, password: validPass } = environment.dashboardAuth;

    if (username === validUser && password === validPass) {
      const user: AppUser = { username };
      this.currentUser.set(user);
      this.isAuthenticated.set(true);
      sessionStorage.setItem(SESSION_KEY, JSON.stringify(user));
      this.router.navigate(['/dashboard']);
    } else {
      this.isLoading.set(false);
      throw new Error('Usuario o contraseña incorrectos');
    }
    this.isLoading.set(false);
  }

  // ─── Logout ──────────────────────────────────────────────────────
  logout(): void {
    this.currentUser.set(null);
    this.isAuthenticated.set(false);
    sessionStorage.removeItem(SESSION_KEY);
    this.router.navigate(['/login']);
  }

  // ─── Check session (usado en el guard) ───────────────────────────
  async checkSession(): Promise<void> {
    this.restoreSession();
  }
}
