import { Component, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { SidebarComponent } from './shared/components/sidebar/sidebar.component';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [
    CommonModule, RouterOutlet, MatIconModule, MatTooltipModule, SidebarComponent,
  ],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css',
})
export class AppComponent implements OnInit, OnDestroy {
  title = 'pipeline-dashboard';
  now = new Date();
  private clockId?: number;

  ngOnInit(): void {
    this.clockId = window.setInterval(() => (this.now = new Date()), 30_000);
  }

  ngOnDestroy(): void {
    if (this.clockId) clearInterval(this.clockId);
  }
}
