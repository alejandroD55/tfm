import { Component, OnDestroy, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterOutlet } from '@angular/router';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { SidebarComponent } from './shared/components/sidebar/sidebar.component';
import { PipelineContextService } from './core/services/pipeline-context.service';

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
  private readonly pipelineCtx = inject(PipelineContextService);
  title = 'pipeline-dashboard';
  now = new Date();
  private clockId?: number;

  ngOnInit(): void {
    this.clockId = window.setInterval(() => (this.now = new Date()), 30_000);
    this.pipelineCtx.loadPipelines().subscribe({
      error: () => { /* API sin /pipelines aún: vistas usan listReports global */ },
    });
  }

  ngOnDestroy(): void {
    if (this.clockId) clearInterval(this.clockId);
  }
}
