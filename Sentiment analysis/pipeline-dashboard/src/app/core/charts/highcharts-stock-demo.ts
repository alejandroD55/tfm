/**
 * Highcharts Stock con módulos de exportación y chrome de demo (zoom, navigator, rangos).
 */
import Highcharts from 'highcharts/highstock';
import 'highcharts/modules/exporting';
import 'highcharts/modules/export-data';

export function initDemoHighcharts(): typeof Highcharts {
  return Highcharts;
}

/** Opciones comunes para gráficos stock en demos con cliente. */
export function demoStockChrome(height: number): Highcharts.Options {
  return {
    chart: {
      height,
      backgroundColor: 'transparent',
      zooming: { type: 'x' },
      panning: { enabled: true, type: 'x' },
      panKey: 'shift',
      style: { fontFamily: 'inherit' },
    },
    credits: { enabled: false },
    rangeSelector: {
      enabled: true,
      inputEnabled: true,
      inputDateFormat: '%d/%m/%Y',
      buttonTheme: {
        fill: '#f1f5f9',
        stroke: '#cbd5e1',
        r: 4,
        style: { color: '#334155', fontWeight: '600', fontSize: '11px' },
        states: {
          hover: { fill: '#e2e8f0' },
          select: { fill: '#2563eb', style: { color: '#fff' } },
        },
      },
      buttons: [
        { type: 'month', count: 1, text: '1M' },
        { type: 'month', count: 3, text: '3M' },
        { type: 'month', count: 6, text: '6M' },
        { type: 'year', count: 1, text: '1A' },
        { type: 'all', text: 'Todo' },
      ],
      selected: 4,
    },
    navigator: {
      enabled: true,
      height: 44,
      maskFill: 'rgba(37, 99, 235, 0.12)',
      outlineColor: '#cbd5e1',
      handles: {
        backgroundColor: '#2563eb',
        borderColor: '#1d4ed8',
      },
    },
    scrollbar: {
      enabled: true,
      barBackgroundColor: '#e2e8f0',
      barBorderRadius: 4,
      buttonBackgroundColor: '#cbd5e1',
      trackBackgroundColor: '#f8fafc',
      trackBorderRadius: 4,
    },
    exporting: {
      enabled: true,
      sourceWidth: 1280,
      chartOptions: {
        chart: { backgroundColor: '#ffffff' },
      },
      buttons: {
        contextButton: {
          symbolStroke: '#475569',
          menuItems: [
            'viewFullscreen',
            'printChart',
            'separator',
            'downloadPNG',
            'downloadJPEG',
            'downloadPDF',
            'downloadSVG',
            'separator',
            'downloadCSV',
            'downloadXLS',
            'viewData',
          ],
        },
      },
    },
    lang: {
      viewFullscreen: 'Pantalla completa',
      printChart: 'Imprimir',
      downloadPNG: 'Descargar PNG',
      downloadJPEG: 'Descargar JPEG',
      downloadPDF: 'Descargar PDF',
      downloadSVG: 'Descargar SVG',
      downloadCSV: 'Descargar CSV',
      downloadXLS: 'Descargar Excel',
      viewData: 'Ver datos',
      resetZoom: 'Restablecer zoom',
    },
  };
}

export function mergeStockOptions(
  chrome: Highcharts.Options,
  partial: Highcharts.Options,
): Highcharts.Options {
  return Highcharts.merge(chrome, partial);
}
