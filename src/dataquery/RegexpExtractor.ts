import { ComplexField } from './ComplexField';

export interface RegexpExtractor {
  source?: ComplexField;
  pattern?: string;
  group?: number;
  alias?: string;
  // Optional Grafana data-link URL template for the extracted field, e.g.
  // `https://trace.example/${__value.raw}`. Surfaced as a clickable link in the log row details.
  link?: string;
}
