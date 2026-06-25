import { ComplexField } from './ComplexField';

export const ResultTypes = ['INT', 'LONG', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'TIMESTAMP', 'STRING'];

export interface JsonExtractor {
  source?: ComplexField;
  path?: string;
  resultType?: string;
  alias?: string;
  // Optional Grafana data-link URL template for the extracted field, e.g.
  // `https://trace.example/${__value.raw}`. Surfaced as a clickable link in the log row details.
  link?: string;
}
