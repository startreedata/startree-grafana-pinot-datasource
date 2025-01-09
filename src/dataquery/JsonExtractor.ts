import { ComplexField } from './ComplexField';

export const ResultTypes = ['INT', 'LONG', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'TIMESTAMP', 'STRING'];

export interface JsonExtractor {
  source?: ComplexField;
  path?: string;
  resultType?: string;
  alias?: string;
}
