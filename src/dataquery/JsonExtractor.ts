import { ComplexField } from './ComplexField';

export interface JsonExtractor {
  source?: ComplexField;
  path?: string;
  resultType?: string;
  alias?: string;
}
