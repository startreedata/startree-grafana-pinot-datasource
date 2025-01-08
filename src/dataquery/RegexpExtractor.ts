import { ComplexField } from './ComplexField';

export interface RegexpExtractor {
  source?: ComplexField;
  pattern?: string;
  group?: number;
  alias?: string;
}
