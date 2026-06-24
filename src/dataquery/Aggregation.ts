import { ComplexField } from './ComplexField';

export interface Aggregation {
  function?: string;
  column?: ComplexField;
}
