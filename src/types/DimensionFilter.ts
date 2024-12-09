export interface DimensionFilter {
  columnName?: string;
  columnKey?: string | null;
  operator?: string;
  valueExprs?: string[];
}
