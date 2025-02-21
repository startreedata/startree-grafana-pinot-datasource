import { OrderByClause } from '../dataquery/OrderByClause';
import { ComplexField } from '../dataquery/ComplexField';
import { SelectableValue } from '@grafana/data';
import {columnLabelOf, parseColumnName} from "./complexField";

export interface FormData {
  options: SelectableValue<string>[];
  usedOptions: SelectableValue<string>[];

  getChange(items: SelectableValue<string>[]): OrderByClause[];
}

export function formDataOf(selected: OrderByClause[], columns: ComplexField[]): FormData {
  const usedColumns = new Set(selected.map(({ columnName, columnKey }) => columnLabelOf(columnName, columnKey)));

  const labelToClauseMap = new Map<string, OrderByClause>();
  selected.forEach((clause) => labelToClauseMap.set(labelOf(clause), clause));
  columns
    .filter((col) => !usedColumns.has(columnLabelOf(col.name, col.key)))
    .flatMap<OrderByClause>((col) => [
      { columnName: col.name, columnKey: col.key || undefined, direction: 'ASC' },
      { columnName: col.name, columnKey: col.key || undefined, direction: 'DESC' },
    ])
    .forEach((clause) => labelToClauseMap.set(labelOf(clause), clause));

  columns
    .filter((col) => !usedColumns.has(columnLabelOf(col.name, col.key)))
    .flatMap<OrderByClause>((col) => [
      { columnName: col.name, columnKey: col.key || undefined, direction: 'ASC' },
      { columnName: col.name, columnKey: col.key || undefined, direction: 'DESC' },
    ])
    .forEach((clause) => labelToClauseMap.set(labelOf(clause), clause));

  const usedOptions = selected.map((clause) => ({
    label: labelOf(clause),
    value: labelOf(clause),
  }));

  const options = Array.from(labelToClauseMap.values()).map((clause) => ({
    label: labelOf(clause),
    value: labelOf(clause),
  }));

  const getChange = (items: Array<SelectableValue<string>>) => {
    return items
      .map(({ label }) => labelToClauseMap.get(label || '') || parseOrderByClause(label))
      .filter(({ columnName }) => columnName);
  };

  return { options, usedOptions, getChange };
}

function labelOf({ columnName, columnKey, direction }: OrderByClause): string {
  return `${columnLabelOf(columnName, columnKey)} ${(direction || 'asc').toLowerCase()}`;
}

function parseOrderByClause(text: string | undefined): OrderByClause {
  if (!text) {
    return {};
  }

  const [column, direction] = text.trim().split(/\s+/);
  const { name, key } = parseColumnName(column);
  return {
    columnName: name,
    columnKey: key,
    direction: direction?.toUpperCase(),
  };
}
