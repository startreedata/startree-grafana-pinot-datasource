import { TypedVariableModel, QueryVariableModel } from '@grafana/data';
import type { PinotDataQuery } from './PinotDataQuery';

export function isQueryVariable(v: TypedVariableModel): v is QueryVariableModel {
  return v.type === 'query';
}

/**
 * Returns the variable's backing Pinot SQL with trailing whitespace and a single
 * trailing `;` stripped. The trailing semicolon is fine when the SQL runs as a
 * standalone statement, but breaks when the SQL is injected as a subquery
 * (e.g. `IN (SELECT ... LIMIT 4000;)`), so we normalize it here.
 */
export function getVariableSubquery(variable: QueryVariableModel): string | undefined {
  const varQuery = variable.query as PinotDataQuery | undefined;
  return varQuery?.variableQuery?.pinotQlCode?.trim().replace(/;\s*$/, '');
}

export function getAllOptions(variable: QueryVariableModel): string[] {
  return (variable.options ?? [])
    .map((opt) => (typeof opt.value === 'string' ? opt.value : ''))
    .filter((v) => v !== '' && v !== '$__all');
}

export function getSelectedValues(variable: QueryVariableModel): string[] {
  const current = variable.current;
  if (!current || !('value' in current)) {
    return [];
  }
  const value = current.value;
  if (typeof value === 'string') {
    return value === '$__all' ? getAllOptions(variable) : [value];
  }
  if (Array.isArray(value)) {
    return value.includes('$__all') ? getAllOptions(variable) : value.filter((v) => v !== '$__all');
  }
  return [];
}
