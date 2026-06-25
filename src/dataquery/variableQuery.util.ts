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

/**
 * True when the variable currently has its "All" option selected (or has no concrete
 * selection). Grafana represents an all-values pick with the special `$__all` token — as a
 * scalar, or inside a multi-value array — or with an empty value. Works for any variable type
 * that carries options (query, custom, etc.), so it is typed against TypedVariableModel.
 */
export function isAllSelected(variable: TypedVariableModel): boolean {
  const current = 'current' in variable ? variable.current : undefined;
  const value = current && 'value' in current ? current.value : undefined;
  if (typeof value === 'string') {
    return value === '' || value === '$__all';
  }
  if (Array.isArray(value)) {
    return value.length === 0 || value.includes('$__all');
  }
  return false;
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
