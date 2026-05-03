import { TypedVariableModel, QueryVariableModel } from '@grafana/data';
import type { PinotDataQuery } from './PinotDataQuery';

export function isQueryVariable(v: TypedVariableModel): v is QueryVariableModel {
  return v.type === 'query';
}

export function getVariableSubquery(variable: QueryVariableModel): string | undefined {
  const varQuery = variable.query as PinotDataQuery | undefined;
  return varQuery?.variableQuery?.pinotQlCode;
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
