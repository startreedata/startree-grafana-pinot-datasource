export function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

export function extractColumnNameFromSubquery(subquery: string): string | undefined {
  const match = subquery.match(/SELECT\s+(?:DISTINCT\s+)?["`]?(\w+)["`]?\s+FROM/i);
  return match?.[1];
}

/**
 * Determines which form of subquery to use as a filter replacement:
 *  - Below threshold (≤1000 selected): returns null — the variable reference is left
 *    as-is and the template service later expands it to quoted literals.
 *  - All values selected: returns the raw subquery unchanged (no extra WHERE filtering needed).
 *  - Most values selected, excluded set ≤1000: returns subquery + NOT IN (excluded values) —
 *    avoids a huge IN clause by filtering out the small excluded set instead.
 *  - Excluded set too large to enumerate: falls back to the raw subquery.
 */
export function buildFilterSubqueryReplacement(
  allOptions: string[],
  selectedValues: string[],
  subquery: string,
  threshold: number
): string | null {
  const selectedCount = selectedValues.length;
  const totalCount = allOptions.length;

  if (selectedCount <= threshold) {
    return null;
  }

  if (selectedCount >= totalCount) {
    return subquery;
  }

  const selectedSet = new Set(selectedValues);
  const excludedValues = allOptions.filter((v) => !selectedSet.has(v));

  if (excludedValues.length > threshold) {
    return subquery;
  }

  // If the subquery pattern is too complex to parse (e.g. multi-column SELECT,
  // subquery expressions, or aliased columns), we can't build a NOT IN clause safely,
  // so return the original subquery unchanged.
  const column = extractColumnNameFromSubquery(subquery);
  if (!column) {
    return subquery;
  }

  const excludedLiterals = excludedValues.map((v) => `'${escapeSqlString(v)}'`).join(', ');
  const notInClause = `"${column}" NOT IN (${excludedLiterals})`;
  const hasWhere = /\bWHERE\b/i.test(subquery);

  if (hasWhere) {
    return `${subquery} AND ${notInClause}`;
  } else {
    return `${subquery} WHERE ${notInClause}`;
  }
}
