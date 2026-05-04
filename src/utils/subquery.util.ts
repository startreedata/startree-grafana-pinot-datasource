export function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

export function extractColumnNameFromSubquery(subquery: string): string | undefined {
  const match = subquery.match(/SELECT\s+(?:DISTINCT\s+)?["`]?(\w+)["`]?\s+FROM/i);
  return match?.[1];
}

/**
 * Decides whether to replace a variable's IN-clause expansion with a subquery.
 *
 * @param allOptions     The full set of distinct values returned by the variable's backing query.
 * @param selectedValues The subset of values currently selected in the variable dropdown.
 * @param subquery       The variable's backing SQL (already trimmed of trailing `;`).
 * @param threshold      Maximum number of values the IN clause should ever literal-expand to.
 *
 * @returns
 *   - `null` if the optimization should be skipped (caller should expand as literal IN clause):
 *     * `selectedCount <= threshold` — the IN clause is small enough to expand directly.
 *     * Excluded set fits the threshold but the subquery's column can't be parsed (we'd silently
 *       broaden the filter to all values and drop the user's exclusions, which is incorrect).
 *   - The raw subquery if all values are selected (no extra filtering needed).
 *   - The raw subquery as a last-resort fallback when the excluded set itself exceeds the
 *     threshold — this DOES broaden the filter (matches all values, ignoring user's exclusions),
 *     but is preferable to a multi-MB IN clause that hangs the broker. See deferred work in PR #184.
 *   - A wrapped subquery `SELECT col FROM (subquery) WHERE col NOT IN (excluded...)` when the
 *     excluded set fits the threshold and the column name was extracted successfully.
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
    // Both the selected and excluded sets exceed the threshold — neither IN nor NOT IN can be
    // expressed in a small enough literal list. Return the raw subquery (matches all values,
    // dropping the user's exclusions) rather than emit a multi-MB IN clause that breaks the broker.
    return subquery;
  }

  // If the subquery pattern is too complex to parse (e.g. multi-column SELECT, JSON_EXTRACT_SCALAR,
  // aliased columns), we can't build a safe NOT IN wrap. Returning the raw subquery here would
  // silently match ALL values and drop the user's exclusions — incorrect — so return null instead
  // and let the caller fall back to the literal-expansion path.
  const column = extractColumnNameFromSubquery(subquery);
  if (!column) {
    return null;
  }

  const excludedLiterals = excludedValues.map((v) => `'${escapeSqlString(v)}'`).join(', ');
  const notInClause = `"${column}" NOT IN (${excludedLiterals})`;

  // Wrap the original subquery in a derived table so the NOT IN filter is applied at the outer level.
  // This is safe regardless of trailing clauses (LIMIT, ORDER BY, GROUP BY, HAVING) in the inner subquery —
  // appending WHERE/AND directly would produce invalid SQL like `... LIMIT 4000 WHERE ...`.
  return `SELECT "${column}" FROM (${subquery}) WHERE ${notInClause}`;
}
