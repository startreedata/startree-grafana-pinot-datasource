import { useEffect, useRef } from 'react';
import { DimensionFilter } from '../../dataquery/DimensionFilter';
import { QueryOption } from '../../dataquery/QueryOption';

interface ParamsWithFiltersAndQueryOptions {
  filters: DimensionFilter[];
  queryOptions: QueryOption[];
}

/**
 * Auto-surfaces `useMultistageEngine=true` in the Query Options UI when a filter variable was
 * replaced with a subquery (>1000 values exceeds the IN threshold). Subqueries require MSE in
 * Pinot, and surfacing the option makes the dependency explicit to the user. The internal ref
 * tracks whether *we* added the option, so we know to remove it again when the subquery disappears
 * — without ever touching an option the user set themselves.
 *
 * Three branches:
 *   1. Auto-add: subquery filter present, user hasn't set MSE explicitly, we haven't auto-added → add it.
 *   2. Auto-remove: subquery gone and we previously auto-added → strip our addition.
 *   3. Reset ref: no subquery, no auto-added marker → keep state clean for a future add cycle.
 */
export function useAutoSurfaceMultiStageEngine<T extends ParamsWithFiltersAndQueryOptions>(
  savedParams: T,
  interpolatedParams: T,
  onChangeAndRun: (newParams: T) => void
) {
  const queryHasAutoInjectedMultiStageEngineQueryOption = useRef(false);
  const hasInterpolatedSubqueryFilter = interpolatedParams.filters.some((filter) => Boolean(filter.subqueryExpr));
  useEffect(() => {
    const hasUserAddedMultiStageQueryEngineOption = savedParams.queryOptions.some(
      (queryOption) => queryOption.name?.toLowerCase() === 'usemultistageengine'
    );
    if (
      hasInterpolatedSubqueryFilter &&
      !hasUserAddedMultiStageQueryEngineOption &&
      !queryHasAutoInjectedMultiStageEngineQueryOption.current
    ) {
      queryHasAutoInjectedMultiStageEngineQueryOption.current = true;
      onChangeAndRun({
        ...savedParams,
        queryOptions: [...savedParams.queryOptions, { name: 'useMultistageEngine', value: 'true' }],
      });
    } else if (!hasInterpolatedSubqueryFilter && queryHasAutoInjectedMultiStageEngineQueryOption.current) {
      queryHasAutoInjectedMultiStageEngineQueryOption.current = false;
      onChangeAndRun({
        ...savedParams,
        queryOptions: savedParams.queryOptions.filter(
          (queryOption) => queryOption.name?.toLowerCase() !== 'usemultistageengine'
        ),
      });
    } else if (!hasInterpolatedSubqueryFilter) {
      queryHasAutoInjectedMultiStageEngineQueryOption.current = false;
    }
  }, [JSON.stringify(interpolatedParams.filters)]); // eslint-disable-line react-hooks/exhaustive-deps
}
