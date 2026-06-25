import { DisplayType } from './dataquery/DisplayType';
import { EditorMode } from './dataquery/EditorMode';
import { QueryType } from './dataquery/QueryType';
import { PinotDataQuery } from './dataquery/PinotDataQuery';

// Prefix for refIds of derived logs queries so they don't collide with the user's panel query.
export const LOGS_VOLUME_REF_ID_PREFIX = 'log-volume-';

// A logs builder query is runnable (and derivable) once it has a table and a time column.
export function isLogsBuilderQuery(query: PinotDataQuery): boolean {
  return (
    !query.hide &&
    query.queryType === QueryType.PinotQL &&
    query.editorMode === EditorMode.Builder &&
    query.displayType === DisplayType.LOGS &&
    !!query.tableName &&
    !!query.timeColumn
  );
}

// logsVolumeQuery derives the count(*)-over-time supplementary query from a logs builder query.
// The `logsVolume` flag routes the backend to LogsBuilderQuery.VolumeQuery(); the level column
// (if any) carries over so the volume is broken down by level. Returns undefined when the query
// isn't a runnable logs builder query.
export function logsVolumeQuery(query: PinotDataQuery): PinotDataQuery | undefined {
  if (!isLogsBuilderQuery(query)) {
    return undefined;
  }
  return {
    ...query,
    refId: `${LOGS_VOLUME_REF_ID_PREFIX}${query.refId}`,
    logsVolume: true,
  };
}
