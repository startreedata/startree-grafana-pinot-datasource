import { DataSource } from '../datasource';
import { PinotResourceResponse } from './PinotResourceResponse';
import { useEffect, useState } from 'react';

export function useIsPromQlSupported(datasource: DataSource): [boolean, boolean] {
  const [supported, setSupported] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(true);
  useEffect(() => {
    type IsPromQlSupportedResponse = PinotResourceResponse<boolean>;

    datasource
      .getResource<IsPromQlSupportedResponse>('isPromQlSupported')
      .then((resp) => setSupported(resp.result || false))
      .finally(() => setLoading(false));
  }, [datasource]);
  return [supported, loading];
}
