import { DataSource } from '../datasource';
import { PinotResourceResponse } from './PinotResourceResponse';
import { useEffect, useState } from 'react';

interface IsPromQlSupportedResponse extends PinotResourceResponse {
  isPromQlSupported: boolean | null;
}

export function useIsPromQlSupported(datasource: DataSource): [boolean, boolean] {
  const [supported, setSupported] = useState<boolean>(false);
  const [loading, setLoading] = useState<boolean>(true);
  useEffect(() => {
    datasource
      .getResource<IsPromQlSupportedResponse>('isPromQlSupported')
      .then((resp) => setSupported(resp.isPromQlSupported || false))
      .finally(() => setLoading(false));
  }, [datasource]);
  return [supported, loading];
}
