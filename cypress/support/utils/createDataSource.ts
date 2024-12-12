export async function createDataSource(params: {
  controllerUrl: string;
  brokerUrl: string;
  databaseName: string;
  authType: string;
  authToken: string;
}): Promise<{ name: string; uid: string }> {
  const name = `__Pinot_Test_${Math.floor(Math.random() * 1e6).toString(36)}`;

  return fetch('http://localhost:3001/api/datasources', {
    method: 'POST',
    headers: { ContentType: 'application/json' },
    body: JSON.stringify({
      name: name,
      type: 'startree-pinot-datasource',
      typeName: 'Pinot',
      typeLogoUrl: 'public/plugins/startree-pinot-datasource/img/logo.svg',
      jsonData: {
        brokerUrl: params.brokerUrl,
        controllerUrl: params.controllerUrl,
        databaseName: params.databaseName,
        tokenType: params.authType,
      },
      secureJsonData: {
        authToken: params.authToken,
      },
      access: 'proxy',
    }),
  })
    .then((response) => {
      if (response.ok) {
        return response.json() as Record<string, any>;
      } else {
        throw new Error(response.statusText);
      }
    })
    .then((data) => ({
      name: name,
      uid: data.uid,
    }));
}
