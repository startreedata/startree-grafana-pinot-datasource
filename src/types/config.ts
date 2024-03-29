import { DataSourceJsonData } from '@grafana/data';

/**
 * These are options configured for each DataSource instance
 */
export interface PinotConnectionConfig extends DataSourceJsonData {
    version: string;

    controllerUrl?: string;
    brokerUrl?: string;

    username?: string;

    dialTimeout?: string;
    queryTimeout?: string;

    httpHeaders?: PinotHttpHeader[];
    forwardGrafanaHeaders?: boolean;

    customSettings?: PinotCustomSetting[];
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface PinotSecureConfig {
    authToken?: string;
    password?: string;
}


export interface PinotHttpHeader {
    name: string;
    value: string;
}

export interface PinotCustomSetting {
    setting: string;
    value: string;
}
