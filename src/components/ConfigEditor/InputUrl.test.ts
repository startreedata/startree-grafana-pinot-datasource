import { CREDENTIALS_ERROR, urlError } from './InputUrl';

test.each([
  [undefined, ''],
  ['', ''],
  ['http://localhost:9000', ''],
  ['https://pinot.example.com:8443/path', ''],
  ['ftp://example.com', 'Please enter a valid URL'],
  ['not a url', 'Please enter a valid URL'],
])('urlError(%j)', (url, want) => {
  expect(urlError(url)).toBe(want);
});

test.each(['http://admin:secret@localhost:9000', 'https://admin@localhost:9000', 'http://:secret@localhost:9000'])(
  'urlError rejects credentials in %s',
  (url) => {
    expect(urlError(url)).toBe(CREDENTIALS_ERROR);
  }
);
