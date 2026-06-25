import { searchFilterLikeValue } from './variables';

describe('searchFilterLikeValue', () => {
  test('defaults to match-all when nothing is typed', () => {
    expect(searchFilterLikeValue('')).toBe('%');
  });

  test('appends a trailing wildcard for prefix matching', () => {
    expect(searchFilterLikeValue('check')).toBe('check%');
  });

  test('escapes single quotes to keep the SQL literal valid', () => {
    expect(searchFilterLikeValue("o'brien")).toBe("o''brien%");
  });
});
