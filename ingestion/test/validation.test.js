import test from 'node:test';
import assert from 'node:assert/strict';
import { validate } from '../src/validation.js';

const valid = { cardId: 'tok-0001', amount: 25, country: 'IN', lat: 12.97, lon: 77.59, eventAt: 1000 };
test('accepts synthetic tokens and valid payment envelope', () => assert.equal(validate(valid), null));
test('rejects raw card numbers', () => assert.ok(validate({ ...valid, cardId: '4111111111111111' })));
test('rejects negative amounts and impossible locations', () => {
  assert.ok(validate({ ...valid, amount: -1 }));
  assert.ok(validate({ ...valid, lat: 100 }));
});
