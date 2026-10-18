export function validate(input) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return 'Expected a JSON object';
  if (typeof input.cardId !== 'string' || !/^tok-[A-Za-z0-9-]{1,28}$/.test(input.cardId)) return 'Invalid synthetic card token';
  if (!Number.isFinite(input.amount) || input.amount <= 0 || input.amount > 100000) return 'Invalid amount';
  if (typeof input.country !== 'string' || !/^[A-Z]{2}$/.test(input.country)) return 'Invalid country';
  if (!Number.isFinite(input.lat) || input.lat < -90 || input.lat > 90) return 'Invalid latitude';
  if (!Number.isFinite(input.lon) || input.lon < -180 || input.lon > 180) return 'Invalid longitude';
  if (!Number.isSafeInteger(input.eventAt) || input.eventAt < 0) return 'Invalid event timestamp';
  return null;
}
