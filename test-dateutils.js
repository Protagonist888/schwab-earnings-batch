// test-dateutils.js - Quick test to verify DateUtils works
const { DateUtils } = require('./date_utils');

console.log('Testing DateUtils...');
console.log('1. getTodayNormalized:', DateUtils.getTodayNormalized());
console.log('2. parseApiDate("2025-11-15"):', DateUtils.parseApiDate("2025-11-15"));
console.log('3. daysUntil("2025-11-15"):', DateUtils.daysUntil("2025-11-15"));
console.log('4. isPastDate("2025-11-01"):', DateUtils.isPastDate("2025-11-01"));
console.log('5. getDateRange(730, 365):', DateUtils.getDateRange(730, 365));

console.log('\n✓ DateUtils loaded successfully! You can now use it in process-earnings.js');