// test-env.js - Test if .env file is loading correctly
require('dotenv').config();

console.log('=== Testing .env Configuration ===\n');

const redisUrl = process.env.UPSTASH_REDIS_REST_URL;
const redisToken = process.env.UPSTASH_REDIS_REST_TOKEN;
const apiKey = process.env.EODHD_API_KEY;

console.log('1. UPSTASH_REDIS_REST_URL:');
if (redisUrl) {
  console.log(`   ✓ Loaded: ${redisUrl.substring(0, 30)}...`);
} else {
  console.log('   ✗ NOT FOUND - Check your .env file');
}

console.log('\n2. UPSTASH_REDIS_REST_TOKEN:');
if (redisToken) {
  console.log(`   ✓ Loaded: ${redisToken.substring(0, 20)}...`);
} else {
  console.log('   ✗ NOT FOUND - Check your .env file');
}

console.log('\n3. EODHD_API_KEY:');
if (apiKey) {
  console.log(`   ✓ Loaded: ${apiKey.substring(0, 15)}...`);
} else {
  console.log('   ✗ NOT FOUND - Check your .env file');
}

console.log('\n=== Result ===');
if (redisUrl && redisToken && apiKey) {
  console.log('✓ All environment variables loaded successfully!');
  console.log('You can now run: node process-earnings.js');
} else {
  console.log('✗ Some environment variables are missing.');
  console.log('Please check your .env file.');
}