// verify-redis.js - Check if earnings data is in Redis
require('dotenv').config();
const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

async function checkRedis() {
  console.log('=== Checking Redis Data ===\n');
  
  const testSymbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'GOOGL'];
  
  for (const symbol of testSymbols) {
    try {
      const data = await redis.get(`earnings:${symbol}`);
      
      if (data) {
        console.log(`✓ ${symbol}:`);
        console.log(`  Next earnings: ${data.nextDate || 'N/A'}`);
        console.log(`  Days until: ${data.daysUntil !== null ? data.daysUntil : 'N/A'}`);
        console.log(`  Formatted date: ${data.formattedDate || 'N/A'}`);
        console.log(`  Avg move: ${data.avgMove !== null ? data.avgMove + '%' : 'N/A'}`);
        console.log(`  Last updated: ${data.lastUpdated}`);
        console.log(`  Calculated at: ${data.calculatedAt}\n`);
      } else {
        console.log(`✗ ${symbol}: No data found\n`);
      }
    } catch (error) {
      console.error(`✗ ${symbol}: Error - ${error.message}\n`);
    }
  }
  
  console.log('=== Redis Check Complete ===');
}

checkRedis().catch(console.error);