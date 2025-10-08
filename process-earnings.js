// schwab-earnings-batch/process-earnings.js
const https = require('https');
const { Redis } = require('@upstash/redis');

// Initialize Redis
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const EODHD_API_KEY = process.env.EODHD_API_KEY;
const BATCH_SIZE = 900;  // Stay under 1000/min rate limit [cite: 261]

// REFINEMENT 1: Dynamically fetch all US symbols instead of using a static file. [cite: 262]
async function getAllSymbols() { 
  console.log('Fetching latest list of US exchange symbols...');
  try {
    // This example fetches from the general 'US' exchange. [cite: 266]
    const url = `https://eodhd.com/api/exchange-symbol-list/US?api_token=${EODHD_API_KEY}&fmt=json`; 
    const exchanges = await fetchJSON(url); 
    // The result is an array of objects like {Code: "AAPL", Name: "Apple Inc"}. We just need the "Code". [cite: 269]
    const symbols = exchanges.map(stock => stock.Code); 
    console.log(`Successfully fetched ${symbols.length} symbols.`); 
    return symbols;
  } catch (error) {
    console.error('CRITICAL: Failed to fetch symbol list. Aborting batch.', error);
    // Exit the process with an error code so GitHub Actions marks it as a failure. [cite: 275]
    process.exit(1); 
  }
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms)); 
}

async function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (res.statusCode === 200) {
          try {
            resolve(JSON.parse(data)); 
          } catch (e) {
            reject(new Error('Invalid JSON')); 
          }
        } else if (res.statusCode === 404) {
          resolve({ notFound: true }); 
        } else {
          reject(new Error(`API returned ${res.statusCode}`)); 
        }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function processSymbol(symbol) {
  console.log(`Processing ${symbol}...`); 

  try {
    // Step 1: Get earnings calendar (past 2 years, future 1 year) [cite: 306]
    const today = new Date(); 
    const twoYearsAgo = new Date(); 
    twoYearsAgo.setFullYear(today.getFullYear() - 2); 
    // REFINEMENT 3: Look one year into the future to ensure we capture the next earnings date. [cite: 310]
    const oneYearFuture = new Date(); 
    oneYearFuture.setFullYear(today.getFullYear() + 1); 
    const formatDate = (date) => {
      return date.toISOString().split('T')[0]; 
    };
    const calendarUrl = `https://eodhd.com/api/calendar/earnings?api_token=${EODHD_API_KEY}&symbols=${symbol}.US&from=${formatDate(twoYearsAgo)}&to=${formatDate(oneYearFuture)}&fmt=json`;

    const earningsData = await fetchJSON(calendarUrl); 

    if (earningsData.notFound || !earningsData.earnings || earningsData.earnings.length === 0) {
      console.log(`No earnings data for ${symbol}`); 
      return null;
    }

    // Step 2: Get 2-year price history [cite: 322]
    const priceUrl = `https://eodhd.com/api/eod/${symbol}.US?api_token=${EODHD_API_KEY}&period=d&from=${formatDate(twoYearsAgo)}&to=${formatDate(today)}&fmt=json`; 
    const priceData = await fetchJSON(priceUrl); 

    if (priceData.notFound || !Array.isArray(priceData) || priceData.length < 10) {
      console.log(`Insufficient price data for ${symbol}`); 
      return null;
    }

    // Step 3: Calculate average earnings move [cite: 329]
    const moves = [];
    for (const earning of earningsData.earnings) {
      const earningsDate = new Date(earning.date);

      // Find prices around earnings date [cite: 333]
      const beforePrice = findPriceOnDate(priceData, new Date(earningsDate.getTime() - 86400000)); 
      const afterPrice = findPriceOnDate(priceData, new Date(earningsDate.getTime() + 86400000)); 

      if (beforePrice && afterPrice && beforePrice > 0) {
        const percentMove = Math.abs((afterPrice - beforePrice) / beforePrice) * 100; 
        moves.push(percentMove); 
      }
    }

    if (moves.length === 0) {
      console.log(`No valid earnings moves for ${symbol}`); 
      return null;
    }

    const avgMove = moves.reduce((a, b) => a + b, 0) / moves.length;

    // Step 4: Find next earnings date [cite: 346]
    const futureEarnings = earningsData.earnings.filter(e => new Date(e.date) > today); 
    const nextDate = futureEarnings.length > 0 ? futureEarnings[0].date : null; 

    if (!nextDate) {
      console.log(`No future earnings for ${symbol}`);
      return null;
    }

    // Step 5: Store in Redis [cite: 353]
    const result = {
      symbol: symbol,
      nextDate: nextDate,
      avgMove: parseFloat(avgMove.toFixed(2)),
      lastUpdated: new Date().toISOString()
    }; 

    await redis.set(`earnings:${symbol}`, result, { ex: 2592000 });  // 30 day TTL [cite: 360]

    console.log(`✓ ${symbol}: Next earnings ${nextDate}, avg move ${avgMove.toFixed(2)}%`); 
    return result;

  } catch (error) {
    console.error(`Error processing ${symbol}:`, error.message); 
    return null;
  }
}

// REFINEMENT 2: Make date searching more robust to handle weekends and holidays. [cite: 368]
function findPriceOnDate(priceData, targetDate) {
  // Search for up to 5 days backward to find a valid trading day. [cite: 369]
  for (let i = 0; i < 5; i++) {
    const dateToTry = new Date(targetDate.getTime() - (i * 86400000)); 
    const dateString = dateToTry.toISOString().split('T')[0]; 
    const match = priceData.find(d => d.date === dateString); 
    if (match) { 
      return parseFloat(match.close); 
    }
  }
  return null; // Return null if no price is found within 5 days. [cite: 377]
}

async function main() {
  const SYMBOLS = await getAllSymbols(); // Use the dynamic list [cite: 380]
  // const SYMBOLS = ['DAL','NEOG','APLD']; // For testing, use a small static list
  console.log(`Starting batch processing for ${SYMBOLS.length} symbols...`); 

  let processed = 0; 
  let successful = 0; 
  let failed = 0; 

  // Process in batches to respect rate limits [cite: 385]
  for (let i = 0; i < SYMBOLS.length; i += BATCH_SIZE) {
    const batch = SYMBOLS.slice(i, i + BATCH_SIZE); 
    console.log(`\nBatch ${Math.floor(i / BATCH_SIZE) + 1}: Processing ${batch.length} symbols`); 

    const results = await Promise.all(batch.map(symbol => processSymbol(symbol))); 

    processed += batch.length; 
    successful += results.filter(r => r !== null).length; 
    failed += results.filter(r => r === null).length; 

    console.log(`Progress: ${processed}/${SYMBOLS.length} (${successful} successful, ${failed} failed)`); 

    // Wait 60 seconds between batches to reset rate limit [cite: 394]
    if (i + BATCH_SIZE < SYMBOLS.length) {
      console.log('Waiting 60 seconds before next batch...'); 
      await sleep(60000); 
    }
  }

  console.log(`\n✓ Batch processing complete!`); 
  console.log(`Total processed: ${processed}`); 
  console.log(`Successful: ${successful}`); 
  console.log(`Failed: ${failed}`); 
}

main().catch(console.error);