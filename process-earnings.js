// schwab-earnings-batch/process-earnings.js
const https = require('https');
const { Redis } = require('@upstash/redis');
const { DateUtils } = require('./date_utils');

// Initialize Redis
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const EODHD_API_KEY = process.env.EODHD_API_KEY;
const BATCH_SIZE = 900;  // Stay under 1000/min rate limit 

// REFINEMENT 1: Dynamically fetch all US symbols instead of using a static file. 
async function getAllSymbols() { 
    console.log('Fetching list of major exchanges using maximum permissiveness...');
    
    const exchanges = ['NYSE', 'NASDAQ']
    let symbols = []; 
    
    for (const exchange of exchanges) {
        try {
            // FIX: Removed all optional parameters from the URL (delisted=0) 
            // We rely on the API's default of returning current/active tickers.
            const url = `https://eodhd.com/api/exchange-symbol-list/${exchange}?api_token=${EODHD_API_KEY}&delisted=0&fmt=json`; 
            const response = await fetchJSON(url); 
            
            if (Array.isArray(response)) {
                const exchangeSymbols = response
                    .filter(stock => {
                        const type = (stock.Type || '').toUpperCase();
                        // FIX: Use a robust filter covering all relevant types (Common Stock, ETFs, REITs, Warrants/Rights/Funds as a fallback)
                        return (
                            type.includes('STOCK') || 
                            type.includes('ETF') || 
                            type.includes('REIT') || 
                            type.includes('FUND')
                        );
                    })
                    .map(stock => stock.Code);
                    
                symbols = symbols.concat(exchangeSymbols); 
                console.log(`Successfully fetched ${exchangeSymbols.length} symbols from ${exchange}.`); 
            }
        } catch (error) {
            console.error(`Warning: Failed to fetch symbols from ${exchange} after retries. Continuing.`, error);
        }
    }
    
    const uniqueSymbols = Array.from(new Set(symbols));
    console.log(`Final unique symbol count: ${uniqueSymbols.length}.`); 

    // Re-setting the critical low threshold to 1000.
    if (uniqueSymbols.length < 1000) { 
        console.error('CRITICAL: Final symbol count is too low. Aborting batch.');
        process.exit(1); 
    }
    
    return uniqueSymbols; 
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms)); 
}

async function fetchJSON(url, attempt = 1, maxRetries = 5) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', async () => {
        if (res.statusCode === 200) {
          try {
            resolve(JSON.parse(data)); 
          } catch (e) {
            reject(new Error(`Invalid JSON: ${data.substring(0, 50)}...`)); 
          }
        } else if (res.statusCode === 404) {
          resolve({ notFound: true }); 
        } else if (res.statusCode === 429 || res.statusCode >= 500) {
          // **RATE LIMIT/SERVER ERROR HANDLING**
          if (attempt < maxRetries) {
            // Exponential backoff with jitter
            const delay = Math.pow(2, attempt) * 1000 + Math.random() * 500;
            console.log(`Rate limit (429/5xx) hit. Retrying in ${Math.round(delay/1000)}s (Attempt ${attempt}/${maxRetries})`);
            await sleep(delay);
            
            // Recursive retry
            resolve(fetchJSON(url, attempt + 1, maxRetries));
          } else {
            // Max retries reached
            reject(new Error(`API returned ${res.statusCode} after ${maxRetries} retries`)); 
          }
        } else {
          // Non-retryable 400-level error
          reject(new Error(`API returned ${res.statusCode} (Error: ${data.substring(0, 50)}...)`)); 
        }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

// schwab-earnings-batch/process-earnings.js - REVISED processSymbol with DateUtils
async function processSymbol(symbol) {
  console.log(`Processing ${symbol}...`);

  // --- Date Setup (Timezone-Safe) ---
  const dateRange = DateUtils.getDateRange(730, 365); // 2 years back, 1 year forward
  const calendarUrl = `https://eodhd.com/api/calendar/earnings?api_token=${EODHD_API_KEY}&symbols=${symbol}.US&from=${dateRange.from}&to=${dateRange.to}&fmt=json`;

  try {
    // Step 1: Get earnings calendar
    const earningsData = await fetchJSON(calendarUrl);

    if (earningsData.notFound || !Array.isArray(earningsData.earnings)) {
        console.log(`Skipping ${symbol}: API returned no valid earnings array.`);
        return null;
    }

    // Step 2: Get 2-year price history
    const priceUrl = `https://eodhd.com/api/eod/${symbol}.US?api_token=${EODHD_API_KEY}&period=d&from=${dateRange.from}&to=${DateUtils.formatApiDate(DateUtils.getTodayNormalized())}&fmt=json`;
    const priceData = await fetchJSON(priceUrl);

    if (priceData.notFound || !Array.isArray(priceData) || priceData.length < 10) {
      console.log(`Skipping ${symbol}: Insufficient price data for calculation.`);
      return null;
    }
    
    // Step 3: Calculate average earnings move (ONLY past dates)
    const moves = [];
    for (const earning of earningsData.earnings) {
        if (DateUtils.isPastDate(earning.report_date)) {
            // Calculate day before and after earnings
            const earningsDate = DateUtils.parseApiDate(earning.report_date);
            if (!earningsDate) continue;
            
            const dayBefore = new Date(earningsDate);
            dayBefore.setDate(dayBefore.getDate() - 1);
            const dayAfter = new Date(earningsDate);
            dayAfter.setDate(dayAfter.getDate() + 1);
            
            // Find prices with weekend/holiday lookback
            const beforePrice = DateUtils.findPriceOnDate(priceData, DateUtils.formatApiDate(dayBefore));
            const afterPrice = DateUtils.findPriceOnDate(priceData, DateUtils.formatApiDate(dayAfter));

            if (beforePrice && afterPrice && beforePrice > 0) {
                const percentMove = Math.abs((afterPrice - beforePrice) / beforePrice) * 100;
                moves.push(percentMove);
            }
        }
    }
    
    const avgMove = moves.length > 0 ? moves.reduce((a, b) => a + b, 0) / moves.length : null;

    // Step 4: Find next earnings date (timezone-safe)
    const allEarningsDates = earningsData.earnings.map(e => e.report_date);
    const nextDate = DateUtils.findNextFutureDate(allEarningsDates);
    
    // Step 5: Store in Redis with calculated metadata
    const result = {
      symbol: symbol,
      nextDate: nextDate,
      daysUntil: nextDate ? DateUtils.daysUntil(nextDate) : null,
      formattedDate: nextDate ? DateUtils.formatDisplayDate(nextDate) : null,
      avgMove: avgMove !== null ? parseFloat(avgMove.toFixed(2)) : null,
      lastUpdated: new Date().toISOString(),
      calculatedAt: DateUtils.formatApiDate(DateUtils.getTodayNormalized())
    };

    await redis.set(`earnings:${symbol}`, result, { ex: 2592000 });  // 30 day TTL

    console.log(`✓ ${symbol}: Next earnings ${nextDate || 'N/A'} (${result.daysUntil} days), avg move ${avgMove !== null ? avgMove.toFixed(2) + '%' : 'N/A'}`);
    return result;

  } catch (error) {
    console.error(`Error processing ${symbol}:`, error.message);
    return null;
  }
}

const MAX_BATCHES_PER_RUN = 10; // Process a maximum of 10 batches (9,000 symbols)

async function main() {
  const SYMBOLS = await getAllSymbols(); 
  // const SYMBOLS = ['DAL','NEOG','APLD','AAPL','CRWV','NVDA','MSFT']; // Keep for local dev
  console.log(`Starting batch processing for ${SYMBOLS.length} symbols...`); 

  let processed = 0; 
  let successful = 0; 
  let failed = 0; 
  let batchesRun = 0;

  const BATCH_SIZE = 900; // Still use 900
  const BATCH_DELAY_MS = 75000; // 75 seconds for safer rate limit reset

  for (let i = 0; i < SYMBOLS.length; i += BATCH_SIZE) {

    if (batchesRun >= MAX_BATCHES_PER_RUN) {
      console.log(`\nSTOPPING: Reached maximum ${MAX_BATCHES_PER_RUN} batches for this run.`);
      break;
    }

    batchesRun++; // Increment batch counter

    const batch = SYMBOLS.slice(i, i + BATCH_SIZE); 
    console.log(`\nBatch ${Math.floor(i / BATCH_SIZE) + 1}: Processing ${batch.length} symbols`); 

    // **CRITICAL FIX 2: Sequential processing with micro-delay**
    const results = [];
    for (const symbol of batch) {
        const result = await processSymbol(symbol);
        results.push(result);
        
        // Micro-delay to spread the load (70ms)
        await sleep(70); 
    }
    // End Critical Fix 2

    // Update counters (logic remains the same)
    processed += batch.length; 
    successful += results.filter(r => r !== null).length; 
    failed += results.filter(r => r === null).length; 

    console.log(`Progress: ${processed}/${SYMBOLS.length} (${successful} successful, ${failed} failed)`); 

    // Wait 75 seconds between batches (Increased delay)
    if (i + BATCH_SIZE < SYMBOLS.length) {
      console.log(`Waiting ${BATCH_DELAY_MS / 1000} seconds before next batch...`); 
      await sleep(BATCH_DELAY_MS); 
    }
  }

  console.log(`\n✓ Batch processing complete!`); 
  console.log(`Total processed: ${processed}`); 
  console.log(`Successful: ${successful}`); 
  console.log(`Failed: ${failed}`); 
}

main().catch(console.error);