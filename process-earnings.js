// schwab-earnings-batch/process-earnings.js
const https = require('https');
const { Redis } = require('@upstash/redis');

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

// schwab-earnings-batch/process-earnings.js - REVISED processSymbol
async function processSymbol(symbol) {
  console.log(`Processing ${symbol}...`);

  // --- Date Setup ---
  const today = new Date();
  const isPastDate = (dateString) => new Date(dateString) <= today; 
  const twoYearsAgo = new Date();
  twoYearsAgo.setFullYear(today.getFullYear() - 2);
  const oneYearFuture = new Date();
  oneYearFuture.setFullYear(today.getFullYear() + 1);
  const formatDate = (date) => date.toISOString().split('T')[0];

  // **CRITICAL FIX 1: Ensure calendarUrl is defined BEFORE its use.**
  const calendarUrl = `https://eodhd.com/api/calendar/earnings?api_token=${EODHD_API_KEY}&symbols=${symbol}.US&from=${formatDate(twoYearsAgo)}&to=${formatDate(oneYearFuture)}&fmt=json`;

  try {
    // Step 1: Get earnings calendar
    const earningsData = await fetchJSON(calendarUrl);

    // CRITICAL INTEGRITY CHECK: Abort if API failed or returned a malformed response
    if (earningsData.notFound || !Array.isArray(earningsData.earnings)) {
        console.log(`Skipping ${symbol}: API returned no valid earnings array.`);
        return null;
    }

    // Step 2: Get 2-year price history (Calculation dependency check)
    const priceUrl = `https://eodhd.com/api/eod/${symbol}.US?api_token=${EODHD_API_KEY}&period=d&from=${formatDate(twoYearsAgo)}&to=${formatDate(today)}&fmt=json`; 
    const priceData = await fetchJSON(priceUrl);

    if (priceData.notFound || !Array.isArray(priceData) || priceData.length < 10) {
      console.log(`Skipping ${symbol}: Insufficient price data for calculation.`);
      return null;
    }
    
    // --- NON-AGGRESSIVE CACHING & CALCULATION STARTS HERE ---

    // Step 3: Calculate average earnings move
    const moves = [];
    for (const earning of earningsData.earnings) {
        // Only use valid PAST earnings dates for historical average calculation
        const earningsDate = new Date(earning.report_date);
        if (isPastDate(earning.report_date)) { 
            // Find prices around earnings date (1 day before/after)
            const beforePrice = findPriceOnDate(priceData, new Date(earningsDate.getTime() - 86400000)); 
            const afterPrice = findPriceOnDate(priceData, new Date(earningsDate.getTime() + 86400000)); 

            if (beforePrice && afterPrice && beforePrice > 0) {
                const percentMove = Math.abs((afterPrice - beforePrice) / beforePrice) * 100; 
                moves.push(percentMove); 
            }
        }
    }
    
    // Set avgMove to null if no valid moves found (CRITICAL: prevents filtering)
    const avgMove = moves.length > 0 ? moves.reduce((a, b) => a + b, 0) / moves.length : null;

    // Step 4: Find next earnings date - FIX DATE LOGIC
    // Find ALL future dates, then take the earliest one.
    const futureEarnings = earningsData.earnings
        .filter(e => new Date(e.report_date) > today) // Filter out all past dates (fixes ZS issue)
        .sort((a, b) => new Date(a.report_date) - new Date(b.report_date)); // Sort by date ascending

    // Set nextDate to null if no future earnings found (CRITICAL: prevents filtering)
    const nextDate = futureEarnings.length > 0 ? futureEarnings[0].report_date : null; 
    
    // Step 5: Store in Redis - Always store if integrity checks passed (Steps 1 & 2)
    const result = {
      symbol: symbol,
      nextDate: nextDate, // Can be null
      avgMove: avgMove !== null ? parseFloat(avgMove.toFixed(2)) : null, // Can be null
      lastUpdated: new Date().toISOString()
    }; 

    await redis.set(`earnings:${symbol}`, result, { ex: 2592000 });  // 30 day TTL 

    console.log(`✓ ${symbol}: Next earnings ${nextDate || 'N/A'}, avg move ${avgMove !== null ? avgMove.toFixed(2) + '%' : 'N/A'}`); 
    return result;

  } catch (error) {
    // If the entire process fails due to a network or unexpected error, return null.
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
      console.log('\nSTOPPING: Reached maximum ${MAX_BATCHES_PER_RUN} batches for this run.');
      break;
    }

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