// The Daily Nuthatch - STABLE Minimal Version
// Simplified to prevent crashes and restarts

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const cron = require('node-cron');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Configuration
const CONFIG = {
  NEWSAPI_KEY: process.env.NEWSAPI_KEY || '',
  FRED_API_KEY: process.env.FRED_API_KEY || '',
  ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY || '',
  PORT: process.env.PORT || 3000
};

// Track seen articles
const seenArticles = new Set();

// WebSocket clients
const clients = new Set();

// Serve static files
app.use(express.static('public'));
app.use(express.json());

// Manual input endpoint
app.post('/api/manual-input', (req, res) => {
  try {
    const { headline, category, source } = req.body;
    
    const cardData = {
      type: 'new_card',
      column: category || 'breaking',
      data: {
        time: new Date().toISOString().substr(11, 5),
        headline: headline || 'No headline',
        source: source || 'Manual Input',
        verified: false,
        implications: ['User-submitted item'],
        impact: 2,
        horizon: 'DAYS',
        tripwires: [],
        probNudge: []
      }
    };

    broadcast(cardData);
    res.json({ success: true });
  } catch (error) {
    console.error('Manual input error:', error.message);
    res.status(500).json({ error: error.message });
  }
});

// WebSocket handlers
wss.on('connection', (ws) => {
  console.log('✅ Client connected');
  clients.add(ws);

  ws.on('close', () => {
    console.log('❌ Client disconnected');
    clients.delete(ws);
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error.message);
  });

  // Send initial data
  sendInitialData(ws);
});

function broadcast(data) {
  const message = JSON.stringify(data);
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      try {
        client.send(message);
      } catch (error) {
        console.error('Broadcast error:', error.message);
      }
    }
  });
}

async function sendInitialData(ws) {
  try {
    const marketData = await fetchMarketData();
    const macroData = await fetchMacroData();
    
    ws.send(JSON.stringify({
      type: 'initial',
      market: marketData,
      macro: macroData
    }));
  } catch (error) {
    console.error('Initial data error:', error.message);
  }
}

// ============================================================================
// NEWS API
// ============================================================================

async function pollNewsAPI() {
  if (!CONFIG.NEWSAPI_KEY) {
    console.log('⚠️  NewsAPI key not configured');
    return;
  }

  try {
    const response = await axios.get('https://newsapi.org/v2/top-headlines', {
      params: {
        category: 'business',
        language: 'en',
        pageSize: 10,
        apiKey: CONFIG.NEWSAPI_KEY
      },
      timeout: 10000
    });

    if (response.data.articles) {
      for (const article of response.data.articles.slice(0, 5)) {
        const articleId = article.url;
        
        if (!seenArticles.has(articleId)) {
          seenArticles.add(articleId);
          
          // Limit cache
          if (seenArticles.size > 1000) {
            seenArticles.clear();
          }
          
          await processNewsArticle(article);
        }
      }
    }
  } catch (error) {
    if (error.response?.status !== 429) {
      console.error('NewsAPI error:', error.message);
    }
  }
}

async function processNewsArticle(article) {
  try {
    const category = classifyNews(article.title + ' ' + (article.description || ''));
    
    const cardData = {
      type: 'new_card',
      column: category,
      data: {
        time: new Date().toISOString().substr(11, 5),
        headline: article.title,
        source: article.source.name,
        verified: true,
        implications: [
          'Monitoring for market reaction',
          'Watching for follow-up developments'
        ],
        impact: 2,
        horizon: 'DAYS',
        tripwires: [],
        probNudge: []
      }
    };

    broadcast(cardData);
  } catch (error) {
    console.error('Process article error:', error.message);
  }
}

// ============================================================================
// CLASSIFICATION
// ============================================================================

function classifyNews(text) {
  if (!text) return 'breaking';
  
  const lower = text.toLowerCase();
  
  const keywords = {
    macro: ['fed', 'federal reserve', 'ecb', 'central bank', 'interest rate', 
            'rates', 'yield', 'inflation', 'cpi', 'jobs', 'employment'],
    geo: ['russia', 'ukraine', 'china', 'taiwan', 'iran', 'military', 
          'sanctions', 'war', 'conflict', 'nato'],
    commodity: ['oil', 'crude', 'brent', 'wti', 'gold', 'silver', 'copper', 
                'wheat', 'energy', 'natgas', 'metals'],
    market: ['stock', 'equity', 'nasdaq', 'dow', 'rally', 'selloff']
  };

  let maxScore = 0;
  let maxCategory = 'breaking';
  
  for (let [category, words] of Object.entries(keywords)) {
    const score = words.filter(w => lower.includes(w)).length;
    if (score > maxScore) {
      maxScore = score;
      maxCategory = category;
    }
  }

  return maxCategory;
}

// ============================================================================
// MARKET DATA
// ============================================================================

async function fetchMarketData() {
  const symbols = [
    'DX-Y.NYB',      // Dollar Index
    '^TNX',          // US 10Y
    '^TYX',          // US 30Y
    '^FVX',          // US 5Y
    '^IRX',          // US 3M
    'GC=F',          // Gold
    'SI=F',          // Silver
    'PL=F',          // Platinum
    'CL=F',          // WTI
    'BZ=F',          // Brent
    'NG=F',          // NatGas
    'HG=F',          // Copper
    '^GSPC',         // S&P 500
    '^IXIC',         // NASDAQ
    '^DJI',          // Dow
    'EURUSD=X',      // EUR/USD
    'GBPUSD=X',      // GBP/USD
    'USDJPY=X',      // USD/JPY
    '^VIX'           // VIX
  ];

  const marketData = [];

  for (const symbol of symbols) {
    try {
      const response = await axios.get(
        `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}`,
        {
          params: { interval: '1m', range: '1d' },
          timeout: 5000
        }
      );

      const result = response.data.chart.result[0];
      const quote = result.meta;
      const current = quote.regularMarketPrice;
      const previous = quote.previousClose;
      const change = current - previous;
      const changePercent = (change / previous) * 100;

      marketData.push({
        symbol: symbol,
        label: formatSymbolLabel(symbol),
        value: formatValue(symbol, current),
        change: formatChange(symbol, change, changePercent),
        dir: change > 0 ? 'up' : change < 0 ? 'down' : 'neutral'
      });
    } catch (error) {
      // Skip failed symbols silently
    }
  }

  return marketData;
}

function formatSymbolLabel(symbol) {
  const labels = {
    'DX-Y.NYB': 'DXY',
    '^TNX': 'US 10Y',
    '^TYX': 'US 30Y',
    '^FVX': 'US 5Y',
    '^IRX': 'US 3M',
    'GC=F': 'GOLD',
    'SI=F': 'SILVER',
    'PL=F': 'PLATINUM',
    'CL=F': 'WTI',
    'BZ=F': 'BRENT',
    'NG=F': 'NATGAS',
    'HG=F': 'COPPER',
    '^GSPC': 'S&P 500',
    '^IXIC': 'NASDAQ',
    '^DJI': 'DOW',
    'EURUSD=X': 'EURUSD',
    'GBPUSD=X': 'GBPUSD',
    'USDJPY=X': 'USDJPY',
    '^VIX': 'VIX'
  };
  return labels[symbol] || symbol;
}

function formatValue(symbol, value) {
  if (symbol.includes('USD=X')) {
    return value.toFixed(4);
  } else if (symbol.startsWith('^T')) {
    return value.toFixed(3) + '%';
  } else if (symbol.includes('=F') && !symbol.includes('VIX')) {
    return '$' + value.toFixed(2);
  } else {
    return value.toFixed(2);
  }
}

function formatChange(symbol, change, changePercent) {
  if (symbol.startsWith('^T')) {
    const bps = change * 100;
    return (bps > 0 ? '+' : '') + bps.toFixed(1) + 'bp';
  }
  return (changePercent > 0 ? '+' : '') + changePercent.toFixed(2) + '%';
}

// ============================================================================
// MACRO DATA
// ============================================================================

async function fetchMacroData() {
  if (!CONFIG.FRED_API_KEY) {
    console.log('⚠️  FRED API key not configured');
    return [];
  }

  const indicators = [
    { id: 'SOFR', label: 'SOFR', tripwire: 5.50 },
    { id: 'RRPONTSYD', label: 'Fed RRP', tripwire: 200 }
  ];

  const macroData = [];

  for (const indicator of indicators) {
    try {
      const response = await axios.get(
        'https://api.stlouisfed.org/fred/series/observations',
        {
          params: {
            series_id: indicator.id,
            api_key: CONFIG.FRED_API_KEY,
            file_type: 'json',
            sort_order: 'desc',
            limit: 2
          },
          timeout: 5000
        }
      );

      const observations = response.data.observations;
      if (observations && observations.length >= 2) {
        const latest = parseFloat(observations[0].value);
        const previous = parseFloat(observations[1].value);
        const change = latest - previous;

        macroData.push({
          label: indicator.label,
          value: latest.toFixed(2),
          change: (change > 0 ? '+' : '') + change.toFixed(2),
          dir: change > 0 ? 'up' : change < 0 ? 'down' : 'neutral',
          tripwireHit: latest > indicator.tripwire,
          date: observations[0].date
        });
      }
    } catch (error) {
      // Skip
    }
  }

  return macroData;
}

// ============================================================================
// SCHEDULED JOBS
// ============================================================================

// News every 5 minutes
cron.schedule('*/5 * * * *', () => {
  console.log('📰 Polling NewsAPI...');
  pollNewsAPI();
});

// Market data every 15 seconds
cron.schedule('*/15 * * * * *', async () => {
  try {
    const marketData = await fetchMarketData();
    broadcast({ type: 'market_update', data: marketData });
  } catch (error) {
    console.error('Market update error:', error.message);
  }
});

// Macro data every 5 minutes
cron.schedule('*/5 * * * *', async () => {
  try {
    const macroData = await fetchMacroData();
    broadcast({ type: 'macro_update', data: macroData });
  } catch (error) {
    console.error('Macro update error:', error.message);
  }
});

// ============================================================================
// SERVER START
// ============================================================================

server.listen(CONFIG.PORT, () => {
  console.log(`
  ╔═══════════════════════════════════════════════════════╗
  ║   THE DAILY NUTHATCH LIVE DESK - STABLE VERSION      ║
  ║   Port: ${CONFIG.PORT}                                        ║
  ╚═══════════════════════════════════════════════════════╝
  
  📡 WebSocket: RUNNING
  📰 NewsAPI: ${CONFIG.NEWSAPI_KEY ? '✅ ENABLED' : '❌ DISABLED'}
  🏦 FRED Data: ${CONFIG.FRED_API_KEY ? '✅ ENABLED' : '❌ DISABLED'}
  📊 Market Data: ✅ ENABLED
  
  Assets monitored: 19 (US yields, precious metals, energy, equities, forex)
  
  Frontend: http://localhost:${CONFIG.PORT}
  `);

  // Initial data fetch
  setTimeout(() => {
    console.log('📰 Initial NewsAPI fetch...');
    pollNewsAPI();
  }, 3000);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  server.close(() => process.exit(0));
});

process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error.message);
  // Don't exit, just log
});

process.on('unhandledRejection', (error) => {
  console.error('Unhandled rejection:', error.message);
  // Don't exit, just log
});
