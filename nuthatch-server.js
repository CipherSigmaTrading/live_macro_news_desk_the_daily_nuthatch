// The Daily Nuthatch - Live Data Backend Server (MAXIMUM FREE APIs)
// Enhanced with premium RSS feeds from defense, commodities, finance sources

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const cron = require('node-cron');
const Parser = require('rss-parser');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const rssParser = new Parser({
  timeout: 10000,
  customFields: {
    item: ['category', 'media:content']
  }
});

// Configuration
const CONFIG = {
  NEWSAPI_KEY: process.env.NEWSAPI_KEY,
  FRED_API_KEY: process.env.FRED_API_KEY,
  ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY,
  PORT: process.env.PORT || 3000
};

// Track seen articles to avoid duplicates
const seenArticles = new Set();

// WebSocket clients
const clients = new Set();

// Serve static files and enable JSON parsing
app.use(express.static('public'));
app.use(express.json());

// Manual input endpoint
app.post('/api/manual-input', (req, res) => {
  const { headline, category, source } = req.body;
  
  const cardData = {
    type: 'new_card',
    column: category || 'breaking',
    data: {
      time: new Date().toISOString().substr(11, 5),
      headline: headline,
      source: source || 'Manual Input',
      verified: false,
      implications: ['User-submitted item for tracking'],
      impact: 2,
      horizon: 'DAYS',
      tripwires: [],
      probNudge: []
    }
  };

  broadcast(cardData);
  res.json({ success: true });
});

// WebSocket connection handler
wss.on('connection', (ws) => {
  console.log('✅ New client connected');
  clients.add(ws);

  ws.on('close', () => {
    console.log('❌ Client disconnected');
    clients.delete(ws);
  });

  sendInitialData(ws);
});

// Broadcast to all connected clients
function broadcast(data) {
  const message = JSON.stringify(data);
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Send initial data to new client
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
    console.error('Error sending initial data:', error.message);
  }
}

// ============================================================================
// COMPREHENSIVE RSS FEEDS - ALL FREE
// ============================================================================

const RSS_FEEDS = [
  // BREAKING NEWS & FINANCE
  { url: 'https://feeds.reuters.com/reuters/businessNews', category: 'breaking', name: 'Reuters Business' },
  { url: 'https://feeds.reuters.com/Reuters/worldNews', category: 'geo', name: 'Reuters World' },
  { url: 'https://www.cnbc.com/id/100003114/device/rss/rss.html', category: 'breaking', name: 'CNBC Top News' },
  { url: 'https://www.cnbc.com/id/10000664/device/rss/rss.html', category: 'market', name: 'CNBC Markets' },
  { url: 'https://feeds.marketwatch.com/marketwatch/topstories/', category: 'breaking', name: 'MarketWatch' },
  
  // MACRO & CENTRAL BANKS
  { url: 'https://www.federalreserve.gov/feeds/press_all.xml', category: 'macro', name: 'Federal Reserve' },
  { url: 'https://www.ecb.europa.eu/rss/press.html', category: 'macro', name: 'ECB Press' },
  { url: 'https://www.bis.org/doclist/all_rss.xml', category: 'macro', name: 'BIS' },
  { url: 'https://www.imf.org/en/News/RSS', category: 'macro', name: 'IMF News' },
  { url: 'https://www.treasury.gov/resource-center/data-chart-center/Pages/feed.aspx', category: 'macro', name: 'US Treasury' },
  
  // GEOPOLITICS & DEFENSE
  { url: 'https://www.defensenews.com/arc/outboundfeeds/rss/', category: 'geo', name: 'Defense News' },
  { url: 'https://feeds.reuters.com/Reuters/UKWorldNews', category: 'geo', name: 'Reuters UK World' },
  { url: 'https://rss.nytimes.com/services/xml/rss/nyt/World.xml', category: 'geo', name: 'NYT World' },
  { url: 'https://www.csis.org/analysis/feed', category: 'geo', name: 'CSIS Analysis' },
  { url: 'http://www.understandingwar.org/feeds.xml', category: 'geo', name: 'ISW' },
  
  // COMMODITIES & ENERGY
  { url: 'https://www.eia.gov/rss/todayinenergy.xml', category: 'commodity', name: 'EIA Today' },
  { url: 'https://www.mining.com/feed/', category: 'commodity', name: 'Mining.com' },
  { url: 'https://www.world-grain.com/rss/News.aspx', category: 'commodity', name: 'World Grain' },
  { url: 'https://www.agweb.com/rss-feeds/news', category: 'commodity', name: 'AgWeb' },
  { url: 'https://www.spglobal.com/commodityinsights/RSS-Feeds/latest-news', category: 'commodity', name: 'S&P Commodities' },
  { url: 'https://www.reuters.com/rssFeed/energy', category: 'commodity', name: 'Reuters Energy' },
  { url: 'https://www.cmegroup.com/rss/feed/market_commentary.rss', category: 'commodity', name: 'CME Group' },
  
  // ADDITIONAL QUALITY SOURCES
  { url: 'https://www.wsj.com/xml/rss/3_7085.xml', category: 'breaking', name: 'WSJ Markets' },
  { url: 'https://feeds.bloomberg.com/markets/news.rss', category: 'market', name: 'Bloomberg Markets' },
  { url: 'https://www.ft.com/rss/world', category: 'geo', name: 'Financial Times World' },
  { url: 'https://feeds.bbci.co.uk/news/business/rss.xml', category: 'breaking', name: 'BBC Business' }
];

async function pollRSSFeeds() {
  for (const feed of RSS_FEEDS) {
    try {
      const parsed = await rssParser.parseURL(feed.url);
      
      for (const item of parsed.items.slice(0, 3)) {
        const articleId = item.link || item.guid;
        
        if (!seenArticles.has(articleId)) {
          seenArticles.add(articleId);
          
          // Limit cache size
          if (seenArticles.size > 5000) {
            const firstItem = seenArticles.values().next().value;
            seenArticles.delete(firstItem);
          }
          
          await processRSSItem(item, feed.category, feed.name);
        }
      }
    } catch (error) {
      // Silently skip failed feeds
      if (error.code !== 'ECONNREFUSED' && error.code !== 'ETIMEDOUT') {
        console.error(`RSS error (${feed.name}):`, error.message);
      }
    }
  }
}

async function processRSSItem(item, defaultCategory, sourceName) {
  const text = (item.title || '') + ' ' + (item.contentSnippet || item.content || '');
  const category = classifyNews(text) || defaultCategory;
  const implications = await generateImplications(item.title, category);
  
  const cardData = {
    type: 'new_card',
    column: category,
    data: {
      time: new Date().toISOString().substr(11, 5),
      headline: item.title,
      source: sourceName,
      verified: true,
      implications: implications.bullets || [],
      impact: implications.impact || 2,
      horizon: implications.horizon || 'DAYS',
      tripwires: implications.tripwires || [],
      probNudge: implications.probNudge || []
    }
  };

  broadcast(cardData);
}

// ============================================================================
// NEWS API - Breaking News
// ============================================================================

async function pollNewsAPI() {
  if (!CONFIG.NEWSAPI_KEY) {
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
      for (const article of response.data.articles) {
        const articleId = article.url;
        
        if (!seenArticles.has(articleId)) {
          seenArticles.add(articleId);
          await processNewsArticle(article);
        }
      }
    }
  } catch (error) {
    if (error.response?.status !== 429) {
      console.error('NewsAPI error:', error.response?.data?.message || error.message);
    }
  }
}

async function processNewsArticle(article) {
  const category = classifyNews(article.title + ' ' + article.description);
  const implications = await generateImplications(article.title, category);
  
  const cardData = {
    type: 'new_card',
    column: category,
    data: {
      time: new Date().toISOString().substr(11, 5),
      headline: article.title,
      source: article.source.name,
      verified: true,
      implications: implications.bullets || [],
      impact: implications.impact || 2,
      horizon: implications.horizon || 'DAYS',
      tripwires: implications.tripwires || [],
      probNudge: implications.probNudge || []
    }
  };

  broadcast(cardData);
}

// ============================================================================
// GDELT PROJECT - Geopolitical Events
// ============================================================================

async function pollGDELT() {
  try {
    const keywords = '(military OR conflict OR sanctions OR "central bank" OR china OR russia OR taiwan OR iran OR oil OR gold)';
    
    const response = await axios.get('http://api.gdeltproject.org/api/v2/doc/doc', {
      params: {
        query: keywords,
        mode: 'artlist',
        maxrecords: 15,
        format: 'json',
        sort: 'datedesc'
      },
      timeout: 10000
    });

    if (response.data && response.data.articles) {
      for (const article of response.data.articles.slice(0, 5)) {
        const articleId = article.url;
        
        if (!seenArticles.has(articleId)) {
          seenArticles.add(articleId);
          await processGDELTArticle(article);
        }
      }
    }
  } catch (error) {
    // GDELT can be slow, skip silently
  }
}

async function processGDELTArticle(article) {
  const category = classifyNews(article.title);
  const implications = await generateImplications(article.title, category);
  
  const cardData = {
    type: 'new_card',
    column: category,
    data: {
      time: new Date().toISOString().substr(11, 5),
      headline: article.title,
      source: article.domain || 'GDELT',
      verified: false,
      implications: implications.bullets || [],
      impact: implications.impact || 2,
      horizon: implications.horizon || 'DAYS',
      tripwires: implications.tripwires || [],
      probNudge: implications.probNudge || []
    }
  };

  broadcast(cardData);
}

// ============================================================================
// CLASSIFICATION ENGINE
// ============================================================================

function classifyNews(text) {
  if (!text) return 'breaking';
  
  const lower = text.toLowerCase();
  
  const keywords = {
    breaking: ['breaking', 'just in', 'alert', 'urgent', 'now:', 'developing', 'update:'],
    
    macro: ['fed', 'federal reserve', 'ecb', 'boj', 'pboc', 'central bank', 'bank of england',
            'interest rate', 'rates', 'yield', 'inflation', 'cpi', 'ppi', 'pce', 
            'jobs', 'employment', 'unemployment', 'payrolls', 'wages',
            'fomc', 'powell', 'lagarde', 'yellen', 'treasury', 'bonds', 'qe', 'qt',
            'liquidity', 'credit', 'spread', 'repo', 'sofr', 'libor', 'gdp', 'recession',
            'fiscal', 'deficit', 'debt ceiling', 'stimulus', 'quantitative'],
    
    geo: ['russia', 'ukraine', 'china', 'taiwan', 'iran', 'israel', 'gaza', 'hamas',
          'north korea', 'pyongyang', 'military', 'sanctions', 'war', 'conflict', 
          'drone', 'strike', 'attack', 'nato', 'invasion', 'escalation', 'tension', 
          'strait', 'blockade', 'missile', 'nuclear', 'defense', 'weapon', 'arms',
          'pentagon', 'sovereignty', 'territorial', 'diplomacy', 'ceasefire',
          'geopolitical', 'xi jinping', 'putin', 'biden', 'netanyahu'],
    
    commodity: ['oil', 'crude', 'brent', 'wti', 'opec', 'petroleum', 'gasoline', 'diesel',
                'gold', 'silver', 'platinum', 'copper', 'aluminum', 'zinc', 'nickel',
                'wheat', 'corn', 'soybeans', 'rice', 'grain', 'agriculture',
                'energy', 'natgas', 'natural gas', 'lng', 'pipeline', 'refinery',
                'mining', 'metals', 'commodities', 'shipping', 'freight', 'tanker',
                'drought', 'harvest', 'crop', 'fertilizer', 'steel', 'iron ore'],
    
    market: ['stock', 'equity', 'sp500', 's&p', 'nasdaq', 'dow', 'ftse', 'dax', 'nikkei',
             'rally', 'selloff', 'correction', 'crash', 'bull', 'bear',
             'eur', 'euro', 'dollar', 'dxy', 'forex', 'currency', 'yuan', 'yen',
             'vix', 'volatility', 'futures', 'options', 'derivatives',
             'earnings', 'profit', 'revenue', 'guidance', 'ipo', 'merger', 'acquisition']
  };

  let scores = {};
  
  for (let [category, words] of Object.entries(keywords)) {
    scores[category] = words.filter(w => lower.includes(w)).length;
  }

  // Breaking takes priority if keywords match
  if (scores.breaking > 0 && Object.values(scores).some(s => s > 1)) {
    return 'breaking';
  }

  // Return highest scoring category
  const maxScore = Math.max(...Object.values(scores));
  if (maxScore === 0) return 'breaking';

  return Object.keys(scores).find(k => scores[k] === maxScore);
}

// ============================================================================
// AI IMPLICATIONS GENERATOR
// ============================================================================

async function generateImplications(headline, category) {
  if (!CONFIG.ANTHROPIC_API_KEY) {
    return generateBasicImplications(headline, category);
  }

  try {
    const prompt = `You are an intelligence analyst for a financial news desk. 

Headline: "${headline}"
Category: ${category}

Provide a structured analysis in JSON format:
{
  "bullets": [2-3 short implication bullets],
  "impact": 1-3 (1=low, 2=medium, 3=high),
  "horizon": "NOW" | "DAYS" | "WEEKS" | "MONTHS",
  "tripwires": [1-2 specific levels or events to watch],
  "probNudge": [{"label": "risk type", "dir": "up"|"down"}]
}

Focus on:
- Second-order effects (who benefits, who is constrained, what flows change)
- Specific asset implications
- Concrete tripwire levels

Be concise and clinical.`;

    const response = await axios.post(
      'https://api.anthropic.com/v1/messages',
      {
        model: 'claude-sonnet-4-20250514',
        max_tokens: 500,
        messages: [{
          role: 'user',
          content: prompt
        }]
      },
      {
        headers: {
          'x-api-key': CONFIG.ANTHROPIC_API_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json'
        },
        timeout: 10000
      }
    );

    const text = response.data.content[0].text;
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    
    if (jsonMatch) {
      return JSON.parse(jsonMatch[0]);
    }
    
    return generateBasicImplications(headline, category);
  } catch (error) {
    return generateBasicImplications(headline, category);
  }
}

function generateBasicImplications(headline, category) {
  const categoryInsights = {
    macro: ['Monitor central bank response and forward guidance shifts', 'Watch bond yields and dollar reaction'],
    geo: ['Track escalation indicators and allied responses', 'Monitor safe haven flows and energy prices'],
    commodity: ['Watch supply chain disruptions and inventory levels', 'Monitor inflation expectations and currency impacts'],
    market: ['Track sector rotation and volatility measures', 'Watch for contagion across asset classes']
  };

  return {
    bullets: categoryInsights[category] || [
      'Monitoring for market reaction and follow-up developments',
      'Watching for cross-asset implications'
    ],
    impact: 2,
    horizon: 'DAYS',
    tripwires: [],
    probNudge: []
  };
}

// ============================================================================
// MARKET DATA (Yahoo Finance)
// ============================================================================

async function fetchMarketData() {
  const symbols = [
    // US Treasuries
    'DX-Y.NYB',      // Dollar Index
    '^TNX',          // US 10Y
    '^TYX',          // US 30Y
    '^IRX',          // US 2Y (13-week T-bill as proxy)
    
    // International Yields
    '^TNJ',          // Japan 10Y (not always available, fallback handled)
    '^TNB',          // UK 10Y (not always available, fallback handled)
    '^TNG',          // Germany 10Y (not always available, fallback handled)
    
    // Precious Metals
    'GC=F',          // Gold
    'SI=F',          // Silver
    'PL=F',          // Platinum
    
    // Energy
    'CL=F',          // WTI Crude
    'BZ=F',          // Brent Crude
    'NG=F',          // Natural Gas
    
    // Base Metals
    'HG=F',          // Copper
    
    // Equities
    '^GSPC',         // S&P 500
    '^IXIC',         // NASDAQ
    
    // Forex
    'EURUSD=X',      // EUR/USD
    'GBPUSD=X',      // GBP/USD
    'USDJPY=X',      // USD/JPY
    
    // Volatility
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
      // Skip failed symbols
    }
  }

  return marketData;
}

function formatSymbolLabel(symbol) {
  const labels = {
    // US Treasuries
    'DX-Y.NYB': 'DXY',
    '^TNX': 'US 10Y',
    '^TYX': 'US 30Y',
    '^IRX': 'US 2Y',
    
    // International Yields
    '^TNJ': 'JAPAN 10Y',
    '^TNB': 'UK 10Y',
    '^TNG': 'GER 10Y',
    
    // Precious Metals
    'GC=F': 'GOLD',
    'SI=F': 'SILVER',
    'PL=F': 'PLATINUM',
    
    // Energy
    'CL=F': 'WTI',
    'BZ=F': 'BRENT',
    'NG=F': 'NATGAS',
    
    // Base Metals
    'HG=F': 'COPPER',
    
    // Equities
    '^GSPC': 'S&P 500',
    '^IXIC': 'NASDAQ',
    
    // Forex
    'EURUSD=X': 'EURUSD',
    'GBPUSD=X': 'GBPUSD',
    'USDJPY=X': 'USDJPY',
    
    // Volatility
    '^VIX': 'VIX'
  };
  return labels[symbol] || symbol;
}

function formatValue(symbol, value) {
  if (symbol.includes('USD=X')) {
    return value.toFixed(4);
  } else if (symbol.startsWith('^T')) {
    // All Treasury and yield symbols (US and international)
    // Yahoo returns yields as percentages already, so just format
    return value.toFixed(3) + '%';
  } else if (symbol.includes('=F') && !symbol.includes('VIX')) {
    return '$' + value.toFixed(2);
  } else {
    return value.toFixed(2);
  }
}

function formatChange(symbol, change, changePercent) {
  if (symbol.startsWith('^T')) {
    // Yields - show change in basis points
    const bps = change * 100; // Convert percentage change to basis points
    return (bps > 0 ? '+' : '') + bps.toFixed(1) + 'bp';
  }
  return (changePercent > 0 ? '+' : '') + changePercent.toFixed(2) + '%';
}

// ============================================================================
// MACRO DATA (FRED)
// ============================================================================

async function fetchMacroData() {
  if (!CONFIG.FRED_API_KEY) return [];

  const indicators = [
    { id: 'SOFR', label: 'SOFR', tripwire: 5.50 },
    { id: 'RRPONTSYD', label: 'Fed RRP', tripwire: 200 },
    { id: 'BAMLH0A0HYM2', label: 'HY Spread', tripwire: 500 },
    { id: 'BAMLC0A0CM', label: 'IG Spread', tripwire: 150 }
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

// RSS feeds every 2 minutes (comprehensive coverage)
cron.schedule('*/2 * * * *', pollRSSFeeds);

// NewsAPI every 5 minutes
cron.schedule('*/5 * * * *', pollNewsAPI);

// GDELT every 10 minutes
cron.schedule('*/10 * * * *', pollGDELT);

// Market data every 10 seconds
cron.schedule('*/10 * * * * *', async () => {
  const marketData = await fetchMarketData();
  broadcast({ type: 'market_update', data: marketData });
});

// Macro data every 5 minutes
cron.schedule('*/5 * * * *', async () => {
  const macroData = await fetchMacroData();
  broadcast({ type: 'macro_update', data: macroData });
});

// Hourly segments
cron.schedule('0,15,30,45 * * * *', () => {
  broadcast({ type: 'trigger_segment' });
});

// ============================================================================
// SERVER START
// ============================================================================

server.listen(CONFIG.PORT, () => {
  console.log(`
  ╔═══════════════════════════════════════════════════════╗
  ║   THE DAILY NUTHATCH LIVE DESK - MAXIMUM EDITION     ║
  ║   Port: ${CONFIG.PORT}                                        ║
  ╚═══════════════════════════════════════════════════════╝
  
  📡 WebSocket server running
  📰 NewsAPI: ${CONFIG.NEWSAPI_KEY ? '✅ ENABLED' : '❌ DISABLED'}
  🌍 GDELT: ✅ ENABLED (free)
  📡 RSS Feeds: ✅ ${RSS_FEEDS.length} PREMIUM SOURCES
  📊 Market data: ✅ ENABLED (Yahoo Finance)
  🏦 Macro data: ${CONFIG.FRED_API_KEY ? '✅ ENABLED' : '❌ DISABLED'} (FRED)
  🤖 AI implications: ${CONFIG.ANTHROPIC_API_KEY ? '✅ ENABLED' : '❌ DISABLED'}
  
  RSS Sources Active:
  ├─ Reuters (Business, World, Energy)
  ├─ Federal Reserve, ECB, BIS, IMF, Treasury
  ├─ Defense News, CSIS, ISW
  ├─ EIA, Mining.com, AgWeb, CME Group
  ├─ CNBC, MarketWatch, WSJ, Bloomberg, FT, BBC
  └─ ${RSS_FEEDS.length} total premium feeds
  
  Frontend: http://localhost:${CONFIG.PORT}
  Manual input: POST to /api/manual-input
  `);

  // Initial fetches
  setTimeout(pollRSSFeeds, 2000);
  setTimeout(pollNewsAPI, 5000);
  setTimeout(pollGDELT, 8000);
});

process.on('SIGTERM', () => {
  server.close(() => process.exit(0));
});
