/*
 *  Script to use WebSocket with kucoin depth 50 endpoint to retrieve any amount of order book level price and 
 *  volume data between levels 1 and 50.

 *  Fetches number of levels stored in config.kucoin.numLevel and stores price and volume for every level up to the level 
 *  stored in config.binance.numLevel in a Clickhouse database, labelled with their associated level and a timestamp.
 *   
 *  Once the script reaches the level stored in config.kucoin.numLevel it stops. It waits for an update from the Websocket
 *  and when an update is received the order book is adjusted correctly and the new local order book is stored in the database.
 *  
 *  Local orderbook maintenance logic is contained inside handleOrderBook() which is inserted in ws.on 'message' so it's ran
 *  everytime there's an update from the WebSocket
 * 
 *  Order Book is inserted into database using async insertOrderBooktoDB function. This function now takes the orderbook and
 *  a timestamp from ws.on, so the timestamp is from when data is received from WebSocket and not from when the data was inserted
 *  into the database.
 *  
 *  IMPORTANT NOTE: Due to 'deletions' you will not get 50 levels every time as kucoin API doesn't provide this
 *  Blank levels might be added to make queries easier?
 * 
 *  All cfgs listed below library requires.
 *  
*/


// Required modules
const crypto = require('crypto'); // For cryptographic operations
const axios = require('axios'); // For HTTP requests
const WebSocket = require('ws'); // For WebSocket connection
const ClickHouse = require('@apla/clickhouse'); // For interacting with ClickHouse database
const moment = require('moment'); // For handling date and time
const config = require('./config.json'); // Loading configuration file


// Determine the number of levels to include based on the configuration
const cfgNumLevels = config.kucoin.numLevel;
if (cfgNumLevels < 1 || cfgNumLevels > 50) {
    console.error("Invalid number of levels. Please choose between 1 and 50 levels for Kucoin", err);
}

// Initialize orderBook structure
let orderBook = { asks: [], bids: [] };

// API credentials and settings
const cfgAPI_key = config.kucoin.APIKEY;
const cfgAPI_secret = config.kucoin.APISECRET;
const cfgAPI_passphrase = 'Wunna123!';

const cfgBase_url = config.kucoin.baseUrl; // Base API URL
const cfgWebSocket_endpoint = config.kucoin.WebSocket_endpoint; // WebSocket 50 levels general endpoint
const cfgWebSocket_authToken_endpoint = config.kucoin.REST_authToken_endpoint; // REST authentication endpoint
const cfgREST_endpoint = config.kucoin.REST_endpoint; // REST 100 levels general endpoint
const cfgTradePair = config.kucoin.BTCUSDT_tradePair;  // Trade Pair to be analyzed

const REST_auth_URL = cfgBase_url + cfgWebSocket_authToken_endpoint; // Get authentication token URL
const REST_URL = cfgBase_url + cfgREST_endpoint + cfgTradePair; // Get full REST URL request for 100 levels URL
const WS_topic = cfgWebSocket_endpoint + cfgTradePair;

const method = 'GET';
const strToSign = Date.now() + method + cfgREST_endpoint;
const signature = crypto.createHmac('sha256', cfgAPI_secret).update(strToSign).digest('base64');
const passphrase = crypto.createHmac('sha256', cfgAPI_secret).update(cfgAPI_passphrase).digest('base64');
const headers = { 'KC-API-SIGN': signature, 'KC-API-TIMESTAMP': Date.now(), 'KC-API-KEY': cfgAPI_key, 'KC-API-PASSPHRASE': passphrase, 'KC-API-KEY-VERSION': '2', };


// ClickHouse database configuration extracted from config.json
const cfgCHost = config.chDatabase.host;
const cfgPort = config.chDatabase.port;
const cfgUser = config.chDatabase.user;
const cfgPassword = config.chDatabase.password;
const cfgTableName = config.table.BTCUSDT_table; // get table name

// ClickHouse client setup
const chClient = new ClickHouse({ host: cfgCHost, port: cfgPort, user: cfgUser, password: cfgPassword });



// HTTP request to get initial order book
axios.get(REST_URL, { headers: headers })
  .then((response) => {
    // Process the response to extract the order book
    const data = response.data.data;
    orderBook.bids = data.bids.slice(0, 50);
    orderBook.asks = data.asks.slice(0, 50);

    // Request WebSocket token to set up the connection
    axios.post(REST_auth_URL)
      .then((response) => {
        const { token, instanceServers } = response.data.data;
        const endpoint = instanceServers[0].endpoint;
        const ws = new WebSocket(endpoint + '?token=' + token);

        // WebSocket event handlers
        ws.on('open', function open() {
          console.log('Connection opened.');
          const subscribeMsg = { id: Date.now(), type: "subscribe", topic: WS_topic, response: true };
          ws.send(JSON.stringify(subscribeMsg));
          console.log('Subscription message sent.');
        });

        ws.on('message', handleOrderBook); // Every time there's a message update the orderbook

        ws.on('close', function close(code, reason) { console.log('WebSocket closed. Code:', code, 'Reason:', reason); });

        ws.on('error', function error(err) { console.error('WebSocket error:', err); });
      }).catch((error) => { console.error('Error fetching public token:', error); });
  })
  .catch((error) => { console.error(error.response ? error.response.data : error); });



/*
 * 
 * Function takes 'data' received from WebSocket, parses and uses it to update a local orderbook
 * 
*/
function handleOrderBook(data) {
  // Convert message to String
  const message = JSON.parse(data.toString());

  const currentTimestamp = moment().format("YYYY-MM-DD HH:mm:ss.SSS"); // Timestamp put in handleOrderBook as we want the timestamp at ws.on (message received)
  if (message.type === 'message' && message.subject === 'trade.l2update') {
    ["asks", "bids"].forEach(type => {
      const changes = message.data.changes[type];
      changes.forEach(change => {

        // Process order book changes
        const price = change[0];
        const size = change[1];
        let levels = orderBook[type];

        // Update the order levels accordingly
        if (size === "0") { levels = levels.filter(level => level[0] !== price); } // Remove level if size is 0
        else {
          const existingLevel = levels.find(level => level[0] === price); // Find existing level
          if (existingLevel) { existingLevel[1] = size; } // Update existing level
          else { levels.push([price, size]); } // Add new level
        }

        // Retain only the top 50 levels and update the order book
        levels = levels.sort((a, b) => type === "asks" ? a[0] - b[0] : b[0] - a[0]).slice(0, 50);
        orderBook[type] = levels;
      });
    });
    insertOrderBooktoDB(orderBook, currentTimestamp); // Insert updated order book to database
  }
}



/*
 *
 * Takes orderbook from handleOrderBook and inserts to table with timestamp from ws.on when data was received
 * 
*/
async function insertOrderBooktoDB(orderbook, timestamp) {
  // Structure data for insertion
  const platform = 'Kucoin';
  const values = [];
  ['bids', 'asks'].forEach(type => {
    orderbook[type].forEach((order, index) => {
      const [price, volume] = order;
      values.push(`('${timestamp}', '${platform}', ${index + 1}, '${type}', ${price}, ${volume})`);
    });
  });

  // Construct and execute SQL query
  const query = `INSERT INTO ${cfgTableName} (timestamp, platform, order_level, order_type, price, volume) VALUES ${values.join(',')}`;
  try {
    await chClient.querying(query);
    console.log('Data inserted successfully');
  } catch (err) {
    console.error('Error inserting data:', err);
  }
}



// ClickHouse database initialization
async function init() {
  try {
    await chClient.querying(`
      CREATE TABLE IF NOT EXISTS ${cfgTableName} (
        timestamp DateTime64(3, 'Europe/London'),
        platform String CODEC(ZSTD(1)),
        order_level Float64 CODEC(ZSTD(1)),
        order_type String CODEC(ZSTD(1)),
        price Float64 CODEC(ZSTD(1)),
        volume Float64 CODEC(ZSTD(1))
      ) ENGINE = MergeTree() ORDER BY timestamp
    `);
  } catch (err) {
    console.error('Failed to initialize ClickHouse:', err);
  }
}
init(); // Call the database initialization function
