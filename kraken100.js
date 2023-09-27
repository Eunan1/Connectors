/*
 *  Script to use WebSocket with kraken depth 100 endpoint to retrieve any amount of order book level price and 
 *  volume data between levels 1 and 100.

 *  Fetches number of levels stored in config.kraken.numLevel and stores price and volume for every level up to the level 
 *  stored in config.kraken.numLevel in a Clickhouse database, labelled with their associated level and a timestamp.
 *   
 *  Once the script reaches the level stored in config.kraken.numLevel it stops. It waits for an update from the Websocket
 *  and when an update is received the order book is adjusted correctly and the new local order book is stored in the database.
 *  
 *  Local orderbook maintenance logic is contained inside handleOrderBook() which is inserted in ws.on 'message' so it's ran
 *  everytime there's an update from the WebSocket
 * 
 *  Order Book is inserted into database using async insertOrderBooktoDB function. This function now takes the orderbook and
 *  a timestamp from ws.on, so the timestamp is from when data is received from WebSocket and not from when the data was inserted
 *  into the database.
 *  
 *  If config.kraken.baseVolume = true. Volume is displayed in base currency.
 *  If config.kraken.baseVolume = false. Volume is displayed in quote currency.
 * 
 *  All cfgs listed below library requires.
 *  
*/

  
const _ = require('lodash');
const WebSocket = require('ws'); // WebSockets for real-time communication
const ClickHouse = require('@apla/clickhouse');
const config = require('./config'); // replace './config' with the path to your config file
const moment = require('moment'); // Moment for time manipulation


// All cfgs. Change the value assigned to the cfgs to change configurables.

// Determine the number of levels to include based on the configuration
const cfgNumLevels = config.kraken.numLevel;

if (cfgNumLevels < 1 || cfgNumLevels > 100) {
  console.error("Invalid number of levels. Please choose between 1 and 100 levels for Kraken", err);
}

const cfgWS_URL = config.kraken.WebSocketUrl; // get Binance WebSocket URL
const cfgTradePair = config.kraken.BTCUSDT_tradePair; // get trade pair to be monitored
const cfgTableName = config.table.BTCUSDT_table; // get table name


const cfgVolumeFlag = config.kraken.baseVolume; // config.bitmex.baseVolume = true when volume is wanted in the base currency
                                                // config.bitmex.baseVolume = false when volume is wanted in quote currency


// Extract ClickHouse configuration details from the configuration file
const cfgCHost = config.chDatabase.host; // Database host
const cfgPort = config.chDatabase.port; // Database port
const cfgUser = config.chDatabase.user; // Database user
const cfgPassword = config.chDatabase.password; // Database password


// Creating a ClickHouse client with necessary configurations 
const chClient = new ClickHouse({ host: cfgCHost, port: cfgPort, user: cfgUser, password: cfgPassword });

// Set up a WebSocket for real-time data communication
const ws = new WebSocket(cfgWS_URL);  // Use the WebSocket URL from the configuration file


let orderbook = {}; // Initialize an empty order book


// On WebSocket Open
ws.on('open', function open() {
    const payload = {
    "event": "subscribe",
    "pair": [cfgTradePair],
    "subscription": {
      "name": "book",
      "depth": 100
    }
  };
  ws.send(JSON.stringify(payload));
  console.log("Connected to Kraken WebSocket API Successfully"); // Logging successful connection to WebSocket
});


// The websocket message event listener now uses the handleOrderBook function
ws.on('message', handleOrderBook);

// WebSocket on Error
ws.on('error', function error(err) {
  console.log('WebSocket error: ' + err);
});


/*
 * 
 * Function takes 'data' received from WebSocket, parses and uses it to update a local orderbook
 * 
*/
function handleOrderBook(data) {  
  // Parse the incoming data
  const message = JSON.parse(data);

  // Get the current timestamp in the specified format
  const currentTimestamp = moment().format("YYYY-MM-DD HH:mm:ss.SSS");

  // Check if the message contains ask and bid snapshots
  if (message[1] && message[1].as && message[1].bs) {
    // If so, create a new orderbook with the received snapshot data
    orderbook = {
      asks: _.fromPairs(message[1].as),
      bids: _.fromPairs(message[1].bs)
    };
  } 
  // If the message doesn't contain snapshots but updates for ask or bid
  else if (message[1] && (message[1].a || message[1].b)) {
    // If there are updates for asks
    if (message[1].a) {
      // Iterate over each update
      message[1].a.forEach(update => {
        // Destructure price and volume from the update
        const [price, volume] = update;
        // If volume is zero, delete the ask price from the orderbook
        if (volume === "0.00000000") {
          delete orderbook.asks[price];
        } 
        // Otherwise, update the volume for the ask price
        else {
          orderbook.asks[price] = volume;
        }
      });
    }
    // If there are updates for bids
    if (message[1].b) {
      // Iterate over each update
      message[1].b.forEach(update => {
        // Destructure price and volume from the update
        const [price, volume] = update;
        // If volume is zero, delete the bid price from the orderbook
        if (volume === "0.00000000") {
          delete orderbook.bids[price];
        } 
        // Otherwise, update the volume for the bid price
        else {
          orderbook.bids[price] = volume;
        }
      });
    }
  }
  
  // Create a new ordered book object by sorting asks in ascending 
  // and bids in descending order of prices
  const orderedBook = {
    asks: _.orderBy(_.toPairs(orderbook.asks), x => parseFloat(x[0]), 'asc'),
    bids: _.orderBy(_.toPairs(orderbook.bids), x => parseFloat(x[0]), 'desc')
  };

  // Insert the ordered book into the database and handle any errors
  insertOrderBookToDB(orderedBook, currentTimestamp).catch(console.error);
}


/*
 *
 * Takes orderbook from handleOrderBook and inserts to table with timestamp from ws.on when data was received
 * 
*/
async function insertOrderBookToDB(orderbook, timestamp) {
  const platform = 'Kraken';
  const orderbookData = [];

  for (const [index, [price, volume]] of orderbook.asks.slice(0, cfgNumLevels).entries()) {
    let finalVolume;
    switch(cfgVolumeFlag) {
      case true:
        // The volume is already in base currency, so no conversion is needed
        finalVolume = volume;
        break;
      case false:
        // Convert the volume to quote currency by multiplying by the price
        finalVolume = parseFloat(volume) * parseFloat(price);
        break;
      default:
        console.error("Invalid cfgVolumeFlag. Please set it to either true or false.", err);
        return; // Exit the function
    }
    orderbookData.push(`('${timestamp}', '${platform}', ${index + 1}, 'ask', ${parseFloat(price)}, ${parseFloat(finalVolume)})`);
  }

  for (const [index, [price, volume]] of orderbook.bids.slice(0, cfgNumLevels).entries()) {
    let finalVolume;
    switch(cfgVolumeFlag) {
      case true:
        // The volume is already in base currency, so no conversion is needed
        finalVolume = volume;
        break;
      case false:
        // Convert the volume to quote currency by multiplying by the price
        finalVolume = parseFloat(volume) * parseFloat(price);
        break;
      default:
        console.error("Invalid cfgVolumeFlag. Please set it to either true or false.", err);
        return; // Exit the function
    }
    orderbookData.push(`('${timestamp}', '${platform}', ${index + 1}, 'bid', ${parseFloat(price)}, ${parseFloat(finalVolume)})`);
  }

  const query = `INSERT INTO ${cfgTableName} (timestamp, platform, order_level, order_type, price, volume) VALUES ${orderbookData.join(', ')}`;
  
  try {
    await chClient.querying(query);
  } catch (err) {
    console.error('Failed to insert order book:', err);
  }
}



// Define a function to initialize the ClickHouse database
async function init() {
  try {
    // Create the BTCUSDT table in the database if it doesn't already exist
    await chClient.querying(`
    CREATE TABLE IF NOT EXISTS ${cfgTableName} (
      timestamp DateTime64(3, 'Europe/London'),
      platform String CODEC(ZSTD(1)),
      order_level Float64 CODEC(ZSTD(1)),
      order_type String CODEC(ZSTD(1)),
      price Float64 CODEC(ZSTD(1)),
      volume Float64 CODEC(ZSTD(1))
    ) ENGINE = MergeTree() 
    ORDER BY timestamp
   `);
  } catch (err) {
    // If there was an error initializing the database, log the error
    console.error('Failed to initialize:', err);
  }
}

// Call the database initialization function
init().catch(console.error);
