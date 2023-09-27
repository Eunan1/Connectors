/*
 *  Script to use WebSocket with binance depth20 endpoint to retrieve any amount of order book level price and 
 *  volume data between levels 1 and 20.

 *  Fetches number of levels stored in config.binance.numLevel and stores price and volume for every level up to the level 
 *  stored in config.binance.numLevel in a Clickhouse database, labelled with their associated level and a timestamp.
 *   
 *  Once the script reaches the level stored in config.binance.numLevel it stops. It waits for an update from the Websocket
 *  and when an update is received the order book is adjusted correctly and the new local order book is stored in the database.
 * 
 *  Local orderbook maintenance logic is contained inside handleOrderBook() which is inserted in ws.addEventListener 'message' so it's ran
 *  everytime there's an update from the WebSocket
 * 
 *  Order Book is inserted into database using async insertOrderBooktoDB function. This function now takes the orderbook and
 *  a timestamp from ws.on, so the timestamp is from when data is received from WebSocket and not from when the data was inserted
 *  into the database.
 *  
 *  If config.bitmex.baseVolume = true. Volume is displayed in base currency.
 *  If config.bitmex.baseVolume = false. Volume is displayed in quote currency.
 * 
 *  All cfgs listed below library requires.
 *  
*/


// Importing necessary modules 
const WebSocket = require('ws'); // WebSockets for real-time communication 
const axios = require('axios'); // Axios for making HTTP requests 
const ClickHouse = require('@apla/clickhouse'); // Importing ClickHouse database module
const moment = require('moment'); // Moment for time manipulation 
const config = require('./config.json'); // Importing config file 


// All cfgs. Change the value assigned to the cfgs to change configurables.

// Determine the number of levels to include based on the configuration
const cfgNumLevels = config.binance.numLevel;
if (cfgNumLevels < 1 || cfgNumLevels > 20) {
    console.error("Invalid number of levels. Please choose between 1 and 20 levels for Binance", err);
}

const cfgWS_URL = config.binance.WebSocketUrl; // get Binance WebSocket URL
const cfgREST_URL = config.binance.REST_Url; // get Binance REST URL
const cfgTradePair = config.binance.BTCUSDT_tradePair; // get trade pair to be monitored
const cfgTableName = config.table.BTCUSDT_table; // get table name


const cfgVolumeFlag = config.binance.baseVolume; // config.bitmex.baseVolume = true when volume is wanted in the base currency
                                                 // config.bitmex.baseVolume = false when volume is wanted in quote currency
  
// Extract ClickHouse configuration details from the configuration file
const cfgCHost = config.chDatabase.host; // Database host
const cfgPort = config.chDatabase.port; // Database port
const cfgUser = config.chDatabase.user; // Database user
const cfgPassword = config.chDatabase.password; // Database password

// Creating a ClickHouse client with necessary configurations 
const chClient = new ClickHouse({ host: cfgCHost, port: cfgPort, user: cfgUser, password: cfgPassword });


let orderBook = []; // Initialize an empty order book


// Fetching initial state of order book from Binance's REST API
axios.get(cfgREST_URL)
    .then(response => {
        const { bids, asks } = response.data;
        // Pushing all bids to orderBook
        for (let bid of bids) {
            orderBook.push({ price: bid[0], quantity: bid[1], side: 'Buy' });
        }
        // Pushing all asks to orderBook
        for (let ask of asks) {
            orderBook.push({ price: ask[0], quantity: ask[1], side: 'Sell' });
        }
        console.log("Binance: Initial Order Book Received");
    })
    .catch(error => console.error(error)); // Logging any errors during the process



// Creating a WebSocket connection to the Binance API
const ws = new WebSocket(cfgWS_URL + cfgTradePair);


// Event listener for opening of the WebSocket connection
ws.addEventListener('open', function (event) {
    console.log("Connected to Binance WebSocket API Successfully"); // New line
});


// Event listener for receiving data from the WebSocket
ws.addEventListener('message', handleOrderBook);


/*
 * 
 * Function takes 'data' received from WebSocket, parses and uses it to update a local orderbook
 * 
*/
function handleOrderBook(event) {
    // Parse the incoming message as JSON
    const message = JSON.parse(event.data);

    // Getting the current timestamp
    const currentTimestamp = moment().format("YYYY-MM-DD HH:mm:ss.SSS");

    // Updating the orderBook with new bid data
    for (const bid of message.bids) {
        const [price, quantity] = bid;
        const index = orderBook.findIndex(order => order.price === price && order.side === 'Buy');
        if (quantity === '0.00000000') {
            if (index > -1) orderBook.splice(index, 1);
        } else {
            if (index > -1) {
                orderBook[index].quantity = quantity;
            } else {
                orderBook.push({ price, quantity, side: 'Buy' });
            }
        }
    }

    // Updating the orderBook with new ask data
    for (const ask of message.asks) {
        const [price, quantity] = ask;
        const index = orderBook.findIndex(order => order.price === price && order.side === 'Sell');
        if (quantity === '0.00000000') {
            if (index > -1) orderBook.splice(index, 1);
        } else {
            if (index > -1) {
                orderBook[index].quantity = quantity;
            } else {
                orderBook.push({ price, quantity, side: 'Sell' });
            }
        }
    }

    // Calling the function to insert the updated order book into the database
    insertOrderBookToDB(orderBook, currentTimestamp);
}


/*
 *
 * Takes orderbook from handleOrderBook and inserts to table with timestamp from ws.on when data was received
 * 
*/
async function insertOrderBookToDB(orderBook, timestamp) {
    // Filtering and sorting top bids and asks from the orderBook
    const bids = orderBook.filter(order => order.side === 'Buy').sort((a, b) => b.price - a.price).slice(0, cfgNumLevels);
    const asks = orderBook.filter(order => order.side === 'Sell').sort((a, b) => a.price - b.price).slice(0, cfgNumLevels);
  
    // Inserting the top bid orders into the database
    for (let i = 0; i < bids.length; i++) {
        const bid = bids[i];

        // Switch for volume calculations
        let volumeBid;
        switch (cfgVolumeFlag) {
            case true:
                // If baseVolume = true in the config file, base volume = default for binance
                volumeBid = bid.quantity;
                break;
            case false:
                // If baseVolume = false in the config file, calculate the quote volume
                volumeBid = bid.price * bid.quantity;
                break;
            default:
                console.error("volumeFlag: Unexpected Value", err);
        }

        const query = `
            INSERT INTO ${cfgTableName} (timestamp, platform, order_level, order_type, price, volume)
            VALUES ('${timestamp}', 'Binance', '${i+1}', 'Bid', '${bid.price}', '${volumeBid}')
        `;
        await chClient.querying(query);
    }
  

    // Inserting the top ask orders into the database
    for (let i = 0; i < asks.length; i++) {
        const ask = asks[i];

        let volumeAsk;
        switch (cfgVolumeFlag) {
            case true:
                // If baseVolume = true in the config file, base volume = default for binance
                volumeAsk = ask.quantity;
                break;
            case false:
                // If baseVolume = false in the config file, calculate the quote volume
                volumeAsk = ask.price * ask.quantity;
                break;
            default:
                console.error("volumeFlag: Unexpected Value", err);
        }

      const query = `
        INSERT INTO ${cfgTableName} (timestamp, platform, order_level, order_type, price, volume)
        VALUES ('${timestamp}', 'Binance', '${i+1}', 'Ask', '${ask.price}', '${volumeAsk}')
      `;
  
      await chClient.querying(query);
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
init();

