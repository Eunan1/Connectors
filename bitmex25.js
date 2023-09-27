/*
 *  Script to use WebSocket with bitmex realtime orderBookL2_25 endpoint to retrieve any amount of order book level price and 
 *  volume data between levels 1 and 25.

 *  Fetches number of levels stored in config.bitmex.numLevel and stores price and volume for every level up to the level 
 *  stored in config.bitmex.numLevel in a Clickhouse database, labelled with their associated level and a timestamp.
 *   
 *  Once the script reaches the level stored in config.bitmex.numLevel it stops. It waits for an update from the Websocket
 *  and when an update is received the order book is adjusted correctly and the new local order book is stored in the database.
 *
 *  Local orderbook maintenance logic is contained inside handleOrderBook() which is inserted in ws.on 'message' so it's ran
 *  everytime there's an update from the WebSocket
 *
 *  Order Book is inserted into database using async insertOrderBooktoDB function. This function now takes the orderbook and
 *  a timestamp from ws.on, so the timestamp is from when data is received from WebSocket and not from when the data was inserted
 *  into the database.
 * 
 *  Global variable lastInsertedOrderBook is used to stop double insertions that were occuring sometimes.
 * 
 *  Bitmex by default displays volume in quote currency instead of base currency, so I added a cfgVolumeFlag.
 *  If config.bitmex.baseVolume = true. Volume is displayed in base currency.
 *  If config.bitmex.baseVolume = false. Volume is displayed in quote currency.
 * 
 *  All cfgs listed below library requires.
 *
*/


// Import necessary libraries and modules
const WebSocket = require('ws');  // WebSocket module for real-time communication
const ClickHouse = require('@apla/clickhouse');  // ClickHouse module for database interactions
const moment = require('moment');  // Moment.js module for handling dates and times
const config = require('./config.json');  // Configuration file containing database and BitMEX settings


// All cfgs. Change the value assigned to the cfgs to change configurables.

// Determine the number of levels to include based on the configuration
const cfgNumLevels = config.bitmex.numLevel;
if (cfgNumLevels < 1 || cfgNumLevels > 25) {
    console.error("Invalid number of levels. Please choose between 1 and 25 levels for Bitmex", err);
}

const cfgWS_URL = config.bitmex.WebSocketUrl; // get Bitmex WebSocket URL
const cfgTradePair = config.bitmex.BTCUSDT_tradePair; // get trade pair to be monitored
const cfgTableName = config.table.BTCUSDT_table; // get table name


const cfgVolumeFlag = config.bitmex.baseVolume; // config.bitmex.baseVolume = true when volume is wanted in the base currency
                                                // config.bitmex.baseVolume = false when volume is wanted in quote currency

// Extract ClickHouse configuration details from the configuration file
const cfgCHost = config.chDatabase.host; // Database host
const cfgPort = config.chDatabase.port; // Database port
const cfgUser = config.chDatabase.user; // Database user
const cfgPassword = config.chDatabase.password; // Database password

let lastInsertedOrderBook; // Previous orderBook global

// Initialize a new ClickHouse client using the configuration details
const chClient = new ClickHouse({ host: cfgCHost, port: cfgPort, user: cfgUser, password: cfgPassword });


// Set up a WebSocket for real-time data communication
const ws = new WebSocket(cfgWS_URL);  // Use the WebSocket URL from the configuration file


// Create an array to store the state of the order book
let orderBook = [];


// Set up event handlers for the WebSocket connection
ws.on('open', () => { // When the WebSocket connection is opened...
    // Send a message to BitMEX to subscribe to updates about the trading pair
    ws.send(JSON.stringify({
        op: "subscribe",
        args: ["orderBookL2_25:" + cfgTradePair]
    }));
    console.log("Connected to Bitmex WebSocket API Successfully"); // Logging successful connection to WebSocket
});


// Use handleOrderBook function in WebSocket message handler
ws.on('message', handleOrderBook);


ws.on('error', (error) => {  // If there is an error with the WebSocket...
    // Log the error
    console.error(`WebSocket error: ${error}`);
});


ws.on('close', () => {  // When the WebSocket connection is closed...
    // Log the closure
    console.log('WebSocket connection closed');
});



/*
 * 
 * Function takes 'data' received from WebSocket, parses and uses it to update a local orderbook
 * 
*/
function handleOrderBook(data) {
    // Parse the incoming message as JSON
    const message = JSON.parse(data);

    // Record the current time
    const currentTimestamp = moment().format("YYYY-MM-DD HH:mm:ss.SSS");

    // If this is a snapshot of the entire order book...
    if (message.table === 'orderBookL2_25' && message.action === 'partial') {
        // Replace the current order book with the new one
        orderBook = message.data;

        // Insert the new order book data into the ClickHouse database
        insertOrderBookToDB(orderBook, currentTimestamp);
    }

    // If this is an update to the order book (rather than a complete snapshot)
    if (message.table === 'orderBookL2_25' && message.action !== 'partial') {
        // Apply the updates to the current order book
        const updates = message.data;
        for (const update of updates) {
            if (message.action === 'insert') {  // If this is a new order
                orderBook.push(update);  // Add it to the order book
            } else if (message.action === 'update') {  // If this is an update to an existing order
                // Find the existing order in the order book
                const index = orderBook.findIndex(item => item.id === update.id);
                // If the order was found in the order book...
                if (index > -1) {
                    // Update the order in the order book
                    orderBook[index] = {...orderBook[index], ...update};
                }
            } else if (message.action === 'delete') {  // If this is a deletion of an existing order
                // Find the order in the order book
                const index = orderBook.findIndex(item => item.id === update.id);
                // If the order was found in the order book
                if (index > -1) {
                    // Remove the order from the order book
                    orderBook.splice(index, 1);
                }
            }
        }
        // Insert the updated order book data into the ClickHouse database
        insertOrderBookToDB(orderBook, currentTimestamp);
    }
}


/*
 *
 * Takes orderbook from handleOrderBook and inserts to table with timestamp from ws.on when data was received
 * 
*/
async function insertOrderBookToDB(orderBook, timestamp) {
    // Check if the order book data is the same as the last inserted data
    if (JSON.stringify(orderBook) === JSON.stringify(lastInsertedOrderBook)) {
        console.log("Insertion Skipped");
        return;
    }

    // Store the order book data that was just inserted
    lastInsertedOrderBook = JSON.parse(JSON.stringify(orderBook)); // Create a deep copy

    // Split the order book into bids and asks, sort them by price, and take the top few based on the configuration
    const bids = orderBook.filter(order => order.side === 'Buy').sort((a, b) => b.price - a.price);
    const asks = orderBook.filter(order => order.side === 'Sell').sort((a, b) => a.price - b.price);

    // For each of the top few bids...
    for (const [i, order] of bids.slice(0, cfgNumLevels).entries()) {
        let volumeBid = order.size;

        switch (cfgVolumeFlag) {
            case true:
                // If baseVolume = true in the config file, calculate the base volume
                volumeBid = volumeBid / order.price;
                break;
            case false:
                // If baseVolume = false in the config file, quote volume = default for bitmex
                volumeBid = order.size;
                break;
            default:
                console.error("volumeFlag: Unexpected Value", err);
                break;
        }
    
    // Insert the bid into the ClickHouse database
    await chClient.querying(`
        INSERT INTO ${cfgTableName} (timestamp, platform, order_level, order_type, price, volume)
        VALUES ('${timestamp}', 'BitMEX', '${i+1}', 'Bid', '${order.price}', '${volumeBid}')
        `);
    }

    // For each of the top few asks...
    for (const [i, order] of asks.slice(0, cfgNumLevels).entries()) {
        let volumeAsk = order.size;

        switch (cfgVolumeFlag) {
            case true:
                // If baseVolume = true in the config file, calculate the base volume
                volumeAsk = volumeAsk / order.price;
                break;
            case false:
                // If baseVolume = false in the config file, quote volume = default for bitmex
                volumeAsk = order.size;
                break;
            default:
                console.error("volumeFlag: Unexpected Value");
                break;
        }
    
    // Insert the ask into the ClickHouse database
    await chClient.querying(`
        INSERT INTO ${cfgTableName} (timestamp, platform, order_level, order_type, price, volume)
        VALUES ('${timestamp}', 'BitMEX', '${i+1}', 'Ask', '${order.price}', '${volumeAsk}')
        `);
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
