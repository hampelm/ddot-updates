/*jslint node: true, indent: 2, sloppy: true, white: true, vars: true */

var converter = require('./avl-gtfs-converter.js');
var csv = require('csv');

// cb(error, tripMap)
function createTripMap(builder, avlTrips, cb) {
  var tripMap = null;
  var blockMap = {};
  var goodCount = 0;
  var count = 0;
  csv()
  .from(avlTrips, {columns: false, trim: true})
  .on('data', function (data, index) {
    count += 1;
    var startNode = data[3];
    var endNode = data[2];
    var endTime = data[1];
    var avlTripId = data[0];
    var blockId = data[4];

    tripMap = builder(avlTripId, startNode, endNode, endTime);

    if (tripMap[avlTripId] !== undefined) {
      goodCount += 1;
    }

    var trips = blockMap[blockId];
    if (trips === undefined) {
      trips = [];
      blockMap[blockId] = trips;
    }
    trips.push({
      id: avlTripId,
      endTime: endTime
    });
  })
  .on('end', function (count) {
    console.log('Successfully mapped ' + goodCount + ' trips out of ' + count);
    cb(null, tripMap, blockMap);
  })
  .on('error', function (error) {
    cb(error);
  });
}

// cb(error, stopMap)
function createStopMap(builder, avlStop, cb) {
  var stopMap = null;
  var count = 0;
  var badCount = 0;
  csv()
  .from(avlStop, {columns: false, trim: true})
  .on('data', function (data, index) {
    var avlId = data[0];
    var stopName = data[1].toLocaleLowerCase();

    stopMap = builder(stopName, avlId);

    if (stopMap[avlId] === undefined) {
      console.log('Error: did not find ' + stopName + ' in GTFS. AVL ID: ' + avlId);
      badCount += 1;
    }

    count += 1;
  })
  .on('end', function (count) {
    console.log('');
    console.log('Processed ' + count + ' stops from AVL.');
    console.log('Found ' + badCount + ' that did not match GTFS stop names.');

    cb(null, stopMap);
  })
  .on('error', function (error) {
    cb(error);
  });
}

// cb(error, workBlockMap)
function createWorkTripMap(blockMap, avlWorkBlock, cb) {
  // Map work piece IDs to a set of trips
  // Each trip is an object containing an ID and an end time
  var map = {};
  var count = 0;
  var badCount = 0;
  csv()
  .from(avlWorkBlock, {columns: false, trim: true})
  .on('data', function (data, index) {
    map[data[0]] = blockMap[data[1]];
    count += 1;
    if (map[data[0]] === undefined) {
      badCount += 1;
    }
  })
  .on('end', function (count) {
    console.log('');
    console.log('Processed ' + count + ' work piece IDs from AVL.');
    console.log('Found ' + badCount + ' that did not match block IDs from the AVL trips table.');

    cb(null, map);
  })
  .on('error', function (error) {
    cb(error);
  });
}


function StaticData() {
  this.tripMap = null;
  this.stopMap = null;
  this.avlTripsDebug = null;

  this.avlTimestamp = 0;
  this.timestamps = {
    trips: null,
    stops: null,
    blocks: null
  };
}

//StaticData.prototype = new EventEmitter();
StaticData.prototype = {};

// Check if we've created all of the necessary data to understand the regular AVL updates
StaticData.prototype.hasCompleteData = function () {
  console.log("Checking if we have complete data...");
  // XXX
  if (!this.avlTrips) {
    console.log('Need static trip info.');
    return false;
  }
  if (!this.startNodeMap) {
    console.log('Need the GTFS start node map.');
    return false;
  }
  if (!this.stopNameMap) {
    console.log('Need the GTFS stop name map.');
    return false;
  }
  if (!this.calendar) {
    console.log('Need the GTFS calendar function.');
    return false;
  }
  if (!this.avlBlocks) {
    console.log('Need the map from work piece ID to block ID');
    return false;
  }
  if (!this.avlStops) {
    console.log('Need static stop info');
    return false;
  }
  // XXX

  // Checking if null doesn't catch undefined. 
  var complete = (this.avlStops != null &&
          !this.avlTrips !== null &&
          this.avlBlocks !== null &&
          this.startNodeMap !== null &&
          this.stopNameMap !== null &&
          this.calendar !== null);

  console.log("hasCompleteData?", complete);
  return complete;
};

// create tripMap and stopMap
StaticData.prototype.createIdMaps = function() {
  var self = this;

  var tripMapBuilder = converter.getTripMapBuilder(self.startNodeMap);
  console.log("Using AVL Trips", self.avlTrips)
  createTripMap(tripMapBuilder, self.avlTrips, function (error, tripMap, blockMap) {

    if (error) {
      console.log(error.message);
      return;
    }

    self.tripMap = tripMap;

    console.log('Built new map from AVL trips to GTFS trips.');

    createWorkTripMap(blockMap, self.avlBlocks, function (error, map) {
      // Save AVL trips for debugging
      // XXX
      self.avlTripsDebug = self.avlTrips;

      // Reset the AVL static trip data
      self.avlTrips = null;
      // Reset the AVL work piece/block data
      self.avlBlocks = null;

      if (error) {
        console.log(error.message);
      } else {
        self.workTripMap = map;

        console.log('Built new map from AVL work pieces to AVL trips.');
      }
    });
  });

  var stopMapBuilder = converter.getStopMapBuilder(self.stopNameMap);
  createStopMap(stopMapBuilder, self.avlStops, function (error, map) {
    // Reset the AVL static stop data
    self.avlStops = null;

    if (error) {
      console.log(error.message);
    } else {
      self.stopMap = map;

      console.log('Built new map from AVL stops to GTFS stops.');
    }
  });
};

StaticData.prototype.setGtfsTables = function (tables) {
  this.startNodeMap = tables.startNodeMap;
  this.stopNameMap = tables.stopNameMap;
  // Calendar is really a function, not a table.
  this.calendar = tables.calendar;

  if (this.hasCompleteData()) {
    this.createIdMaps();
  }
};

StaticData.prototype.setAvlTrips = function (avlTrips) {
  this.avlTrips = avlTrips;
  if (this.hasCompleteData()) {
    this.createIdMaps();
  }

  // Update the timestamp
  this.setTimestamp('trips', Date.now());
};

StaticData.prototype.setAvlStops = function (avlStops) {
  this.avlStops = avlStops;
  console.log('Got AVL static stop info.');
  if (this.hasCompleteData()) {
    this.createIdMaps();
  }

  // Update the timestamp
  this.setTimestamp('stops', Date.now());
};

StaticData.prototype.setAvlBlocks = function (avlBlocks) {
  this.avlBlocks = avlBlocks;

  if (this.hasCompleteData()) {
    this.createIdMaps();
  }

  // Update the timestamp
  this.setTimestamp('blocks', Date.now());
};

StaticData.prototype.setTimestamp = function (name, ts) {
  this.timestamps[name] = ts;
  var tripsTs = this.timestamps.trips;
  var stopsTs = this.timestamps.stops;
  var blocksTs = this.timestamps.blocks;

  if ((this.timestamps.trips !== null) &&
      (this.timestamps.stops !== null) &&
      (this.timestamps.blocks !== null)) {
    this.avlTimestamp = ts;
    this.timestamps.trips = null;
    this.timestamps.stops = null;
    this.timestamps.blocks = null;
  }
};

StaticData.prototype.getAvlAge = function () {
  return Date.now() - this.avlTimestamp;
};

module.exports = (function () {
  return {
    StaticData: StaticData
  };
}());
