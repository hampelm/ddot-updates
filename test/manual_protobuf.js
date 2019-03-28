var protobuf = require("protobufjs");

// For testing how the new protobufjs works

var feedMessage = {
   header: {
     gtfsRealtimeVersion: 1,
     incrementality: 2,
     timestamp: Date.now()
   },
   entity: []
}

protobuf.load("gtfs-realtime.proto", function(error, root) {
  if (error) { 
    console.log(error); 
    return; 
  }

  // Obtain a message type
  FeedMessage = root.lookupType("transit_realtime.FeedMessage");

  // Create a new message
  var message = FeedMessage.fromObject(feedMessage); // or use .fromObject if conversion is necessary
  serializedFeed = FeedMessage.encode(message).finish();
});

