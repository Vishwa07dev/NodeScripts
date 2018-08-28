var mongoClient = require('mongodb').MongoClient;

var group = {
    "key": {
    "distributorId": true,
        "carrierMethodId": true
},
    "initial": {
    "totalUnitCount": 0,
        "totalShipmentCost": 0
},
    "reduce": function(obj, prev) {
    prev.totalUnitCount = prev.totalUnitCount + obj.shipmentUnitCount;
    prev.totalShipmentCost = prev.totalShipmentCost + obj.shipment_shippingCost;
},
    "finalize": function(prev) {

},
    "cond": {
    "createdAt": {$gt: new Date("2017-04-15T00:00:00.000Z"), $lt: new Date("2017-04-18T00:00:00.000Z")},
    "chosen": "Y",
        "consumerId": "OMS",
        "callType":"OMS"
}
}


mongoClient.connect('mongodb://mcse:mcsew@10.65.164.37:27017/mcse-trace?socketTimeoutMS=7777777777', function(err, db) {
    if(!err) {
        console.log("connection success");
        db.collection("shipment").group(group.key,group.cond,group.initial,group.reduce,group.finalize,true , function(err, results){
            if(err){
                console.log(err);
            }
            console.log('Success' + results.length);
        } );
    }

});
