var mongoose = require('mongoose');
var config = require('config.json')('configurations/config.json');
var async = require("async");
var nodemailer = require('nodemailer');
var json2csv = require('json2csv');

var pqaConnection = mongoose.createConnection(config.mongoDetails.pqa.connectionUrl,{
    server: {
        socketOptions: {
            socketTimeoutMS: 0,
            connectionTimeout: 0
        }
    }
});
var prodConnection = mongoose.createConnection(config.mongoDetails.prod.connectionUrl,{
    server: {
        socketOptions: {
            socketTimeoutMS: 0,
            connectionTimeout: 0
        }
    }
});
var stgConnection = mongoose.createConnection(config.mongoDetails.stg.connectionUrl);

exports.handleRequest = function (request) {

    //choosing the type of connection needed
    var connection;
    switch (request.body.env) {
        case "prod":
            connection = prodConnection;
            break;
        case "pqa":
            connection = pqaConnection;
            break;
        case "stg":
            connection = stgConnection;
            break;
    }

    if (!connection) {
        console.log("no valid connection was made :(");
        return null;
    }


    console.log(request.body.emailIds);
    //defining the type of variables needed for the response based on the type of requests
    var overAllResult = {};
    var splitByDCType = {};
    var splitBySLATier = {};
    var splitByCMType = {};
    var splitByDCId = {};
    var splitByCM = {};
    var splitByDCDestinationState = {};
    var splitByDCCmType = {};
    var splitByDCZone = {};

    //Getting the startDate and endDate from the request
    var startDate = request.startDate;
    var endDate = request.endDate;

    //calltype FORCED_RESHOP has to be excluded
    var condition = {
        "consumerId": "OMS", "callType": "SOURCING", "orderDate": {$gt: startDate, $lt: endDate}
    }

    var simulatorProfile = request.body.profileName;
    if (simulatorProfile == "" || simulatorProfile == null || simulatorProfile == undefined) {
        console.log("inside if");

		condition = {
            "consumerId": "OMS", "callType": "SOURCING", "orderDate": {$gt: startDate, $lt: endDate}, "dc": { $nin: [  887581, 227970061, 100001, 100002, 100003, 100004, 100005] } ,"orderNo" :{$ne :'-1'}
        }
    } else {
        console.log("isnide esle");
        condition = {
            "consumerId": "OMS",
            "callType": "SOURCING",
            "orderDate": {$gt: startDate, $lt: endDate},
			"simulatorProfile": simulatorProfile,	
			"dc": { $nin: [ 887581, 227970061, 100001, 100002, 100003, 100004, 100005 ] },
			"orderNo": {$ne:'-1'}
		}
    }

    //node-mailer setup
    var transporter = nodemailer.createTransport();

    //Tasks for getting the overAllResult
    var asyncOverAllTasks = [];
    asyncOverAllTasks.push(overAllResult1);
    asyncOverAllTasks.push(overAllResult2);
    asyncOverAllTasks.push(overAllResult3);

    var htmlBody = '<html><head></head><body style="background-color:#f2f2f2"><h1> Thanks for using MCSE Reporting Framework !!!</h1> <br><br> <h2 align="center">We have started preparing the report ...</h2>' +
        '</body></html>';

    //Async job for the overallResults ##################################################
    function overAllReport(callback) {
        async.parallel(asyncOverAllTasks, function () {

            callback();
        });

    }

    //Tasks for getting the reportSplitBy-DCType
    var asyncSplitByDCTypeTasks = [];
    asyncSplitByDCTypeTasks.push(splitByDCTypeOrderCount);
    asyncSplitByDCTypeTasks.push(splitByDCTypeShipmentCount);
    asyncSplitByDCTypeTasks.push(splitByDCTypeCostAndCount);
    asyncSplitByDCTypeTasks.push(splitByDCTypeBoxCount);

    //Tasks for getting the report Split by SLATier
    var asyncSplitBySLATierTasks = [];
    asyncSplitBySLATierTasks.push(splitBySLATierOrderCount);
    asyncSplitBySLATierTasks.push(splitBySLATierShipmentCount);
    asyncSplitBySLATierTasks.push(splitBySLATierCostAndCount);
    asyncSplitBySLATierTasks.push(splitBySLATierBoxCount);

    //Tasks for getting the report Split by CMType
    var asyncSplitByCMTypeTasks = [];
    asyncSplitByCMTypeTasks.push(splitByCMTypeOrderCount);
    asyncSplitByCMTypeTasks.push(splitByCMTypeShipmentCount);
    asyncSplitByCMTypeTasks.push(splitByCMTypeCostAndCount);
    asyncSplitByCMTypeTasks.push(splitByCMTypeBoxCount);

    //Tasks for getting the report Split by DC
    var asyncSplitByDCIdTasks = [];
    asyncSplitByDCIdTasks.push(splitByDcIdOrderCount);
    asyncSplitByDCIdTasks.push(splitByDCIdShipmentCount);
    asyncSplitByDCIdTasks.push(splitByDCIdCostAndCount);
    asyncSplitByDCIdTasks.push(splitByDCIdBoxCount);

    //Tasks for getting the report Split by CM
    var asyncSplitByCMTasks = [];
    asyncSplitByCMTasks.push(splitByCMOrderCount);
    asyncSplitByCMTasks.push(splitByCMShipmentCount);
    asyncSplitByCMTasks.push(splitByCMCostAndCount);
    asyncSplitByCMTasks.push(splitByCMBoxCount);

    //Tasks for getting the report Split by DC-Destination State
    var asyncSplitByDCDestStateTasks = [];
    asyncSplitByDCDestStateTasks.push(splitByDCDestStateOrderCount);
    asyncSplitByDCDestStateTasks.push(splitByDCDestStateShipmentCount);
    asyncSplitByDCDestStateTasks.push(splitByDCDestStateCostAndCount);
    asyncSplitByDCDestStateTasks.push(splitByDCDestStateBoxCount);

    //Tasks for getting the report Split by DC-CM Type
    var asyncSplitByDCCmTypeTasks = [];
    asyncSplitByDCCmTypeTasks.push(splitByDCCmTypeOrderCount);
    asyncSplitByDCCmTypeTasks.push(splitByDCCmTypeShipmentCount);
    asyncSplitByDCCmTypeTasks.push(splitByDCCmTypeCostAndCount);
    asyncSplitByDCCmTypeTasks.push(splitByDCCmTypeBoxCount);

    //Tasks for getting the report Split by DC-Zone
    var asyncSplitByDCZoneTasks = [];
    asyncSplitByDCZoneTasks.push(splitByDCZoneOrderCount);
    asyncSplitByDCZoneTasks.push(splitByDCZoneShipmentCount);
    asyncSplitByDCZoneTasks.push(splitByDCZoneCostAndCount);
    asyncSplitByDCZoneTasks.push(splitByDCZoneBoxCount);

    function reportSplitByDCType(callback) {
        async.parallel(asyncSplitByDCTypeTasks, function () {
            console.log("In the second one -> " + JSON.stringify(splitByDCType));
            callback();
        });

    }

    function reportSplitBySLATier(callback) {
        async.parallel(asyncSplitBySLATierTasks, function () {
            console.log("In the third one -> " + JSON.stringify(splitBySLATier));
            callback();
        });

    }

    function reportSplitByCMType(callback) {
        async.parallel(asyncSplitByCMTypeTasks, function () {
            console.log("In the Fourth one -> " + JSON.stringify(splitByCMType));
            callback();
        });

    }

    function reportSplitByDCId(callback) {
        async.parallel(asyncSplitByDCIdTasks, function () {
            console.log("In the Fifth one -> " + JSON.stringify(splitByDCId));
            callback();
        });

    }

    function reportSplitByCM(callback) {
        async.parallel(asyncSplitByCMTasks, function () {
            console.log("In the sixth one -> " + JSON.stringify(splitByCM));
            callback();
        });

    }

    function reportSplitByDCDestState(callback) {
        async.parallel(asyncSplitByDCDestStateTasks, function () {
            console.log("In the seventh one -> " + JSON.stringify(splitByDCDestinationState));
            callback();
        });

    }

    function reportSplitByDCCmType(callback) {
        async.parallel(asyncSplitByDCCmTypeTasks, function () {
            console.log("In the eighth one -> " + JSON.stringify(splitByDCCmType));
            callback();
        });

    }

    function reportSplitByDCZone(callback) {
        async.parallel(asyncSplitByDCZoneTasks, function () {
            console.log("In the ninth one -> " + JSON.stringify(splitByDCZone));
            callback();
        });

    }

    // JOBs object
    var requestedJobs = {
        "overallResult": overAllReport,
        "splitBySLATier": reportSplitBySLATier,
        "splitByDCType": reportSplitByDCType,
        "splitByCMType": reportSplitByCMType,
        "splitByDC": reportSplitByDCId,
        "splitByCM": reportSplitByCM,
        "splitByDCDestState": reportSplitByDCDestState,
        "splitByDCCmType": reportSplitByDCCmType,
        "splitByDCZone": reportSplitByDCZone
    };

    var asyncJobs = [];
    request.body.setOfMatrics.split(",").forEach(function (matric) {
        asyncJobs.push(requestedJobs[matric]);
    })


    async.parallel(asyncJobs, function () {
        htmlBody = '<html><head>' +
            '<style>' +
            'table {' +
            'font-family: arial, sans-serif;' +
            'border-collapse: collapse;' +
            'width: 100%;' +
            '}' +

            'td, th {' +
            'border: 1px solid #dddddd;' +
            'text-align: left;' +
            'padding: 8px;' +
            '}' +

            'tr:nth-child(even) {' +
            'background-color: #dddddd;' +
            '}' +
            '</style>' +
            '</head><body style="background-color:#f2f2f2"><h2> Report for the duration - ' + startDate + ' to ' + endDate + ' in ' + request.body.env + ' environment</h2> <br>';


        if (!(Object.keys(overAllResult).length === 0)) {
            htmlBody = htmlBody + '<h2 align="center">Overall report</h2>';
            htmlBody = htmlBody + '<table>' +
                '<tr> <td><b>Total Shipments</b></td><td>' + overAllResult.totalShipments + '</td>' +
                '<tr> <td><b>Total Orders</b></td><td>' + overAllResult.totalOrder + '</td>' +
                '<tr> <td><b>Total FulfillmentCost</b></td><td>' + overAllResult.totalFulfillmentCost + '</td>' +
                '<tr> <td><b>Total ShippingCost</b></td><td>' + overAllResult.totalShippingCost + '</td>' +
                '<tr> <td><b>Total Units</b></td><td>' + overAllResult.totalUnits + '</td>' +
                '<tr> <td><b>Total Box Units</b></td><td>' + overAllResult.boxCount + '</td>' +
                '</table><br>';
        }


        if (!(Object.keys(splitByDCType).length === 0)) {
            htmlBody = htmlBody + '<b></b><h2 align="center"> Report with split by DC Type</h2></b>';
            htmlBody = htmlBody + '<table>';
            htmlBody = htmlBody + '<tr> <th><b>DC Type</b></th><th><b>Total Shipments</b></th><th><b>Total Orders</b></th><th><b>Total FulfillmentCost</b></th><th><b>Total ShippingCost</b></th><th><b>Total Units</b></th><th><b>Total Box Units</b></th></tr>';

            for (var property in splitByDCType) {
                if (splitByDCType.hasOwnProperty(property)) {
                    htmlBody = htmlBody + '<tr> <td><b>' + property + '</b></td>';
                    htmlBody = htmlBody + '<td>' + splitByDCType[property].totalShipments + '</td><td>' + splitByDCType[property].totalOrders + '</td><td>' + splitByDCType[property].totalFulfillmentCost + '</td><td>' + splitByDCType[property].totalShippingCost + '</td><td>' + splitByDCType[property].totalUnits + '</td><td>'+ splitByDCType[property].boxCount + '</td></tr>';
                }
            }
            htmlBody = htmlBody + '</table><br><br>';


        }

        if (!(Object.keys(splitBySLATier).length === 0)) {
            htmlBody = htmlBody + '<b></b><h2 align="center"> Report with split by SLA Tier</h2></b>';
            htmlBody = htmlBody + '<table>';
            htmlBody = htmlBody + '<tr> <th><b>SLA Tier</b></th><th><b>Total Shipments</b></th><th><b>Total Orders</b></th><th><b>Total FulfillmentCost</b></th><th><b>Total ShippingCost</b></th><th><b>Total Units</b></th><th><b>Total Box Units</b></th></tr>';

            for (var property in splitBySLATier) {
                if (splitBySLATier.hasOwnProperty(property)) {
                    htmlBody = htmlBody + '<tr> <td><b>' + property + '</b></td>';
                    htmlBody = htmlBody + '<td>' + splitBySLATier[property].totalShipments + '</td><td>' + splitBySLATier[property].totalOrders + '</td><td>' + splitBySLATier[property].totalFulfillmentCost + '</td><td>' + splitBySLATier[property].totalShippingCost + '</td><td>' + splitBySLATier[property].totalUnits + '</td><td>'+ splitBySLATier[property].boxCount + '</td></tr>';
                }
            }
            htmlBody = htmlBody + '</table><br><br>';

        }

        if (!(Object.keys(splitByCMType).length === 0)) {
            htmlBody = htmlBody + '<b></b><h2 align="center"> Report with split by Carrier Method Type</h2></b>';
            htmlBody = htmlBody + '<table>';
            htmlBody = htmlBody + '<tr> <th><b>CM Type</b></th><th><b>Total Shipments</b></th><th><b>Total Orders</b></th><th><b>Total FulfillmentCost</b></th><th><b>Total ShippingCost</b></th><th><b>Total Units</b></th><th><b>Total Box Units</b></th></tr>';
            for (var property in splitByCMType) {
                if (splitByCMType.hasOwnProperty(property)) {

                    htmlBody = htmlBody + '<tr> <td><b>' + property + '</b></td>';
                    htmlBody = htmlBody + '<td>' + splitByCMType[property].totalShipments + '</td><td>' + splitByCMType[property].totalOrders + '</td><td>' + splitByCMType[property].totalFulfillmentCost + '</td><td>' + splitByCMType[property].totalShippingCost + '</td><td>' + splitByCMType[property].totalUnits + '</td><td>'+ splitByCMType[property].boxCount + '</td></tr>';
                }
            }
            htmlBody = htmlBody + '</table>';

        }

        htmlBody = htmlBody + '</body></html>';

        var csvDCData = [];
        for (var property in splitByDCId) {
            if (splitByDCId.hasOwnProperty(property)) {
                var cData = {};
                cData.DC = property;
                cData.TotalShipments = splitByDCId[property].totalShipments;
                cData.TotalOrders = splitByDCId[property].totalOrders;
                cData.TotalFulfillmentCost = splitByDCId[property].totalFulfillmentCost;
                cData.TotalShippingCost = splitByDCId[property].totalShippingCost;
                cData.TotalUnitCount = splitByDCId[property].totalUnits;
                cData.TotalBoxCount = splitByDCId[property].boxCount;
                csvDCData.push(cData);


            }

        }

        var csvFieldsDC = ['DC', 'TotalShipments', 'TotalOrders', 'TotalFulfillmentCost', 'TotalShippingCost', 'TotalUnitCount', 'TotalBoxCount'];

        var csvDC = json2csv({data: csvDCData, fields: csvFieldsDC});


        var csvCMData = [];
        for (var property in splitByCM) {
            if (splitByCM.hasOwnProperty(property)) {
                var cData = {};
                cData.CM = property;
                cData.TotalShipments = splitByCM[property].totalShipments;
                cData.TotalOrders = splitByCM[property].totalOrders;
                cData.TotalFulfillmentCost = splitByCM[property].totalFulfillmentCost;
                cData.TotalShippingCost = splitByCM[property].totalShippingCost;
                cData.TotalUnitCount = splitByCM[property].totalUnits;
                cData.TotalBoxCount = splitByCM[property].boxCount;
                csvCMData.push(cData);


            }

        }

        var csvFieldsCM = ['CM', 'TotalShipments', 'TotalOrders', 'TotalFulfillmentCost', 'TotalShippingCost', 'TotalUnitCount', 'TotalBoxCount'];

        var csvCM = json2csv({data: csvCMData, fields: csvFieldsCM});


        var csvDCDestStateData = [];
        for (var property in splitByDCDestinationState) {
            if (splitByDCDestinationState.hasOwnProperty(property)) {
                var cData = {};
                cData.DC = property.split('~~')[0];
                cData.DestinationState = property.split('~~')[1];
                cData.TotalShipments = splitByDCDestinationState[property].totalShipments;
                cData.TotalOrders = splitByDCDestinationState[property].totalOrders;
                cData.TotalFulfillmentCost = splitByDCDestinationState[property].totalFulfillmentCost;
                cData.TotalShippingCost = splitByDCDestinationState[property].totalShippingCost;
                cData.TotalUnitCount = splitByDCDestinationState[property].totalUnits;
                cData.TotalBoxCount = splitByDCDestinationState[property].boxCount;
                csvDCDestStateData.push(cData);


            }

        }

        var csvFieldsDCDestState = ['DC', 'DestinationState', 'TotalShipments', 'TotalOrders', 'TotalFulfillmentCost', 'TotalShippingCost', 'TotalUnitCount', 'TotalBoxCount'];

        var csvDCDestState = json2csv({data: csvDCDestStateData, fields: csvFieldsDCDestState});



        var csvDCCmTypeData = [];
        for (var property in splitByDCCmType) {
            if (splitByDCCmType.hasOwnProperty(property)) {
                var cData = {};
                cData.DCType = property.split('~~')[0];
                cData.CmType = property.split('~~')[1];
                cData.TotalShipments = splitByDCCmType[property].totalShipments;
                cData.TotalOrders = splitByDCCmType[property].totalOrders;
                cData.TotalFulfillmentCost = splitByDCCmType[property].totalFulfillmentCost;
                cData.TotalShippingCost = splitByDCCmType[property].totalShippingCost;
                cData.TotalUnitCount = splitByDCCmType[property].totalUnits;
                cData.TotalBoxCount = splitByDCCmType[property].boxCount;
                csvDCCmTypeData.push(cData);
            }
        }
        var csvFieldsDCCmType = ['DCType', 'CmType', 'TotalShipments', 'TotalOrders', 'TotalFulfillmentCost', 'TotalShippingCost', 'TotalUnitCount', 'TotalBoxCount'];
        var csvDCCmType = json2csv({data: csvDCCmTypeData, fields: csvFieldsDCCmType});


        var csvDCZoneData = [];
        for (var property in splitByDCZone) {
            if (splitByDCZone.hasOwnProperty(property)) {
                var cData = {};
                cData.DC = property.split('~~')[0];
                cData.Zone = property.split('~~')[1];
                cData.TotalShipments = splitByDCZone[property].totalShipments;
                cData.TotalOrders = splitByDCZone[property].totalOrders;
                cData.TotalFulfillmentCost = splitByDCZone[property].totalFulfillmentCost;
                cData.TotalShippingCost = splitByDCZone[property].totalShippingCost;
                cData.TotalUnitCount = splitByDCZone[property].totalUnits;
                cData.TotalBoxCount = splitByDCZone[property].boxCount;
                csvDCZoneData.push(cData);
            }
        }
        var csvFieldsDCZone = ['DC', 'Zone', 'TotalShipments', 'TotalOrders', 'TotalFulfillmentCost', 'TotalShippingCost', 'TotalUnitCount', 'TotalBoxCount'];
        var csvDCZone = json2csv({data: csvDCZoneData, fields: csvFieldsDCZone});

        var attach = [];
        console.log(Object.keys(splitByDCDestinationState).length);
        if (!(Object.keys(splitByDCDestinationState).length === 0)) {
            attach.push({
                filename: 'splitByDC-DestState.csv',
                content: csvDCDestState
            });
        }

        console.log(Object.keys(splitByDCId).length);
        if (!(Object.keys(splitByDCId).length === 0)) {
            attach.push({
                filename: 'splitByDC.csv',
                content: csvDC
            });
        }
        console.log(Object.keys(splitByCM).length);
        if (!(Object.keys(splitByCM).length === 0)) {
            attach.push({
                filename: 'splitByCM.csv',
                content: csvCM
            });
        }
        console.log(Object.keys(splitByDCCmType).length);
        if (!(Object.keys(splitByDCCmType).length === 0)) {
            attach.push({
                filename: 'splitByDCCmType.csv',
                content: csvDCCmType
            });
        }

        console.log(Object.keys(splitByDCZone).length);
        if (!(Object.keys(splitByDCZone).length === 0)) {
            attach.push({
                filename: 'splitByDCZone.csv',
                content: csvDCZone
            });
        }
        
        transporter.sendMail({
            from: "mcse_reporting_framework@walmartlabs.com",
            to: request.body.emailIds,
            subject: 'MCSE Reporting Framework - Overall result<Do not Reply to this mail>',
            html: htmlBody,
            attachments: attach

        });

        console.log("Hurray all jobs done :)");
    });


    //Sending the acknowledgement mail ############################################
    transporter.sendMail({
        from: "mcse_reporting_framework@walmartlabs.com",
        to: request.body.emailIds,
        subject: 'MCSE Reporting Framework <Do not Reply to this mail>',
        html: htmlBody,
    });

    //This function gives the total number of orders split by Distributor type
    function splitByDCTypeOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dCType": "$distributorType"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dcType = item._id.dCType;
                if (splitByDCType[dcType] && splitByDCType[dcType].totalOrders) {
                    splitByDCType[dcType].totalOrders = splitByDCType[dcType].totalOrders + 1;
                } else {
                    if (splitByDCType[dcType]) {
                        splitByDCType[dcType].totalOrders = 1;
                    } else {
                        splitByDCType[dcType] = {};
                        splitByDCType[dcType].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by Distributor type
    function splitByDCTypeShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "dCType": "$distributorType",
                    "cmId": "$cm",
                    "dcId": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dcType = item._id.dCType;
                if (splitByDCType[dcType] && splitByDCType[dcType].totalShipments) {
                    splitByDCType[dcType].totalShipments = splitByDCType[dcType].totalShipments + 1;
                } else {
                    if (splitByDCType[dcType]) {
                        splitByDCType[dcType].totalShipments = 1;
                    } else {
                        splitByDCType[dcType] = {};
                        splitByDCType[dcType].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by Distributor type
    function splitByDCTypeCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"dCType": "$distributorType"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcType = item._id.dCType;
                if (splitByDCType[dcType] && splitByDCType[dcType].totalUnits) {
                    splitByDCType[dcType].totalUnits = splitByDCType[dcType].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByDCType[dcType]) {
                        splitByDCType[dcType].totalUnits = item.sumOfUnits;
                    } else {
                        splitByDCType[dcType] = {};
                        splitByDCType[dcType].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByDCType[dcType] && splitByDCType[dcType].totalFulfillmentCost) {
                    splitByDCType[dcType].totalFulfillmentCost = splitByDCType[dcType].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByDCType[dcType]) {
                        splitByDCType[dcType].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByDCType[dcType] = {};
                        splitByDCType[dcType].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByDCType[dcType] && splitByDCType[dcType].totalShippingCost) {
                    splitByDCType[dcType].totalShippingCost = splitByDCType[dcType].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByDCType[dcType]) {
                        splitByDCType[dcType].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByDCType[dcType] = {};
                        splitByDCType[dcType].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }

    //This function gives the total box Count, split by DC Type
    function splitByDCTypeBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dCType": "$distributorType","boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcType = item._id.dCType;
                if (splitByDCType[dcType] && splitByDCType[dcType].boxCount) {
                    splitByDCType[dcType].boxCount = splitByDCType[dcType].boxCount + item.callToMCSE;
                } else {
                    if (splitByDCType[dcType]) {
                        splitByDCType[dcType].boxCount = item.callToMCSE;
                    } else {
                        splitByDCType[dcType] = {};
                        splitByDCType[dcType].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }

    //#############  split by SLATier
    //This function gives the total number of orders split by slaTier
    function splitBySLATierOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "slaTier": "$shippingSLATier"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var slaTier = item._id.slaTier;
                if (splitBySLATier[slaTier] && splitBySLATier[slaTier].totalOrders) {
                    splitBySLATier[slaTier].totalOrders = splitBySLATier[slaTier].totalOrders + 1;
                } else {
                    if (splitBySLATier[slaTier]) {
                        splitBySLATier[slaTier].totalOrders = 1;
                    } else {
                        splitBySLATier[slaTier] = {};
                        splitBySLATier[slaTier].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by SLATier
    function splitBySLATierShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "slaTier": "$shippingSLATier",
                    "cmId": "$cm",
                    "dcId": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var slaTier = item._id.slaTier;
                if (splitBySLATier[slaTier] && splitBySLATier[slaTier].totalShipments) {
                    splitBySLATier[slaTier].totalShipments = splitBySLATier[slaTier].totalShipments + 1;
                } else {
                    if (splitBySLATier[slaTier]) {
                        splitBySLATier[slaTier].totalShipments = 1;
                    } else {
                        splitBySLATier[slaTier] = {};
                        splitBySLATier[slaTier].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by slaTier
    function splitBySLATierCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"slaTier": "$shippingSLATier"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var slaTier = item._id.slaTier;
                if (splitBySLATier[slaTier] && splitBySLATier[slaTier].totalUnits) {
                    splitBySLATier[slaTier].totalUnits = splitBySLATier[slaTier].totalUnits + item.sumOfUnits;
                } else {
                    if (splitBySLATier[slaTier]) {
                        splitBySLATier[slaTier].totalUnits = item.sumOfUnits;
                    } else {
                        splitBySLATier[slaTier] = {};
                        splitBySLATier[slaTier].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitBySLATier[slaTier] && splitBySLATier[slaTier].totalFulfillmentCost) {
                    splitBySLATier[slaTier].totalFulfillmentCost = splitBySLATier[slaTier].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitBySLATier[slaTier]) {
                        splitBySLATier[slaTier].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitBySLATier[slaTier] = {};
                        splitBySLATier[slaTier].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitBySLATier[slaTier] && splitBySLATier[slaTier].totalShippingCost) {
                    splitBySLATier[slaTier].totalShippingCost = splitBySLATier[slaTier].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitBySLATier[slaTier]) {
                        splitBySLATier[slaTier].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitBySLATier[slaTier] = {};
                        splitBySLATier[slaTier].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }

    //This function gives the total box Count, split by SLATier
    function splitBySLATierBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "slaTier": "$shippingSLATier","boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var slaTier = item._id.slaTier;
                if (splitBySLATier[slaTier] && splitBySLATier[slaTier].boxCount) {
                    splitBySLATier[slaTier].boxCount = splitBySLATier[slaTier].boxCount + item.callToMCSE;
                } else {
                    if (splitBySLATier[slaTier]) {
                        splitBySLATier[slaTier].boxCount = item.callToMCSE;
                    } else {
                        splitBySLATier[slaTier] = {};
                        splitBySLATier[slaTier].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }

    //#############  split by CM Type
    //This function gives the total number of orders split by CMType
    function splitByCMTypeOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "cmType": "$carrierMethodType"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var cmType = item._id.cmType;
                if (splitByCMType[cmType] && splitByCMType[cmType].totalOrders) {
                    splitByCMType[cmType].totalOrders = splitByCMType[cmType].totalOrders + 1;
                } else {
                    if (splitByCMType[cmType]) {
                        splitByCMType[cmType].totalOrders = 1;
                    } else {
                        splitByCMType[cmType] = {};
                        splitByCMType[cmType].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by CM Type
    function splitByCMTypeShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cmType": "$carrierMethodType",
                    "cmId": "$cm",
                    "dcId": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var cmType = item._id.cmType;
                if (splitByCMType[cmType] && splitByCMType[cmType].totalShipments) {
                    splitByCMType[cmType].totalShipments = splitByCMType[cmType].totalShipments + 1;
                } else {
                    if (splitByCMType[cmType]) {
                        splitByCMType[cmType].totalShipments = 1;
                    } else {
                        splitByCMType[cmType] = {};
                        splitByCMType[cmType].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by CM Type
    function splitByCMTypeCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"cmType": "$carrierMethodType"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var cmType = item._id.cmType;
                if (splitByCMType[cmType] && splitByCMType[cmType].totalUnits) {
                    splitByCMType[cmType].totalUnits = splitByCMType[cmType].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByCMType[cmType]) {
                        splitByCMType[cmType].totalUnits = item.sumOfUnits;
                    } else {
                        splitByCMType[cmType] = {};
                        splitByCMType[cmType].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByCMType[cmType] && splitByCMType[cmType].totalFulfillmentCost) {
                    splitByCMType[cmType].totalFulfillmentCost = splitByCMType[cmType].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByCMType[cmType]) {
                        splitByCMType[cmType].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByCMType[cmType] = {};
                        splitByCMType[cmType].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByCMType[cmType] && splitByCMType[cmType].totalShippingCost) {
                    splitByCMType[cmType].totalShippingCost = splitByCMType[cmType].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByCMType[cmType]) {
                        splitByCMType[cmType].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByCMType[cmType] = {};
                        splitByCMType[cmType].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }

    //This function gives the total box Count, split by CM Type
    function splitByCMTypeBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "cmType": "$carrierMethodType","boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var cmType = item._id.cmType;
                if (splitByCMType[cmType] && splitByCMType[cmType].boxCount) {
                    splitByCMType[cmType].boxCount = splitByCMType[cmType].boxCount + item.callToMCSE;
                } else {
                    if (splitByCMType[cmType]) {
                        splitByCMType[cmType].boxCount = item.callToMCSE;
                    } else {
                        splitByCMType[cmType] = {};
                        splitByCMType[cmType].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }

    //#############  split by DC Id
    //This function gives the total number of orders split by DC Id
    function splitByDcIdOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dc": "$dc"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dc = item._id.dc;
                if (splitByDCId[dc] && splitByDCId[dc].totalOrders) {
                    splitByDCId[dc].totalOrders = splitByDCId[dc].totalOrders + 1;
                } else {
                    if (splitByDCId[dc]) {
                        splitByDCId[dc].totalOrders = 1;
                    } else {
                        splitByDCId[dc] = {};
                        splitByDCId[dc].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by DC
    function splitByDCIdShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cmId": "$cm",
                    "dc": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dc = item._id.dc;
                if (splitByDCId[dc] && splitByDCId[dc].totalShipments) {
                    splitByDCId[dc].totalShipments = splitByDCId[dc].totalShipments + 1;
                } else {
                    if (splitByDCId[dc]) {
                        splitByDCId[dc].totalShipments = 1;
                    } else {
                        splitByDCId[dc] = {};
                        splitByDCId[dc].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by DC
    function splitByDCIdCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"dc": "$dc"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dc = item._id.dc;
                if (splitByDCId[dc] && splitByDCId[dc].totalUnits) {
                    splitByDCId[dc].totalUnits = splitByDCId[dc].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByDCId[dc]) {
                        splitByDCId[dc].totalUnits = item.sumOfUnits;
                    } else {
                        splitByDCId[dc] = {};
                        splitByDCId[dc].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByDCId[dc] && splitByDCId[dc].totalFulfillmentCost) {
                    splitByDCId[dc].totalFulfillmentCost = splitByDCId[cmType].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByDCId[dc]) {
                        splitByDCId[dc].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByDCId[dc] = {};
                        splitByDCId[dc].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByDCId[dc] && splitByDCId[dc].totalShippingCost) {
                    splitByDCId[dc].totalShippingCost = splitByDCId[dc].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByDCId[dc]) {
                        splitByDCId[dc].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByDCId[dc] = {};
                        splitByDCId[dc].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }

    //This function gives the total box Count, split by DC Id
    function splitByDCIdBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dc": "$dc","boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dc = item._id.dc;
                if (splitByDCId[dc] && splitByDCId[dc].boxCount) {
                    splitByDCId[dc].boxCount = splitByDCId[dc].boxCount + item.callToMCSE;
                } else {
                    if (splitByDCId[dc]) {
                        splitByDCId[dc].boxCount = item.callToMCSE;
                    } else {
                        splitByDCId[dc] = {};
                        splitByDCId[dc].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }
    //#############  split by CM
    //This function gives the total number of orders split by CM
    function splitByCMOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "cm": "$cm"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var cm = item._id.cm;
                if (splitByCM[cm] && splitByCM[cm].totalOrders) {
                    splitByCM[cm].totalOrders = splitByCM[cm].totalOrders + 1;
                } else {
                    if (splitByCM[cm]) {
                        splitByCM[cm].totalOrders = 1;
                    } else {
                        splitByCM[cm] = {};
                        splitByCM[cm].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by CM
    function splitByCMShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cm": "$cm",
                    "dc": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var cm = item._id.cm;
                if (splitByCM[cm] && splitByCM[cm].totalShipments) {
                    splitByCM[cm].totalShipments = splitByCM[cm].totalShipments + 1;
                } else {
                    if (splitByCM[cm]) {
                        splitByCM[cm].totalShipments = 1;
                    } else {
                        splitByCM[cm] = {};
                        splitByCM[cm].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by CM
    function splitByCMCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"cm": "$cm"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var cm = item._id.cm;
                if (splitByCM[cm] && splitByCM[cm].totalUnits) {
                    splitByCM[cm].totalUnits = splitByCM[cm].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByCM[cm]) {
                        splitByCM[cm].totalUnits = item.sumOfUnits;
                    } else {
                        splitByCM[cm] = {};
                        splitByCM[cm].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByCM[cm] && splitByCM[cm].totalFulfillmentCost) {
                    splitByCM[cm].totalFulfillmentCost = splitByCM[cm].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByCM[cm]) {
                        splitByCM[cm].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByCM[cm] = {};
                        splitByCM[cm].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByCM[cm] && splitByCM[cm].totalShippingCost) {
                    splitByCM[cm].totalShippingCost = splitByCM[cm].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByCM[cm]) {
                        splitByCM[cm].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByCM[cm] = {};
                        splitByCM[cm].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }

    //This function gives the total box Count, split by CM
    function splitByCMBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "cm": "$cm","boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var cm = item._id.cm;
                if (splitByCM[cm] && splitByCM[cm].boxCount) {
                    splitByCM[cm].boxCount = splitByCM[cm].boxCount + item.callToMCSE;
                } else {
                    if (splitByCM[cm]) {
                        splitByCM[cm].boxCount = item.callToMCSE;
                    } else {
                        splitByCM[cm] = {};
                        splitByCM[cm].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }
    //#############  split by DC Destination State
    //This function gives the total number of orders split by DC Destination State
    function splitByDCDestStateOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dc": "$dc", "destinationState": "$destState"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {

            docs.forEach(function (item) {
                var dcDestState = item._id.dc + "~~" + item._id.destinationState;
                if (splitByDCDestinationState[dcDestState] && splitByDCDestinationState[dcDestState].totalOrders) {
                    splitByDCDestinationState[dcDestState].totalOrders = splitByDCDestinationState[dcDestState].totalOrders + 1;
                } else {
                    if (splitByDCDestinationState[dcDestState]) {
                        splitByDCDestinationState[dcDestState].totalOrders = 1;
                    } else {
                        splitByDCDestinationState[dcDestState] = {};
                        splitByDCDestinationState[dcDestState].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by DC Destination State
    function splitByDCDestStateShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cm": "$cm",
                    "dc": "$dc",
                    "destinationState": "$destState"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dcDestState = item._id.dc + "~~" + item._id.destinationState;
                if (splitByDCDestinationState[dcDestState] && splitByDCDestinationState[dcDestState].totalShipments) {
                    splitByDCDestinationState[dcDestState].totalShipments = splitByDCDestinationState[dcDestState].totalShipments + 1;
                } else {
                    if (splitByDCDestinationState[dcDestState]) {
                        splitByDCDestinationState[dcDestState].totalShipments = 1;
                    } else {
                        splitByDCDestinationState[dcDestState] = {};
                        splitByDCDestinationState[dcDestState].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by DC Destination state
    function splitByDCDestStateCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"dc": "$dc", "destinationState": "$destState"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcDestState = item._id.dc + "~~" + item._id.destinationState;
                if (splitByDCDestinationState[dcDestState] && splitByDCDestinationState[dcDestState].totalUnits) {
                    splitByDCDestinationState[dcDestState].totalUnits = splitByDCDestinationState[dcDestState].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByDCDestinationState[dcDestState]) {
                        splitByDCDestinationState[dcDestState].totalUnits = item.sumOfUnits;
                    } else {
                        splitByDCDestinationState[dcDestState] = {};
                        splitByDCDestinationState[dcDestState].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByDCDestinationState[dcDestState] && splitByDCDestinationState[dcDestState].totalFulfillmentCost) {
                    splitByDCDestinationState[dcDestState].totalFulfillmentCost = splitByDCDestinationState[dcDestState].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByDCDestinationState[dcDestState]) {
                        splitByDCDestinationState[dcDestState].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByDCDestinationState[dcDestState] = {};
                        splitByDCDestinationState[dcDestState].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByDCDestinationState[dcDestState] && splitByDCDestinationState[dcDestState].totalShippingCost) {
                    splitByDCDestinationState[dcDestState].totalShippingCost = splitByDCDestinationState[dcDestState].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByDCDestinationState[dcDestState]) {
                        splitByDCDestinationState[dcDestState].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByDCDestinationState[dcDestState] = {};
                        splitByDCDestinationState[dcDestState].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }

    //This function gives the total box Count, split by DC Destination state
    function splitByDCDestStateBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dc": "$dc", "destinationState": "$destState","boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcDestState = item._id.dc + "~~" + item._id.destinationState;
                if (splitByDCDestinationState[dcDestState] && splitByDCDestinationState[dcDestState].boxCount) {
                    splitByDCDestinationState[dcDestState].boxCount = splitByDCDestinationState[dcDestState].boxCount + item.callToMCSE;
                } else {
                    if (splitByDCDestinationState[dcDestState]) {
                        splitByDCDestinationState[dcDestState].boxCount = item.callToMCSE;
                    } else {
                        splitByDCDestinationState[dcDestState] = {};
                        splitByDCDestinationState[dcDestState].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }

    //#############  split by DC CM Type
    //This function gives the total number of orders split by DC CM Type
    function splitByDCCmTypeOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "cmType": "$carrierMethodType", "dCType": "$distributorType"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {

            docs.forEach(function (item) {
                var dcCmType = item._id.dCType + "~~" + item._id.cmType;
                if (splitByDCCmType[dcCmType] && splitByDCCmType[dcCmType].totalOrders) {
                    splitByDCCmType[dcCmType].totalOrders = splitByDCCmType[dcCmType].totalOrders + 1;
                } else {
                    if (splitByDCCmType[dcCmType]) {
                        splitByDCCmType[dcCmType].totalOrders = 1;
                    } else {
                        splitByDCCmType[dcCmType] = {};
                        splitByDCCmType[dcCmType].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by DC CM Type
    function splitByDCCmTypeShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cmType": "$carrierMethodType", 
                    "dCType": "$distributorType",
                    "cm": "$cm",
                    "dc": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dcCmType = item._id.dCType + "~~" + item._id.cmType;
                if (splitByDCCmType[dcCmType] && splitByDCCmType[dcCmType].totalShipments) {
                    splitByDCCmType[dcCmType].totalShipments = splitByDCCmType[dcCmType].totalShipments + 1;
                } else {
                    if (splitByDCCmType[dcCmType]) {
                        splitByDCCmType[dcCmType].totalShipments = 1;
                    } else {
                        splitByDCCmType[dcCmType] = {};
                        splitByDCCmType[dcCmType].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by DC CM Type
    function splitByDCCmTypeCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cmType": "$carrierMethodType", 
                    "dCType": "$distributorType"
                },
                callToMCSE: {$sum: 1},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcCmType = item._id.dCType + "~~" + item._id.cmType;
                if (splitByDCCmType[dcCmType] && splitByDCCmType[dcCmType].totalUnits) {
                    splitByDCCmType[dcCmType].totalUnits = splitByDCCmType[dcCmType].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByDCCmType[dcCmType]) {
                        splitByDCCmType[dcCmType].totalUnits = item.sumOfUnits;
                    } else {
                        splitByDCCmType[dcCmType] = {};
                        splitByDCCmType[dcCmType].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByDCCmType[dcCmType] && splitByDCCmType[dcCmType].totalFulfillmentCost) {
                    splitByDCCmType[dcCmType].totalFulfillmentCost = splitByDCCmType[dcCmType].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByDCCmType[dcCmType]) {
                        splitByDCCmType[dcCmType].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByDCCmType[dcCmType] = {};
                        splitByDCCmType[dcCmType].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByDCCmType[dcCmType] && splitByDCCmType[dcCmType].totalShippingCost) {
                    splitByDCCmType[dcCmType].totalShippingCost = splitByDCCmType[dcCmType].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByDCCmType[dcCmType]) {
                        splitByDCCmType[dcCmType].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByDCCmType[dcCmType] = {};
                        splitByDCCmType[dcCmType].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }
    //This function gives the total box Count, split by DC CM Type
    function splitByDCCmTypeBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: { 
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "cmType": "$carrierMethodType", 
                    "dCType": "$distributorType",
                    "boxId": "$boxId"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcCmType = item._id.dCType + "~~" + item._id.cmType;
                if (splitByDCCmType[dcCmType] && splitByDCCmType[dcCmType].boxCount) {
                    splitByDCCmType[dcCmType].boxCount = splitByDCCmType[dcCmType].boxCount + item.callToMCSE;
                } else {
                    if (splitByDCCmType[dcCmType]) {
                        splitByDCCmType[dcCmType].boxCount = item.callToMCSE;
                    } else {
                        splitByDCCmType[dcCmType] = {};
                        splitByDCCmType[dcCmType].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }


    
    function overAllResult1(callback) {

        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "cmId": "$cm", "dcId": "$dc"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            console.log("result came" + docs.length);
            overAllResult.totalShipments = docs.length;
            callback();
        });


    }

    function overAllResult2(callback) {

        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId"},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCost: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            console.log("#############" + err);
            console.log("result came for second" + docs.length);
            overAllResult.totalOrder = docs.length;

            docs.forEach(function (data) {
                if (overAllResult.totalFulfillmentCost == null) {
                    overAllResult.totalFulfillmentCost = data.totalFulfillmentCost;
                } else {
                    overAllResult.totalFulfillmentCost = overAllResult.totalFulfillmentCost + data.totalFulfillmentCost;
                }

                if (overAllResult.totalUnits == null) {
                    overAllResult.totalUnits = data.sumOfUnits;
                } else {
                    overAllResult.totalUnits = overAllResult.totalUnits + data.sumOfUnits;
                }

                if (overAllResult.totalShippingCost == null) {
                    overAllResult.totalShippingCost = data.totalShippingCost;
                } else {
                    overAllResult.totalShippingCost = overAllResult.totalShippingCost + data.totalShippingCost;
                }
            });

            callback();
        });


    }

    function overAllResult3(callback) {

        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "boxId": "$boxId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            console.log("result came" + docs.length);
            docs.forEach(function (data) {
                if (overAllResult.boxCount == null) {
                    overAllResult.boxCount = data.callToMCSE;
                } else {
                    overAllResult.boxCount = overAllResult.boxCount + data.callToMCSE;
                }
            });
            callback();
        });


    }


    //#############  split by DC  Zone Id
    //This function gives the total number of orders split by DC  Zone Id
    function splitByDCZoneOrderCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {"grp_tranx_id": "$callDetail_groupTransactionId", "dc": "$dc", "zone": "$zoneId"},
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {

            docs.forEach(function (item) {
                var dcZone = item._id.dc + "~~" + item._id.zone;
                if (splitByDCZone[dcZone] && splitByDCZone[dcZone].totalOrders) {
                    splitByDCZone[dcZone].totalOrders = splitByDCZone[dcZone].totalOrders + 1;
                } else {
                    if (splitByDCZone[dcZone]) {
                        splitByDCZone[dcZone].totalOrders = 1;
                    } else {
                        splitByDCZone[dcZone] = {};
                        splitByDCZone[dcZone].totalOrders = 1;
                    }
                }

            });
            callback();
        });
    }

    //This function gives the total number of shipments split by DC  Zone Id
    function splitByDCZoneShipmentCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "dc": "$dc", 
                    "zone": "$zoneId",
                    "cm": "$cm",
                    "dc": "$dc"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {
                var dcZone = item._id.dc + "~~" + item._id.zone;
                if (splitByDCZone[dcZone] && splitByDCZone[dcZone].totalShipments) {
                    splitByDCZone[dcZone].totalShipments = splitByDCZone[dcZone].totalShipments + 1;
                } else {
                    if (splitByDCZone[dcZone]) {
                        splitByDCZone[dcZone].totalShipments = 1;
                    } else {
                        splitByDCZone[dcZone] = {};
                        splitByDCZone[dcZone].totalShipments = 1;
                    }


                }

            });
            callback();
        });
    }

    //This function gives the total cost , shipping cost and total number of units , split by DC  Zone Id
    function splitByDCZoneCostAndCount(callback) {
        connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: {
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "dc": "$dc", 
                    "zone": "$zoneId",
                },
                callToMCSE: {$sum: 1},
                sumOfUnits: {$sum: "$noOfUnitsInBox"},
                totalFulfillmentCOst: {$sum: "$totalCostOfBox"},
                totalShippingCost: {$sum: "$boxShippingCost"},
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcZone = item._id.dc + "~~" + item._id.zone;
                if (splitByDCZone[dcZone] && splitByDCZone[dcZone].totalUnits) {
                    splitByDCZone[dcZone].totalUnits = splitByDCZone[dcZone].totalUnits + item.sumOfUnits;
                } else {
                    if (splitByDCZone[dcZone]) {
                        splitByDCZone[dcZone].totalUnits = item.sumOfUnits;
                    } else {
                        splitByDCZone[dcZone] = {};
                        splitByDCZone[dcZone].totalUnits = item.sumOfUnits;
                    }


                }
                if (splitByDCZone[dcZone] && splitByDCZone[dcZone].totalFulfillmentCost) {
                    splitByDCZone[dcZone].totalFulfillmentCost = splitByDCZone[dcZone].totalFulfillmentCost + item.totalFulfillmentCOst;
                } else {
                    if (splitByDCZone[dcZone]) {
                        splitByDCZone[dcZone].totalFulfillmentCost = item.totalFulfillmentCOst;
                    } else {
                        splitByDCZone[dcZone] = {};
                        splitByDCZone[dcZone].totalFulfillmentCost = item.totalFulfillmentCOst;
                    }


                }
                if (splitByDCZone[dcZone] && splitByDCZone[dcZone].totalShippingCost) {
                    splitByDCZone[dcZone].totalShippingCost = splitByDCZone[dcZone].totalShippingCost + item.totalShippingCost;
                } else {
                    if (splitByDCZone[dcZone]) {
                        splitByDCZone[dcZone].totalShippingCost = item.totalShippingCost;
                    } else {
                        splitByDCZone[dcZone] = {};
                        splitByDCZone[dcZone].totalShippingCost = item.totalShippingCost;
                    }


                }


            });
            callback();
        });

    }
    //This function gives the total box Count, split by DC  Zone Id
    function splitByDCZoneBoxCount(callback){
         connection.db.collection('callDetail').aggregate([{
            $match: condition
        }, {
            $group: {
                _id: { 
                    "grp_tranx_id": "$callDetail_groupTransactionId",
                    "dc": "$dc", 
                    "zone": "$zoneId",
                    "boxId": "$boxId"
                },
                callToMCSE: {$sum: 1}
            }
        }
        ], {
            allowDiskUse: true
        }).toArray(function (err, docs) {
            docs.forEach(function (item) {

                var dcZone = item._id.dc + "~~" + item._id.zone;
                if (splitByDCZone[dcZone] && splitByDCZone[dcZone].boxCount) {
                    splitByDCZone[dcZone].boxCount = splitByDCZone[dcZone].boxCount + item.callToMCSE;
                } else {
                    if (splitByDCZone[dcZone]) {
                        splitByDCZone[dcZone].boxCount = item.callToMCSE;
                    } else {
                        splitByDCZone[dcZone] = {};
                        splitByDCZone[dcZone].boxCount = item.callToMCSE;
                    }


                }
               
            });
            callback();
        });

    }
}




