var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');
var Client = require('node-rest-client').Client;
 
var client = new Client();

var inputFile='item_leadtimes2.csv';


var csvData=[];
fs.createReadStream(inputFile)
    .pipe(parse({delimiter: ','}))
    .on('data', function(csvrow) {
        console.log(csvrow);
        console.log(csvrow[0]);
        
          var body ={ "header":null,
              "payload" : {"itemNodeDefinitionList": [
            {          
                "isTransformationRequired": false,
                "itemNodeDefinitionDetails": {
                    "offerID": {
                        "USItemId": csvrow[0],
                        "USSellerId": csvrow[1]
                    },
                    "shipNode": csvrow[2],
                    "unitOfMeasure": "EA",
                    "maxDaysToShip": csvrow[3],
                    "minDaysToShip": csvrow[4]
                }
            }
        ]
       }
                    };
    
       var args = {
         data: body,
         headers: { "Content-Type": "application/json" ,
                    "WM_SVC.VERSION": "1.0.0",
                    "WM_SVC.ENV": "prod",
                    "WM_SVC.NAME": "mosaic-ims-service",
                    "WM_CONSUMER.ID": "1",
                    "WM_TENANT_ID": "WALMART.COM",
                    "WM_QOS.CORRELATION_ID": "2323",
                  }
         };
    
       var req =client.post("http://ui.ims.us.prod.walmart.com/mosaic-ims-app/services/v1.0/inventory/item/itemnodedefinition", args, function (data, response) {
          // parsed response body as js object 
          console.log(data);
    
       });
        
       req.on('requestTimeout', function (req) {
    console.log('request has expired');
    req.abort();
});
 
req.on('responseTimeout', function (res) {
    console.log('response has expired');
 
});
 
//it's usefull to handle request errors to avoid, for example, socket hang up errors on request timeouts 
req.on('error', function (err) {
    console.log('request error', err);
});
       console.log(body);
        //do something with csvrow
        csvData.push(csvrow);        
    })
    .on('end',function() {
      //do something wiht csvData
      console.log(csvData);
    });
