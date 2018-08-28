		var fs = require('fs');
		var parse = require('csv-parse');
		var async = require('async');
		var sleep = require('sleep');
		var Client = require('node-rest-client').Client;
	    var client = new Client();

		var inputFile="/Users/vmohan/Documents/s2sItem.csv";
		
		var csvData=['35710952' ,
'43534218',
'075751' ,
'46108294', 
'46477361', 
'48205820' ,
'50282529' ,
'50259740' ,
'50259742' ,
'52114770' ,
'54593647' ,
'55171406' ,
'55277682' ,
'55278837' ,
'55357887' ,
'55370183' ,
'55497763' ,
'55507087' ,
'55527771' ,
'55541695' ,
'104551769', 
'623413660' ,
'195419242' ,
'192432961' ,
'193520462' ,
'193536608' ,
'804305973' ,
'741261362' ,
'665492270',
'927440931' ,
'922148944' ,
'953209965' ]
;
		fs.createReadStream(inputFile)
		    .pipe(parse({delimiter: ','}))
		    .on('data', function(csvrow) {
		        //console.log(csvrow[0]);
		        //do something with csvrow
		        //csvData.push(csvrow[0]);        
		    })
		    .on('end',function() {
		    	console.log("Inside the end");
		      //do something wiht csvData
		      console.log(csvData.length);
		      
		      async.each(csvData , function(item , callback){
		      	//console.log(item);


	     var body ={       "payload": {
	    "id": "12345",
	    "promiseList": [
	      {
	        "id": "150",
	        "promiseDetails": {
	          "messageID": "messageIdS2S0001",
	          "orderID":"-1",
	          "cacheBased": true,
	          "fulfillmentDetails": {
	            "fulfillmentOption": "S2S",
	            "shipmentMethod": "STORE_DELIVERY",
	            "shippingAddress": {
	              "addressType": "RESIDENTIAL",
	              "postalCode": 94086,
	              "stateOrProvinceCode": "CA",
	              "id": null,
	              "isApoFpo": false,
	              "isPoBox": false
	            } ,"store":{  
	                     "USStoreId":"3455"
	                  }
	          },"promiseLines": [
	            {
	               "offerID": {
	                "upc": "",
	                "USItemId": item,
	                "USSellerId": 0,
	                "legacyItemId": item,
	                "legacySellerId": "0"
	              },
	              "lineId": "LineIdQAS2S-777001",
	              "shippingSLATier": "TWO_DAY",
	              "holidayID": "",
	              "bundleID": "",
	              "siteTypeID": "",
	              "quantity": {
	                "unitOfMeasure": "EA",
	                "measurementValue": 1
	              },
	              "productClass": "STANDARD",
	              "shipAlone": true,
	              "reserveLine": false,
	              "allowPartialLineReservation": false,
	              "resourcingFlag": false
	            
	            }
	          ]
	        }
	      }
	    ]
	  }
	}
		 
			var url = "http://router.prod.mcse-cluster.services.prod.walmart.com/mosaic-ims-lite-router/services/v1/sourcing/fetch";
			var args = {
		               data: body,
		               headers:{ "WM_SVC.VERSION" :"1.0.0",
		                       "WM_SVC.ENV": "prod-oms",
		                       "WM_SVC.NAME":"mosaic-ims-service",
		                       "WM_CONSUMER.ID": "ea2ec678-841d-46d1-8935-9610b1045387",
		                       "WM_CONSUMER.SOURCE_ID":"OMS",
		                       "WM_QOS.CORRELATION_ID":"2323",
		                       "WM_BU_ID": "0",
		                       "Content-Type":"application/json" },
		              requestConfig: {
		                              timeout: 4000, //request timeout in milliseconds 
		                              noDelay: true, //Enable/disable the Nagle algorithm 
		                              keepAlive: true, //Enable/disable keep-alive functionalityidle socket. 
		                              keepAliveDelay: 1000 //and optionally set the initial delay before the first keepalive probe is sent 
		                             },
		              responseConfig: {
		                               timeout: 7000 //response timeout 
		                              }
		              };
			var payload ;
		    var req =client.post(url, args, function (data, response) {
		    	if(data != null && data.payload !=null){
		    		if(data.payload.promiseList[0].promiseDetails.promiseLines[0].status != "FAIL"){
		    	       console.log( item + " --> " + data.payload.promiseList[0].promiseDetails.promiseLines[0].status);
		    	    }else{
		    	       console.log( csvData[count] + " --> " + data.payload.promiseList[0].promiseDetails.promiseLines[0].error.field );
		    	    }
		    	    
		    	    
		    	}
		    	callback();
		    });	

		    req.on('error', function (err) {
	         console.log('request error');
	         callback();
		  });



		      	
		      });
		              
		    });


