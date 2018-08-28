			var MongoClient = require('mongodb').MongoClient;
			var sleep = require('sleep'); 


			console.log("Hello World") ;

			var MongoClient = require('mongodb').MongoClient;
			var count = 0;
			MongoClient.connect('mongodb://mcse:mcsew@10.65.164.37:27017/mcse-trace', function(err, db) {
			    if (err) {
			        console.error(err);
			    }
			   console.log("Connected :)");
			   var collection = db.collection('triplet_7_4').find({"createdAt": {$gt: new Date('2017-04-07T05:15:00.00-07:00'), $lt: new Date('2017-04-07T05:59:59.59-07:00')}});
			   collection.each(function(err, doc){
	               if(!err && doc!=null && doc != undefined){
	               	  count++;
			   	      if(count % 300 ==0){
			   	   	     console.log("###########waiting##############" + count);
			   	   	     sleep.sleep(2);
			   	   	     setTimeout(callBackFunc1(doc ,db),2000);
			   	      }else{
			   	   	     callBackFunc1(doc ,db);
			   	   }
	               }
			   	   

			       
			       
			   });
			    
			});


			function callBackFunc1( data , db){
			   var tempTriplet = data ;
			   console.log(tempTriplet["triplet_carrierMethodId"]);
			   //Second call back
			   db.collection('cmIdcmTypeMap').findOne({"carrier_method_id":tempTriplet["triplet_carrierMethodId"]},function(err, done ) {
			          var cmType ;
			          if(err){
			          	console.log("some error occured");
			          }
			          if(done!=null){
			          	cmType = done["carrier_method_type"];
			            //console.log("Inside second call back");

		                   //Third call back
					       db.collection('shipment_7_4').findOne({"shipment_groupTransactionId" :tempTriplet["triplet_groupTransactionId"] , "shipment_distributorId" : tempTriplet["triplet_distributorId"] , "shipment_carrierMethodId":tempTriplet["triplet_carrierMethodId"],"chosen" : "Y"},function(err, dataReturn){
				                console.log("Inside third call back"  );

				                if(dataReturn !=null){
				                	   console.log("itemList" + dataReturn["itemList"]);
		                              if(dataReturn["itemList"].includes(tempTriplet["triplet_itemId"])){
		                              	console.log("triplet item found");
		                              	tempTriplet["chosenSolution"]="Y";
		                              }else{
		                              	console.log("triplet item not found");
		                              	tempTriplet["chosenSolution"]="N";
		                              }
					            }else{
					                tempTriplet["chosenSolution"] = "N"; 
					            }
					            tempTriplet["carrierType"] = cmType ;
		                      
					            db.collection("triplet_fullData").insert(tempTriplet,function(err , records){
					            	console.log("isnide fourth call back");
					            	if(err){
					            		console.log("Error happened while writing data");
					            		sleep.sleep(2);
					            	}else{
					            		console.log("yahoo ! , started writing");
					            	}
					            });
					       });
				      




			          }


			      


			          
			         
			       });
			}