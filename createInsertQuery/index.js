/**
 * Created by vmohan on 23/02/18.
 */
var xlsx = require('node-xlsx');

var obj = xlsx.parse(__dirname + '/default Carrier method v3_021518.xlsx');

var obj1 = xlsx.parse(__dirname + '/ids.xlsx');

//console.log(JSON.stringify((obj[0]).data));

//console.log(((obj[0]).data).length)


var arr = [20,31,67,501,9,22,79,80,78,97];

var map = {'T' : 'TINY' , 'S' : 'SMALL' ,'M' : 'MEDIUM'}

var idArray = obj1[0].data ;
//console.log((idArray[0])[0]);


var idVar =0 ;
for( var i =1;i<((obj[0]).data).length ;i++){

    if((((obj[0]).data)[i])[0] !=null && arr.includes((((obj[0]).data)[i])[0])){

        //console.log(((obj[0]).data)[i])

        var addressIds = [] ;
        if(((((obj[0]).data)[i])[5].toString()).includes(",")){
            addressIds = (((obj[0]).data)[i])[5].split(",");
        }else{
            addressIds.push((((obj[0]).data)[i])[5]);
        }

        //console.log("addresIDs "+addressIds);

        var itemSize = (((obj[0]).data)[i])[4].split(",");
        //console.log("itemSizes" +itemSize);

        var itemClass = [];
        if(((((obj[0]).data)[i])[6].toString()).includes(",")){
            itemClass = (((obj[0]).data)[i])[6].split(",");
        }else{
            itemClass.push((((obj[0]).data)[i])[6]);
        }

       // console.log("itemClass"+itemClass);

        console.log("queries for CM ID : " + (((obj[0]).data)[i])[0]);
        console.log("================================================");
        var totalQueries =0;
        for( var a =0;a <addressIds.length;a++){
            for(var b=0;b<itemSize.length;b++){
                for(var c=0;c<itemClass.length;c++){

                    totalQueries = totalQueries +1 ;

                    var query = "insert into DCS_DEFAULT_CM_TEMPLATE(DCS_DEFAULT_CM_TEMPLATE_PK , CARRIER_METHOD_ID, ADDRESS_TYPE ,SHIPPING_METHOD_ID,SHIP_CLASS,SHIP_SIZE_CODE,IS_ACTIVE,HOLIDAY_UPGRADE,CREATED_DATE,CREATED_BY,MODIFIED_DATE,MODIFIED_BY,DB_LOCK_VERSION)"+

                        //"values('3646e52e-f3f0-46ac-afea-815764734aaf','"+(((obj[0]).data)[i])[0]+"','"+1+"','"+1+"','"+1+"','"+TINY+"','"+1+"','"+0+"',sysdate,dc-square,sysdate,dc-square,0)";

                    "values('"+(idArray[idVar])[0]+"','"+(((obj[0]).data)[i])[0]+"','"+addressIds[a]+"','"+(((obj[0]).data)[i])[3]+"','"+itemClass[c]+"','"+map[itemSize[b]]+"','"+1+"','"+0+"',sysdate,'dc-square',sysdate,'dc-square',0)";

                    console.log(query);
                    console.log("");
                    idVar = idVar +1 ;


                }

            }
        }

        console.log("total queries : "+totalQueries);
        console.log("");





    }



}

console.log("Total inserts : "+ idVar);
