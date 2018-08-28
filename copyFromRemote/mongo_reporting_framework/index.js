var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var path = require("path");


var reqestHandler = require('./controller/reqHandler');

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

app.use(express.static(__dirname + '/views'));
var port = process.env.PORT || 3000;

//Adding a middleware to do the request body validation
app.use('/api/report', function (req, res, next) {

    var validRequest = true ;

    if (!req.body) {
        res.status(400).end("request body can't be empty");
        validRequest = false ;
        return;
    }
    //Validation for the start date
    if (!req.body.startDate) {
        res.status(400).end("startDate can't be empty");
        validRequest = false ;
        return;
    }

    if (!req.body.startDate.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-07:00$')) {
        res.status(400).end("incorrect start date format, try something like -> 2017-04-11T00:00:00-07:00 in PST time zone");
        validRequest = false ;
        return;
    }

    //validation for the end date
    if (!req.body.endDate) {
        res.status(400).end("endDate can't be empty");
        validRequest = false ;
        return;
    }

    if (!req.body.endDate.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-07:00$')) {
        res.status(400).end("incorrect end Date format, try something like -> 2017-04-11T00:00:00-07:00 in PST time zone ");
        validRequest = false ;
        return;
    }

    //Validation for the difference between start date and end date
    var startDate = new Date(req.body.startDate);
    var endDate = new Date(req.body.endDate);

    var hours = (endDate - startDate) / 36e5;

    if (hours < 0) {
        res.status(400).end("end date is before the start date");
        validRequest = false ;
        return;
    }
    if (hours > 24) {
        res.status(400).end("currently we are supporting only max of 24 hours of data. Please reduce the time range");
        validRequest = false ;
        return;
    }

    if (!req.body.env) {
        res.status(400).end("env sent is empty");
        validRequest = false ;
        return;
    }


    if (!req.body.emailIds) {
        res.status(400).end("email Ids list passed is empty");
        validRequest = false ;
        return;
    }

    //validation for the setOfMatrices
    if (!req.body.setOfMatrics) {
        validRequest = false ;
        res.status(400).end("set of matrices sent is empty, please pass the matrices in comma separated format .  Ex -> overallResult,splitBySLATier,splitByDCType,splitByCMType,splitByDC,splitByCM,splitByDCDestState");

    }
/*
    var matricesListAvailable =["overallResult","splitBySLATier","splitByDCType","splitByCMType","splitByDC", "splitByCM","splitByDCDestState"];
    var matrices = req.body.setOfMatrics.split(",");
    matrices.forEach(function(value){
        if(!matricesListAvailable.includes(value)){
            res.status(400).end(value + " matric passed is not supported.Please pass the matrices among the following : overallResult,splitBySLATier,splitByDCType,splitByCMType,splitByDC,splitByCM,splitByDCDestState");
            validRequest = false ;
            return;

        }
    }); */

    //validation for the emailIds
    var emails = req.body.emailIds.split(",");
    emails.forEach(function (value) {
        var email = value.trim();
        if (!email.match(/^\w+@[a-zA-Z_]+?\.[a-zA-Z]{2,3}$/)) {
            validRequest = false ;
            res.status(400).end("email Ids list passed contains invalid email Id -> " + email);

        }
    });

    //setting the startDate and endDate in the request
    req.startDate = startDate;
    req.endDate = endDate;
    if(validRequest){
        next();
    }

});

app.use(function (err, req, res, next) {
    console.log("Error in server : " + err);
    res.status(500).end('Internal server error');
})

app.post('/api/report', function (req, res) {
    console.log(req.startDate);
    reqestHandler.handleRequest(req);
    res.end('Thanks for the request. Processing has started ... Once completed the report will be sent to the emailIds -> ' + req.body.emailIds);
});

app.get('/mcse/reporting', function (req, res) {
    res.sendFile(path.join(__dirname + '/views/landing.html'));
});


//Starting the server
app.listen(port, function () {
    console.log("Started the server on the port  : " + port);
})

