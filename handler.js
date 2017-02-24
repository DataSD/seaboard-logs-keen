// S3 logs to Keen IO

var AWS = require('aws-sdk');
var https = require('https');
var Keen = require('keen-js');

console.log("Version 0.1.0");

var config = {
         "keen_project_id": "example",
         "keen_write_key": "example"
}



var s3 = new AWS.S3();

var kClient = new Keen({
    "projectId": "58b083098db53dfda8a88bcf",
    "writeKey": "2983826826B0D221CC34E08C7831B29D6E5F47DAF3C119462DA0942BA31391D03B9651CFB5EEE7CD8171489FF09CD159E0BE0E1F8CBA49E3813F9D7EDA7931C63B72120E78AD753244EB57925D3E78ED7A8EC3B15215A3568BD33E16F93D5E77"});

exports.send_logs = function(event, context) {
    console.log(JSON.stringify(event));

    for (var i = 0; i < event.Records.length; i++) {
        var srcBucket = event.Records[i].s3.bucket.name;
        var srcKey = unescape(event.Records[i].s3.object.key);

        s3.getObject({ Bucket: srcBucket, Key: srcKey}, function(error, data) {
            if (error !== null) {
                console.log('error');
                context.done(error);
            } else {
                console.log('parsing');
                parsedLogs = parse(data.Body.toString());
                for (var l = 0; l < parsedLogs.length; l++) {
                    console.log(parsedLogs[l]);
                    kClient.addEvent('s3log_events', parsedLogs[l], function(err, res) {
                        context.done(err, res);
                    });
                }
            }

        });
    }
};

// Source https://github.com/ifit/s3-log-parser
function parse(log) {
    var logs = log.split('\n')
    , parsedLogs = []
    , bracketRegEx = /\[(.*?)\]/
    , quoteRegex = /\"(.*?)\"/
    ;


    for(var i = 0; i < logs.length; i++) {
        var logString = logs[i];
        if(logString.length === 0)
            continue;
        var time = bracketRegEx.exec(logString)[1];
        time = time.replace(/\//g, ' ');
        time = time.replace(/:/, ' ');
        time = new Date(time);
        logString = logString.replace(bracketRegEx, '');

        var requestUri = quoteRegex.exec(logString)[1];
        logString = logString.replace(quoteRegex, '');

        var referrer = quoteRegex.exec(logString)[1];
        logString = logString.replace(quoteRegex, '');

        var userAgent = quoteRegex.exec(logString)[1];
        logString = logString.replace(quoteRegex, '');

        var logStringSplit = logString.split(' ')
        , bucketOwner    = logStringSplit[0]
        , bucket         = logStringSplit[1]
        , remoteIp       = logStringSplit[3]
        , requestor      = logStringSplit[4]
        , requestId      = logStringSplit[5]
        , operation      = logStringSplit[6]
        , key            = logStringSplit[7]
        , statusCode     = logStringSplit[9]
        , errorCode      = logStringSplit[10]
        , bytesSent      = logStringSplit[11]
        , objectSize     = logStringSplit[12]
        , totalTime      = logStringSplit[13]
        , turnAroundTime = logStringSplit[14]
        , ctime          = logStringSplit[17]
        ;

        var event = {
            "bucket_owner":          bucketOwner,
            "bucket":                bucket,
            "ip":                    remoteIp,
            "requestor_id":          requestor,
            "request_id":            requestId,
            "operation":             operation,
            "key":                   key,
            "http_method_uri_proto": requestUri,
            "http_status":           (statusCode == '-' ? 0 : parseInt(statusCode, 10)),
            "s3_error":              errorCode,
            "bytes_sent":            (bytesSent == '-' ? 0 : parseInt(bytesSent, 10)),
            "object_size":           (objectSize == '-' ? 0 : parseInt(objectSize, 10)),
            "total_time":            (totalTime == '-' ? 0 : parseInt(totalTime, 10)),
            "turn_around_time":      (turnAroundTime == '-' ? 0 : parseInt(turnAroundTime, 10)),
            "referer":               referrer,
            "user_agent":            userAgent,
            "keen":                 { "timestamp": time }
        }

        parsedLogs.push(event);
    }

    return parsedLogs;
}
