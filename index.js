var aws = require('aws-sdk');
var s3 = new aws.S3({apiVersion: '2006-03-01'});
var constants = require("constants");
var request = require("request");
var csv = require("csv");
var config = require("./config.json");

exports.handler = function(event, context) {
  var bucket = event.Records[0].s3.bucket.name;
  var key = event.Records[0].s3.object.key;

  if (!/\.csv$/g.test(key)) return;

  s3.getObject({Bucket: bucket, Key: key}, function(err, data) {
    if (err) {
      console.log("Error getting object " + key + " from bucket " + bucket +
        ". Make sure they exist and your bucket is in the same region as this function.");
      context.fail('Error', "Error getting file: " + err);
    } else {
      csv.parse(new Buffer(data.Body, "base64").toString("utf8"), {}, function(err, rows) {
        if (!err) {
          var nameIndex = rows[0].indexOf("ProductName");
          var costIndex = rows[0].indexOf("TotalCost");
          var currencyIndex = rows[0].indexOf("CurrencyCode");
          var records = rows.slice(1, -1);
          var costsByService = {};

          for (var index in records) {
            var record = records[index];
            if (!record[nameIndex].length) continue;

            if (!costsByService[record[nameIndex]]) {
              costsByService[record[nameIndex]] = {};
            }
            if (!costsByService[record[nameIndex]][record[currencyIndex]]) {
              costsByService[record[nameIndex]][record[currencyIndex]] = 0;
            }
            costsByService[record[nameIndex]][record[currencyIndex]] += Number(record[costIndex]);
          };

          var payload = {
            attachments: [
              {
                color: "warning",
                fields: []
              }
            ]
          };

          if (config.slack.username && config.slack.username.length) {
            payload.username = config.slack.username;
          }
          if (config.slack.channel && config.slack.channel.length) {
            payload.channel = config.slack.channel;
          }
          if (config.slack.icon_emoji && config.slack.icon_emoji.length) {
            payload.icon_emoji = config.slack.icon_emoji;
          }

          for (var key in costsByService) {
            var field = {
              title: key,
              value: "",
              short: true
            };
            for (var currency in costsByService[key]) {
              if (field.value.length) field.value += "\n";

              switch(currency) {
                case "USD":
                  field.value += "$ " + costsByService[key][currency];
                  break;
                default:
                  field.value += currency + " " + costsByService[key][currency];
                  break;
              }
            }
            payload.attachments[0].fields.push(field);
          }

          console.log(JSON.stringify(payload));
          request({
            url: config.slack.webhook_url,
            method: "POST",
            json: true,
            form: JSON.stringify(payload)
          }, function(error, response, body) {
            if (!error && response.statusCode == 200) {
              console.log(body);
              context.succeed();
              console.log("Status: " + response.statusCode);
              context.succeed();
            } else {
              context.fail('Error', "Error Slack: " + error);
            }
          });
        } else {
          console.log("err: " + err);
        }
      });
    }
  });
};
