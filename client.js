const {Kafka} = require("kafkajs");

exports.kafka = new Kafka({
    clientId :"stock-market",
    brokers : ["localhost:9092"]
});