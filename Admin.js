
const {kafka} = require('./client')

async function init() {
    const admin = kafka.admin();
    console.log("Admin connecting");
    admin.connect();
    console.log("Admin connection Success ...");
    
    console.log("Creating Topics");
    await admin.createTopics({
        topics :[{
            topic:"rider-updates",
            numPartitions:2,
            replicationFactor:1,
        }],
    });
    console.log("Topic created successfully");

    console.log("Disconnecting Admin");
    await admin.disconnect();
}

init();