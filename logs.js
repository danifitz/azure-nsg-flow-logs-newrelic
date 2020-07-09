/**
* This is an azure function to collect logs data from Blob storage and send it to New Relic logs API.
*    Author: Amine Benzaied
*    Team: Expert Services
*/


var https = require('https');
var url = require('url');
var zlib = require('zlib');

//
// Testing (DELETE)
//
var fs = require('fs');

const VERSION = '1.0.0';

// Global constants
const NR_LICENSE_KEY = process.env.NR_LICENSE_KEY;
const NR_INSERT_KEY = process.env.NR_INSERT_KEY;
const NR_ENDPOINT = process.env.NR_ENDPOINT || 'https://log-api.newrelic.com/log/v1';
const NR_TAGS = process.env.NR_TAGS; //Semicolon-seperated tags
const NR_LOGS_SOURCE = 'azure';
const NR_MAX_PAYLOAD_SIZE = 1000 * 1024;
const NR_MAX_RETRIES = process.env.NR_MAX_RETRIES || 3;
const NR_RETRY_INTERVAL =  process.env.NR_RETRY_INTERVAL || 2000; // default: 2 seconds

module.exports = async function (context, myBlob) {
    if (!NR_LICENSE_KEY && !NR_INSERT_KEY) {
        context.log.error(
            'You have to configure either your LICENSE key or insights insert key. Please follow the instructions in README'
        );
        return;
    }
    let logs;
    if (typeof myBlob === 'string') {
        logs = myBlob.trim().split('\n');
    } else if (Buffer.isBuffer(myBlob)) {
        logs = myBlob.toString('utf8').trim().split('\n');
    } else {
        logs = JSON.stringify(myBlob).trim().split('\n');
    }
    let buffer = transformData(logs, context);
    if (buffer.length === 0) {
        context.log.warn('logs format is invalid');
        return;
    }
    let compressedPayload;
    let payloads = generatePayloads(buffer, context);
    for (const payload of payloads) {
        try {
            compressedPayload = await compressData(JSON.stringify(payload));
            try {
                await retryMax(httpSend, NR_MAX_RETRIES, NR_RETRY_INTERVAL, [compressedPayload, context]);
                context.log("Logs payload successfully sent to New Relic");
            } catch {
                context.log.error("Max retries reached: failed to send logs payload to New Relic");
            }
        } catch {
            context.log.error("Error during payload  compression");
        }
    }
};

function compressData(data) {
    return new Promise( (resolve,reject) => {
        zlib.gzip(data, function(e, compressedData) {
            if (!e) {
                resolve(compressedData);
            } else {
                reject({'error':e,'res':null});
            }
        });
    });
}

function generatePayloads(logs, context) {

    const common = {
        'attributes': {
            'plugin': {
                'type': NR_LOGS_SOURCE,
                'version': VERSION,
                // https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-trigger?tabs=javascript#blob-name-patterns
                // Using the context bindings we get from the blob path to parameterise the path and add those variables as common attrs
                'azureSourceTimestamp': `${context.bindingData.year}-${context.bindingData.month}-${context.bindingData.day} ${context.bindingData.hour}:${'minute'}: ${context.bindingData.minute}:00`,
                'macAddress': context.bindingData.macAddress,
                'networkSecurityGroupId': context.bindingData.networkSecurityGroupId
                },
            'azure': {
                'forwardername': context.executionContext.functionName,
                'invocationid': context.executionContext.invocationId
            },
            'tags': getTags(),
        }
    };
    let payload = [{
        'common': common,
        'logs': []
    }];
    let payloads = [];

    console.log('logs', logs.length);

    let newLogs = [];
    logs.forEach(logLine => {
        // Check if we have flows data which we need to expand
        if (logLine.hasOwnProperty('properties') && logLine.properties.hasOwnProperty('flows'))
        {
            let flows = logLine.properties.flows;
            let flowsRule = flows.rule;
            flows.forEach((flow) => {
                flow.flows[0].flowTuples.forEach((tuple) => {
                    tuple = tuple.split(',');

                    data = {
                        rule: flowsRule,
                        unixtimestamp: tuple[0],
                        srcIp: tuple[1],
                        destIp: tuple[2],
                        srcPort: tuple[3],
                        destPort: tuple[4],
                        protocol: tuple[5],
                        trafficflow: tuple[6],
                        traffic: tuple[7],
                        flowstate: tuple[8],
                        packetsSourceToDest: parseInt(tuple[9]) || 0,
                        bytesSentSourceToDest: parseInt(tuple[10]) || 0,
                        packetsDestToSource: parseInt(tuple[11]) || 0,
                        bytesSentDestToSource: parseInt(tuple[12]) || 0
                    };

                    // Check if we have an existing log line which matches everything but the srcPort and the packets/bytes data
                    let previousLog = newLogs.find(element => {
                        return element.unixtimestamp === data.unixtimestamp &&
                            element.srcIp === data.srcIp &&
                            element.destIp === data.destIp &&
                            element.destPort === data.destPort &&
                            element.protocol === data.protocol &&
                            element.trafficflow === data.trafficflow &&
                            element.traffic === data.traffic &&
                            element.flowstate === data.flowstate
                    });

                    // Update the last log line with new data
                    if (previousLog) {
                        previousLog.packetsSourceToDest += data.packetsSourceToDest;
                        previousLog.bytesSentSourceToDest += data.bytesSentSourceToDest;
                        previousLog.packetsDestToSource += data.packetsDestToSource;
                        previousLog.bytesSentDestToSource += data.bytesSentDestToSource;
                    } else {
                        // Create a new logLine
                        logNew = {...logLine, ...data};

                        // Delete the old data and push to logs
                        delete logNew.properties;
                        newLogs.push(logNew);
                    }
                });
            });
        } else {
            newLogs.push(logLine);
        }
    });

    // One more pass to remove the srcPort data
    newLogs.forEach(logLine => {
        if (logLine.hasOwnProperty('srcPort')) {
            delete logLine.srcPort;
        }
    })

    console.log('newLogs', newLogs.length);

    // Send data to New Relic
    newLogs.forEach(logLine => {
        const log = addMetadata(logLine);

        // Split up payloads
        if ((JSON.stringify(payload).length + JSON.stringify(log).length) < NR_MAX_PAYLOAD_SIZE) {
            payload[0].logs.push(log);
        } else {
            payloads.push(payload);
            payload = [{
                'common': common,
                'logs': []
            }];
            payload[0].logs.push(log);
        }
    });
    payloads.push(payload);
    return payloads;
};

function getTags() {
    const tagsObj = {};
    if (NR_TAGS) {
        const tags = NR_TAGS.split(';');
        tags.forEach(tag => {
        keyValue = tag.split(':');
        if (keyValue.length > 1) {
            tagsObj[keyValue[0]] = keyValue[1];
        }
        });
    };
    return tagsObj;
};

function addMetadata(logEntry) {
    if (
        logEntry.resourceId !== undefined &&
        typeof logEntry.resourceId === 'string' &&
        logEntry.resourceId.toLowerCase().startsWith('/subscriptions/')
    ) {
        let resourceId = logEntry.resourceId.toLowerCase().split('/');
        if (resourceId.length > 2) {
            logEntry['metadata'] = {};
            logEntry['metadata']['subscriptionId'] = resourceId[2];
        }
        if (resourceId.length > 4) {
            logEntry['metadata']['resourceGroup'] = resourceId[4];
        }
        if (resourceId.length > 6 && resourceId[6]) {
            logEntry['metadata']['source'] = resourceId[6].replace('microsoft.', 'azure.');
        }
    }
    return logEntry;
};

function transformData(logs, context) {

    // buffer is an array of JSON objects
    let buffer = [];

    let parsedLogs = parseData(logs, context);

    if (!Array.isArray(parsedLogs) && typeof parsedLogs === 'object' && parsedLogs !== null) { // type JSON object
        if (parsedLogs.records !== undefined) {
            context.log("Type of logs: records Object");
            parsedLogs.records.forEach(log => buffer.push(log));
            return buffer;
        } else {
            context.log("Type of logs: JSON Object");
            buffer.push(parsedLogs);
            return buffer;
        }
    }
    if (!Array.isArray(parsedLogs)) { // Bad Format
        return buffer;
    }
    if (typeof parsedLogs[0] === 'object') {
        if (parsedLogs[0].records !== undefined) { // type JSON records
            context.log("Type of logs: records Array");
            parsedLogs.forEach(message => {
                message.records.forEach(log => buffer.push(log));
            });
            return buffer;
        } else { // type JSON array
            context.log("Type of logs: JSON Array");
            parsedLogs.forEach(log => buffer.push({ message: log })); // normally should be "buffer.push(log)" but that will fail if the array is a mix of JSON and strings
            return buffer;                                           // Our API can parse the data in "log" to a JSON and ignore "message", so we are good!
        }
    }
    if (typeof parsedLogs[0] === 'string') { // type string array
        context.log("Type of logs: string Array");
        parsedLogs.forEach(logString => buffer.push({ message: logString }));
        return buffer;
    }
    return buffer;
};

function parseData(logs, context) {
    let newLogs = logs;

    if (!Array.isArray(logs)) {
        try {
            newLogs = JSON.parse(logs); // for strings let's see if we can parse it into Object
        } catch {
            context.log.warn("cannot parse logs to JSON");
        }
    } else {
        newLogs = logs.map(log => {     // for arrays let's see if we can parse it into array of Objects
            try {
                return JSON.parse(log);
            } catch {
                return log;
            }
        });
    }
    return newLogs;
};

function httpSend(data, context) {

    return new Promise( (resolve,reject) => {
        const urlObj = url.parse(NR_ENDPOINT);
        const options = {
            hostname: urlObj.hostname,
            port: 443,
            path: urlObj.pathname,
            protocol: urlObj.protocol,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Encoding': 'gzip'
            }
        }

        NR_LICENSE_KEY ? options.headers["X-License-Key"] = NR_LICENSE_KEY : options.headers["X-Insert-Key"] = NR_INSERT_KEY

        var req = https.request(options, function (res) {
            var body = '';
            res.setEncoding('utf8');
            res.on('data', function (chunk) {
                body += chunk; // don't really do anything with body
            });
            res.on('end', function () {
                context.log('Got response:'+res.statusCode);
                if (res.statusCode === 202) {
                    resolve(body);
                } else {
                    reject({'error':null,'res':res});
                }
            });
        });

        req.on('error', function (e) {
            reject({'error':e,'res':null});
        });
        req.write(data);
        req.end();
    });
};

/**
 * Retry with Promise
 * fn: the function to try
 * retry: the number of retries
 * interval: the interval in millisecs between retries
 * fnParams: list of params to pass to the function
 * @returns A promise that resolves to the final result
 */

function retryMax(fn,retry,interval,fnParams) {
    return fn.apply(this,fnParams).catch( err => {
        return (retry>1? wait(interval).then(()=> retryMax(fn,retry-1,interval, fnParams)):Promise.reject(err));
    });
};

function wait(delay) {
    return new Promise((fulfill,reject)=> {
        setTimeout(fulfill,delay||0);
    });
};

//
// Testing (DELETE)
//
let context = {
    log: (message) => { console.log(message) },
    bindings: {
        year: 1000,
        day: 10,
        hour: 10,
        minute: 10,
        macAddress: 'aa-bb-cc-dd-ee',
        networkSecurityGroupId: 'security-group',
    },
    executionContext: {
        functionName: 'functionName',
        invocationId: 'invocationId'
    }
}
fs.readFile('PT1H.json', function(err, data){
    module.exports(context, data.toString())
});
