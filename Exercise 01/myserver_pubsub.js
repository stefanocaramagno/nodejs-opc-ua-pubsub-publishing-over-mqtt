const opcua = require("node-opcua");
const pubsubexpander = require("node-opcua-pubsub-expander");
const pubsubserver = require("node-opcua-pubsub-server");
const opcuatype = require("node-opcua-types");

const userManager = {
  isValidUser: function(userName, password) {
    if (userName === "user1" && password === "pas1") {
        return true;
    }
    if (userName === "user2" && password === "pas2") {
        return true;
    }
    return false;
  }
};


(async()=>{
    try {

        const server = new opcua.OPCUAServer({
            port: 4334, 
            userManager,
            allowAnonymous: false,
            resourcePath: "/UA/MyLittlePubSubServer",
        });

        await server.initialize();
       
        const addressSpace = server.engine.addressSpace
  
        const namespace = addressSpace.getOwnNamespace(); 

        var objectFolder = namespace.addFolder("ObjectsFolder", { 
            browseName: "MySensors"
        });
      
        var temperatureSensorType = namespace.addObjectType({
            browseName: "TemperatureSensorType"
        });

        var temperature1= namespace.addVariable({
            componentOf: temperatureSensorType,
            browseName: 'Temperature1',
            dataType: opcua.DataType.Double,
            modellingRule: 'Mandatory',
        });

        var temperature2 = namespace.addVariable({
            componentOf: temperatureSensorType,
            browseName: 'Temperature2',
            dataType: opcua.DataType.Double,
            modellingRule: 'Mandatory',
        });
  
        var temperatureSensor = temperatureSensorType.instantiate({
            browseName: "MyTemperatureSensor",
            organizedBy: objectFolder,
        });

        setInterval(() => {
            const value = 10 + 5 * Math.sin(Date.now() / 10000) + Math.random() * 0.3;
            temperatureSensor.temperature1.setValueFromSource({ 
                dataType: opcua.DataType.Double, 
                value });
        }, 100);

        setInterval(() => {
            const value = 19 + 5 * Math.sin(Date.now() / 10000) + Math.random() * 0.2;
            temperatureSensor.temperature2.setValueFromSource({ 
                dataType: opcua.DataType.Double, 
                value });
        }, 100);
       
        const configuration = getPubSubConfiguration(temperatureSensor);

        console.log(configuration.toString());

        await pubsubserver.installPubSub(server, {
            configuration,
        });

        await server.start();
        
        console.log("server started at ", server.getEndpointUrl());
        
    } catch(err) {
        console.log(err);
        process.exit(1);
    }
})();


function getPubSubConfiguration(temperatureSensor) {
  const connection = createConnection();

  const publishedDataSet = createPublishedDataSet(temperatureSensor);

  return new opcuatype.PubSubConfigurationDataType({
    connections: [connection],
    publishedDataSets: [publishedDataSet] });
}

function createConnection() {

    const mqttEndpoint = "mqtt:broker.emqx.io";

    const dataSetWriter = {
        dataSetFieldContentMask: opcuatype.DataSetFieldContentMask.None,
        dataSetName: "PublishedDataSet1",
        dataSetWriterId: 1,
        enabled: true,
        name: "dataSetWriter1",
        messageSettings: {
            dataSetMessageContentMask:
                opcuatype.JsonDataSetMessageContentMask.DataSetWriterId |
                opcuatype.JsonDataSetMessageContentMask.MetaDataVersion,
        },
        transportSettings: {
            queueName: "temperature-sensors",
        },
    };
 
    const writerGroup = {
        dataSetWriters: [dataSetWriter],
        enabled: true,
        publishingInterval: 1000,
        name: "WriterGroup1",
        messageSettings: {
            networkMessageContentMask: opcuatype.JsonNetworkMessageContentMask.PublisherId,
        },
        transportSettings: {
            requestedDeliveryGuarantee: opcuatype.BrokerTransportQualityOfService.AtMostOnce,
        },
    };

    const connection = new pubsubexpander.MyMqttJsonPubSubConnectionDataType({
        enabled: true,
        name: "Connection1",
        transportProfileUri: pubsubexpander.Transport.MQTT_JSON,
        address: {
            url: mqttEndpoint,
        },
        writerGroups: [writerGroup],
        readerGroups: []
    });
    return connection;
}

function createPublishedDataSet(temperatureSensor) {
    const publishedDataSet = {
        name: "PublishedDataSet1",
        dataSetMetaData: {
            fields: [
                {
                    name: "Sensor.Temperature1",
                    builtInType: opcua.DataType.Double,
                    dataType: opcua.resolveNodeId("Double"),
                },
                {
                    name: "Sensor.Temperature2",
                    builtInType: opcua.DataType.Double,
                    dataType: opcua.resolveNodeId("Double"),
                },

            ],
        },
        dataSetSource: new opcuatype.PublishedDataItemsDataType({
            publishedData: [
                {
                    attributeId: opcua.AttributeIds.Value,
                    samplingIntervalHint: 100,
                    publishedVariable: temperatureSensor.temperature1.nodeId,
                },
                {
                    attributeId: opcua.AttributeIds.Value,
                    samplingIntervalHint: 100,
                    publishedVariable: temperatureSensor.temperature2.nodeId,
                },
            ],
        }),
    };
    return publishedDataSet;
}

