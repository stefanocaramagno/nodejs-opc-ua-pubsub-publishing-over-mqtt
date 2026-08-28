/**
 * Estenderemo un server OPC-UA esistente. 
 * Lo trasformeremo quindi in un publisher Pub/Sub che trasmette 
 * in streaming i valori dei suoi sensori a un broker MQTT esterno. 
 * Uno scenario in cui i dispositivi IoT edge devono pubblicare dati 
 * sul cloud per l'analisi.
 */

/**
 * Lo stack node-opcua è reso disponibile 
 * all'applicazione dall'istruzione 'require'.
 */
const opcua = require("node-opcua");
const pubsubexpander = require("node-opcua-pubsub-expander");
const pubsubserver = require("node-opcua-pubsub-server");
const opcuatype = require("node-opcua-types");

/**
 * In JavaScript esiste un metodo chiamato isValidUser.
 *
 * Questo metodo può essere utilizzato per definire l'elenco delle coppie username e password 
 * che possono essere autorizzate, quando avviene l'autenticazione da parte del client.
 *
 * Definiamoci il seguente oggetto Javascript,che utilizza il metodo isValidUser.
 */
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

        /**
         * È necessario creare un'istanza del server OPC UA. 
         * Per personalizzare il nostro server possono essere aggiunte 
         * delle opzioni che modificano il comportamento del server.
         *
         * La configurazione della porta e del resourcePath permetterà 
         * di costruire l’URI del Discovery Endpoint del nostro server:
         * - opc.tcp://<hostname>:4334/UA/MyLittlePubSubServer --> Server started at opc.tcp://MSI:4334/UA/MyLittlePubSubServer
         * - <hostname> sarà sostituito dal nome del computer o dal nome di dominio completo.
         * 
         * Nella creazione dell'istanza del server, è possibile negare l'accesso anonimo 
         * e definire gli accessi (ad esempio con username e password).
         */
        const server = new opcua.OPCUAServer({
            port: 4334, 
            userManager,
            allowAnonymous: false,
            resourcePath: "/UA/MyLittlePubSubServer",
        });

        /**
         * Una volta creato il server deve essere inizializzato. 
         * Durante l'inizializzazione, il server caricherà il suo set di nodi predefinito 
         * e preparerà l'associazione di tutte le variabili OPC UA standard.
         *
         * L'operazione di inizializzazione deve essere bloccante, 
         * in quanto nessuna altra operazione può essere fatta 
         * fino a quando il server non viene inizializzato.
         */
        await server.initialize();

        /**
         * Una volta che il server è stato inizializzato, possiamo creare
         * nuovi nodi ed associare ad essi dei valori runtime.
         *
         * Per fare questo dobbiamo seguire i seguenti passaggi.
         */

        /**
         * Passaggio 1: Accedere all’addressSpace.
         */         
        const addressSpace = server.engine.addressSpace

        /**
         * Passaggio 2: Accedere alla porzione riservata al server (Namespace index=1).
         */   
        const namespace = addressSpace.getOwnNamespace(); 

        /**
         * Una volta avuto accesso al namespace del server è possibile creare
         * tutti gli oggetti e/o tipi che si desidera, così come poi inizializzarli.
         * 
         * Nel seguito supporremo di realizzare un semplice esempio:
         * - definiamo un sottotipo di BaseObjectType, di nome "TemperatureSensorType", 
         *   composto da  DataVariable e da Proprietà (con Modelling Rules);
         * - definiamo un Folder sotto la cartella standard Objects, di nome "MySensors"
         * - istanziamo uno o più oggetti di tipo TemperatureSensorType
         *   dentro questa cartella MySensors;
         * - assegniamo dei valori agli oggetti istanziati.
         */ 

        /** 
         * Creiamo sotto lo standard Folder Objects, un Folder di nome "MySensors".
         */
        var objectFolder = namespace.addFolder("ObjectsFolder", { 
            browseName: "MySensors"
        });

        /** 
         * Creiamo nel namespace di index 1 del server, un nuovo ObjectType
         * TemperatureSensorType definito come SubType di BaseObjectType.
         * 
         * E' possibile inserire e inizializzare altri attributi.
         */        
        var temperatureSensorType = namespace.addObjectType({
            browseName: "TemperatureSensorType"
        });

        /** 
         * Arricchiamo l'ObjectType TemperatureSensorType 
         * con componenti e proprietà. 
         * 
         * Ad esempio aggiungere una proprietà (Model) 
         * e una variabile (Temperature) all'ObjectType.
         * 
         * Nota: Ricordiamoci che possiamo specificare modellingRule 
         * come "Mandatory", "Optional", "PlaceHolderMandatory", "PlaceHolderOptional". 
         * Se manca modellingRule, si assume "Mandatory".
         */

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

        /**
         * Istanziamo un nodo Object dall'ObjectType TemperatureSensorType, 
         * inserendo le istanze nella cartella MySensors creata ad hoc.
         */   
        var temperatureSensor = temperatureSensorType.instantiate({
            browseName: "MyTemperatureSensor",
            organizedBy: objectFolder,
        });

        /**
         * Richiama ripetutamente una funzione o esegue un frammento di codice, 
         * con un ritardo fisso tra ogni chiamata.
         */  
        setInterval(() => {
            const value = 10 + 5 * Math.sin(Date.now() / 10000) + Math.random() * 0.3;
            temperatureSensor.temperature1.setValueFromSource({ 
                dataType: opcua.DataType.Double, 
                value });
        }, 100);

        /**
         * Richiama ripetutamente una funzione o esegue un frammento di codice, 
         * con un ritardo fisso tra ogni chiamata.
         */  
        setInterval(() => {
            const value = 19 + 5 * Math.sin(Date.now() / 10000) + Math.random() * 0.2;
            temperatureSensor.temperature2.setValueFromSource({ 
                dataType: opcua.DataType.Double, 
                value });
            }, 100);

        /**
         * Ci sono due operazioni essenziali per trasformare il nostro server OPC UA in un publisher PubSub-ready:
         * - caricare tutti i parametri di configurazione del publisher;
         * - eseguire il metodo 'installPubSub' in modo sincrono.
         * 
         * Tutto ciò dopo 'await server.initialize();' del codice del nostro server.
        */

        /**
         * Configurazione dei parametri del publisher.
         */         
        const configuration = getPubSubConfiguration(temperatureSensor);

        console.log(configuration.toString());

        /**
         * Creazione dell'oggetto PublishSubscribe 
         * e del binding di tutti i metodi e servizi.
         */
        await pubsubserver.installPubSub(server, {
            configuration,
        });

        /**
         * Una volta che il server è stato creato e inizializzato, 
         * utilizziamo il metodo start per consentire al server 
         * di avviare tutti i suoi endpoint e iniziare ad ascoltare i client.
         * 
         * Anche questa operazione è bloccante.
         */ 
        await server.start();

        /**
         * Server started at opc.tcp://<hostname>:4334/UA/MyLittlePubSubServer
         */        
        console.log("server started at ", server.getEndpointUrl());
        
    } catch(err) {
        console.log(err);
        process.exit(1);
    }
})();


/** 
 * Costruire i parametri di configurazione.
 * 
 * Dobbiamo fornire una serie di parametri per configurare completamente il publisher e il dataset associato. 
 * Un dataset descrive le variabili che verranno pubblicate nel payload.
 * Analizziamo la funzione 'getPubSubConfiguration' che produce l'oggetto di configurazione.
 * Restituisce un oggetto 'PubSubConfigurationDataType' contenente la configurazione Pub/Sub. 
 * La configurazione descrive la connessione e il set di dati pubblicato.
 * 
 * 
 * Il documento PubSubConfiguration descrive le connessioni e i set di dati pubblicati.
 * Una connessione contiene parametri specifici relativi al protocollo di trasporto PubSub utilizzato 
 * e ai vari parametri richiesti al broker o al sistema di trasmissione.
 * Un set di dati pubblicato descrive il contenuto del payload e fornisce le informazioni di mappatura 
 * per collegare le variabili pubblicate con le corrispondenti variabili OPC UA nello spazio degli indirizzi del server.
 * Nel nostro caso, dobbiamo definire una connessione OPCUA JSON MQTT e un singolo dataset.
 */
function getPubSubConfiguration(temperatureSensor) {
  const connection = createConnection();

  const publishedDataSet = createPublishedDataSet(temperatureSensor);

  return new opcuatype.PubSubConfigurationDataType({
    connections: [connection],
    publishedDataSets: [publishedDataSet] });
}

/**
 * 'opcuatype.PubSubConfigurationDataType' è un tipo di dato strutturato 
 * definito nello standard OPC UA, usato per rappresentare l’intera configurazione 
 * di un sistema PubSub.
 * 
 * Questo tipo di dato è una struttura complessa che include:
 * - PublishedDataSets → quali dati vengono messi a disposizione;
 * - Connections → come i nodi sono collegati (rete, protocollo, etc.);
 * - WriterGroups → gruppi che pubblicano dati.
 */

/**
 * Primo passo. Collezione dei dati da pubblicare. 
 * La collezione produce valori per il DataSet da parte del DataSet Collector.
 * La configurazione è definita dal PublishedDataSet, che contiene, tra gli altri:
 * - DataSetMetaData
 * - Lista delle Variabili: NodeId, SamplingInterval, Deadband
 * - Lista Eventi: NodeId Event Notifier, ContentFilter
 */

function createConnection() {

    const mqttEndpoint = "mqtt:broker.emqx.io";

    /**
     * Il secondo passo è realizzato dal DataSetWriter 
     * che crea il DataSetMessage a partire dai DataSet
     * 
     * La creazione di un DataSetMessage è guidata dal DataSetWriter, 
     * che contiene tra gli altri:
     *  - DataSetWriterId
     *  - DataSetFieldContentMask
     *    -  Variant, DataValue, RawData
    */
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
 
    /**
     * Un altro passo è rappresentato dalla creazione di un NetworkMessage.
     * 
     * Il processo di creazione è governato da diversi parametri del WriterGroup.
     * - PublishingInterval: frequenza di creazione ed invio dei NetworkMessage
     * 
     * Pattern di comunicazione possibili:
     * - AtMostOnce = nessuna garanzia di ricevere correttamente il messaggio trasmesso.
     * - AtLeastOnce = il messaggio può essere ritrasmesso se non correttamente ricevuto 
     * (possono arrivare duplicati, ma almeno uno arriva).
     */
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

    /**
     * Ultimo passo è rappresentato dalla creazione di un oggetto 
     * di tipo 'MyMqttJsonPubSubConnectionDataType', che definisce 
     * il protocollo di trasporto (MQTT, UDP, etc.), specifica l’indirizzo 
     * di destinazione del middleware, contiene i WriterGroup
     * 
     * TransportProfileUri. Il parametro TransportProfileUri indica 
     * il mapping con il protocollo di trasporto e il message mapping utilizzato.
     * - Nel nostro caso: MQTT/JSON. 
     * 
     * Address. Il parametro Address contiene l'indirizzo da usare per il 
     * communication middleware. 
     * - Nel nostro caso il Broker: 'const mqttEndpoint = "mqtt:broker.emqx.io";'.
    */
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

/**
 * L'oggetto PublishedDataSet descrive il contenuto di un messaggio di payload e le corrispondenti variabili OPC UA.
 * L'array 'dataSetMetaData.fields' contiene l'elenco delle proprietà esposte nel messaggio di payload.
 * Nel nostro caso, abbiamo una sola variabile da esporre. Diamole un nome di proprietà. Scelgo "Sensor.Temperature". 
 * Dobbiamo anche specificare che si tratta di un valore Double.
 * L'array 'dataSetSource.publishedData' contiene lo stesso numero di elementi di 'dataSetMetaData.fields'.
 * Ciascun elemento di 'dataSetSource.publishedData' viene utilizzato per mappare la proprietà del set di dati 
 * alla variabile OPC-UA corrispondente nello spazio degli indirizzi del server con i parametri 'attributeId' e '.publishedVariable'
 * Indica inoltre l'intervallo di campionamento suggerito per il parametro 'samplingIntervalHint'.
 * Nel nostro caso, il nodeId della variabile da monitorare è "ns=1;s=Temperature"
 * 
 * PublishedDataSet
 * - Name="PublishedDataSet1"
 * - DataSetMetaData
 *   - Elenco dei Fields
 * DataSetSource
 *  - Lista di Variabili (attributo, nodeId, SamplingInterval)
 */
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



