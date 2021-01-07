const fdk = require('@fnproject/fdk');
const os = require("oci-objectstorage");
const common = require("oci-common");
const fs = require("fs").promises;
const oracledb = require('oracledb');

var walletDownloaded = false;

downloadWallet = async function() {
    const provider = common.ResourcePrincipalAuthenticationDetailsProvider.builder();
    const client = new os.ObjectStorageClient({
        authenticationDetailsProvider: provider
    });

    await fs.mkdir("/tmp/wallet", { recursive: true });

    const namespace = process.env.NAMESPACE
    const bucket = process.env.BUCKET_NAME
    console.log("Downloading wallet... Namespace: '" + namespace + "' Bucket: '" + bucket + "'");
    listRequest = {
        namespaceName: namespace,
        bucketName: bucket
    };
    console.log("Listing objects...");
    return client.listObjects(listRequest).then((result) => {
        console.log("Received a list of " + result.listObjects.objects.length + " objects.")
        promises = result.listObjects.objects.map((i) => {
            let name = i.name
            getRequest = {
                namespaceName: namespace,
                bucketName: bucket,
                objectName: name
            };
            console.log("Setting up download of " + name);
            return client.getObject(getRequest)
            .then((response) => {
                console.log("Reading data stream...");
                let chunk;
                let content = Buffer.from([]);
                // Use a loop to make sure we read all currently available data
                while (null !== (chunk = response.value.read())) {
                    console.log(`Read ${chunk.length} bytes of data...`);
                    content = Buffer.concat([content, chunk])
                }
                console.log("Writing file...");
                return fs.writeFile("/tmp/wallet/" + name, content);
            })
            .then((writeResp) => {
                console.log("Written file.");
            })
            .catch((e) => {
                console.error("Error received: " + e.message)
            });
        });
        return Promise.allSettled(promises).then((result) => {
            console.log("Initialising oracle DB client...");
            oracledb.initOracleClient({configDir: '/tmp/wallet'});
            oracledb.outFormat = oracledb.OBJECT;
            oracledb.fetchAsString = [oracledb.CLOB];
            oracledb.autoCommit = true;
            walletDownloaded = true;
            return "Proceeding.";
        });
    })
    .then((ok) => {
        // Just to cause it to wait on this
        console.log(ok);
    })
    .catch((e) => {
        console.error("Error received: " + e.message)
        return {"error": "Unable to list objects"};
    });
}

fdk.handle( async function(input){

    if(!walletDownloaded) {
        downloadError = await downloadWallet()
        if(downloadError) {
            console.error(downloadError)
            return downloadError
        }
    }

    let connection;
    let result = [];

    try {
        console.log("Creating connection...");
        connection = await oracledb.getConnection({
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            // I think connect string only needs DB name as initOracleClient sets up the rest of the stuff
            connectString: process.env.TNS_NAME
        });

        console.log("Opening SODA collection...");
        const collectionName = process.env.COLLECTION_NAME;
        const soda = connection.getSodaDatabase();
        const collection = await soda.createCollection(collectionName);

        if(input) {
            console.log("Getting item by id: " + input);
            // Get by ID
            const filterSpec = { "id": input };
            const documents = await collection.find().filter(filterSpec).getDocuments();
            documents.forEach(function(element) {
                result.push(element);
            });
        } else {
            console.log("Getting all items...");
            // Get all
            const documents = await collection.find().getDocuments();
            documents.forEach(function(element) {
                result.push(element);
            });
        }
    }
    catch(err) {
        console.error(err);
        return {"error": "Unable to read from database"}
    }
    finally {
        if (connection) {
            try {
                await connection.close();
            } catch(err) {
                console.error(err);
            }
        }
    }

  return result;
}, {});

