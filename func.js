const fdk = require('@fnproject/fdk');
const os = require("oci-objectstorage");
const common = require("oci-common");
const fs = require("fs");
const oracledb = require('oracledb');

var walletDownloaded = false;

//private final File walletDir = new File("/tmp", "wallet");
//private final String tnsName = System.getenv().get("TNS_NAME");
//private final String dbUrl = "jdbc:oracle:thin:@" + tnsName + "?TNS_ADMIN=/tmp/wallet";

fdk.handle( async function(input){

    if(!walletDownloaded) {
        try {
            const provider = common.ResourcePrincipalAuthenticationDetailsProvider.builder();
            const client = new os.ObjectStorageClient({
                authenticationDetailsProvider: provider
            });

            console.log("Downloading wallet...");
            listRequest = {
                namespaceName: process.env.NAMESPACE,
                bucketName: process.env.BUCKET_NAME
            };
            console.log("Listing objects...");
            client.listObjects(listRequest)
            .then((result) => {
                result.listObjects.objects.map((i) => i.name).forEach((name) => {
                    console.log("Downloading " + name);
                    getRequest = {
                        namespaceName: process.env.NAMESPACE,
                        bucketName: process.env.BUCKET_NAME,
                        objName: name
                    };
                    client.getObject(getRequest)
                    .then((result) => {
                        fs.writeFile("/tmp/wallet/" + name, result.value);
                    })
                });
            });

            walletDownloaded = true;
        }
        catch(err) {
            console.error(err);
            return {"error": "Unable to download wallet"};
        }

        oracledb.initOracleClient({configDir: '/tmp/wallet'});
        oracledb.outFormat = oracledb.OBJECT;
        oracledb.fetchAsString = [oracledb.CLOB];
        oracledb.autoCommit = true;
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

