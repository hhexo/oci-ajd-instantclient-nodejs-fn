const fdk = require('@fnproject/fdk');
const os = require("oci-objectstorage");
const common = require("oci-common");
const fs = require("fs").promises;
const oracledb = require('oracledb');
oracledb.outFormat = oracledb.OBJECT;
oracledb.fetchAsString = [oracledb.CLOB];
oracledb.autoCommit = true;

var walletDownloaded = false;

downloadWallet = async function() {
    try {
        const provider = common.ResourcePrincipalAuthenticationDetailsProvider.builder();
        const client = new os.ObjectStorageClient({
            authenticationDetailsProvider: provider
        });

        await fs.mkdir("/tmp/wallet", { recursive: true });

        const namespace = process.env.NAMESPACE
        const bucket = process.env.BUCKET_NAME
        console.log("Downloading wallet... Namespace: '" + namespace + "' Bucket: '" + bucket + "'");
        const listRequest = {
            namespaceName: namespace,
            bucketName: bucket
        };
        console.log("Listing objects...");
        response = await client.listObjects(listRequest);
        console.log("Received a list of ", response.listObjects.objects.length, " objects.");
        for(const obj of response.listObjects.objects) {
            const getRequest = {
                namespaceName: namespace,
                bucketName: bucket,
                objectName: obj.name
            };
            console.log("Setting up download of ", obj.name);
            getResponse = await client.getObject(getRequest);
            console.log("Reading data stream...");
            const chunks = [];
            for await (let chunk of getResponse.value) {
                chunks.push(chunk);
            }
            const content = Buffer.concat(chunks);
            console.log("Writing file...");
            if(obj.name === "sqlnet.ora") {
                await fs.writeFile("/tmp/wallet/" + obj.name, "WALLET_LOCATION = (SOURCE = (METHOD = file) (METHOD_DATA = (DIRECTORY=\"/tmp/wallet\")))\nSSL_SERVER_DN_MATCH=yes")
            } else {
                await fs.writeFile("/tmp/wallet/" + obj.name, content);
            }
            console.log("Written file.");
        }

        console.log("Initialising oracle DB client...");
        //oracledb.initOracleClient();
        oracledb.initOracleClient({configDir: '/tmp/wallet'});
        walletDownloaded = true;
    }
    catch(error) {
        console.error("Error received: " + error)
        return {"error": "Unable to download objects"};
    }
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
        console.log("Creating connection to '" + process.env.TNS_NAME + "'...");
        connection = await oracledb.getConnection({
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            // Allegedly connect string only needs DB name as initOracleClient sets up the rest of the stuff by reading the wallet files
            connectString: process.env.TNS_NAME
            //connectString: '(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.us-ashburn-1.oraclecloud.com))(connect_data=(service_name=lfvaqzxk9tc4jcq_ajdspikle_tp.adb.oraclecloud.com))(security=(MY_WALLET_DIRECTORY="/tmp/wallet")(ssl_server_cert_dn="CN=adwc.uscom-east-1.oraclecloud.com,OU=Oracle BMCS US,O=Oracle Corporation,L=Redwood City,ST=California,C=US")))'
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
                result.push({
                    id: element.key,
                    createdOn: element.createdOn,
                    lastModified: element.lastModified,
                    document: element.getContent()
                });
            });
        } else {
            console.log("Getting all items...");
            // Get all
            const documents = await collection.find().getDocuments();
            documents.forEach(function(element) {
                result.push({
                    id: element.key,
                    createdOn: element.createdOn,
                    lastModified: element.lastModified,
                    document: element.getContent()
                });
            });
        }
    }
    catch(err) {
        console.error(err);
        return {"error": "" + err}
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

  return {"data": result};
}, {});

