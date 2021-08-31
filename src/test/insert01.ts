import { MongoClient, ServerApiVersion } from "mongodb";

// Connection URI
// const uri = "mongodb+srv://sample-hostname:27017/?poolSize=20&writeConcern=majority";
const uri = "mongodb+srv://hospimanuser:g9TBp7sD52lmBQNE@hospimantestdb01.if0om.mongodb.net/?writeConcern=majority";

// Create a new MongoClient
const client = new MongoClient(uri, { serverApi: ServerApiVersion.v1 });

async function run() {
  try {
    // Connect the client to the server
    await client.connect();

    // Establish and verify connection
    await client.db("hospimantestdb01").command({ ping: 1 });
    console.log("Connected successfully to server");
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
}
run().catch(console.dir);
