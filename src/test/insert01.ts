import { MongoClient, ServerApiVersion } from "mongodb";

/*
const uri =
  "mongodb+srv://hospimanuser:<password>@hospimantestdb01.if0om.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
const client = new MongoClient(uri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverApi: ServerApiVersion.v1,
});
*/

const uri =
  "mongodb+srv://hospimanuser:g9TBp7sD52lmBQNE@hospimantestdb01.if0om.mongodb.net/hospimandbm1?retryWrites=true&w=majority";

const client = new MongoClient(uri, {
  // @ts-ignore
  useNewUrlParser: true,
  useUnifiedTopology: true,
  serverApi: ServerApiVersion.v1,
});

export async function runInsert() {
  const collection = client.db("hospimantestdb01").collection("patient");
  // perform actions on the collection object

  console.log(collection.dbName);
  await collection.insertOne({ ag: 36, name: "John Martin" });
  console.log(collection.dbName);

  // client.close();
  // process.exit(0);
}

client.connect((err) => {
  if (err) {
    console.error(err);
  } else {
    // runInsert().catch(console.log);
    const collection = client.db("hospimandbm1").collection("patients");
    console.log(collection.dbName);
    collection.insertOne({ ag: 36, name: "John Martin" }, (err) => {
      client.close();
    });
  }
});

// ts-node ./src/test/insert01.ts
