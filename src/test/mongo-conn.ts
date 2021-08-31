import { MocodyInitializerMongo } from "../mongo/mongo-initializer";

const uri = `mongodb+srv://hospimanuser:g9TBp7sD52lmBQNE@hospimantestdb01.if0om.mongodb.net`;

// const urioo = `mongodb+srv://server.example.com/?connectTimeoutMS=300000&authSource=aDifferentAuthDB`;

class MongoConnectionBase {
  private _dbConn!: MocodyInitializerMongo;

  getConnection() {
    if (!this._dbConn) {
      // const uri = `mongodb+srv://hospimandbuser:g9TBp7sD52lmBQNE@hospimandb02.nwgy1.mongodb.net?authSource=admin`;
      this._dbConn = new MocodyInitializerMongo({
        uri,
        databaseName: "hospimantestdb01",
        options: {
          replicaSet: "hospimantestdb01",
          retryWrites: true,
          // w: "majority",
          // auth: {
          //   password: "g9TBp7sD52lmBQNE",
          //   username: "hospimandbuser",
          // },
        },
      });
      console.log({ getDynamoConnection_INITIALIZED: true });
    } else {
      // console.log({ getDynamoConnection_RE_USED: true });
    }
    return this._dbConn;
  }
}

export const MongoConnection = new MongoConnectionBase();

/*

mongodb+srv://chris:<password>@hospimandb02.nwgy1.mongodb.net/myFirstDatabase?retryWrites=true&w=majority

const uri = `mongodb+srv://chris:<password>@hospimandb02.nwgy1.mongodb.net/myFirstDatabase?retryWrites=true&w=majority`;
const client = new MongoClient(uri);
client.connect((err) => {
  const collection = client.db("test").collection("devices");
  client.close();
});
*/
