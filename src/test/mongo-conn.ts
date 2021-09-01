import { MocodyInitializerMongo } from "../mongo/mongo-initializer";

const uri = process.env.MONGO_URI || "";

class MongoConnectionBase {
  private _dbConn!: MocodyInitializerMongo;

  getConnection() {
    if (!this._dbConn) {
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
