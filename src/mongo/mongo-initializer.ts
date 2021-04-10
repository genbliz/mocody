import { MongoClient, MongoClientOptions } from "mongodb";

interface IDbOptions {
  uri: string;
  databaseName: string;
  collectionName: string;
  options?: MongoClientOptions;
}

export class MocodyInitializerMongo {
  private _mongoClient: MongoClient;
  private readonly _inits: IDbOptions;

  constructor(inits: IDbOptions) {
    this._inits = inits;
    this._mongoClient = new MongoClient(inits.uri, inits.options);
  }

  private async getInstance() {
    if (!this._mongoClient) {
      this._mongoClient = new MongoClient(this._inits.uri, this._inits.options);
    }
    if (!this._mongoClient.isConnected()) {
      await this._mongoClient.connect();
    }
    return this._mongoClient;
  }

  async getDbInstance<T = any>() {
    const client = await this.getInstance();
    const database = client.db(this._inits.databaseName);
    const collection = database.collection<T>(this._inits.collectionName);
    return collection;
  }

  async close() {
    if (this._mongoClient) {
      await this._mongoClient.close();
    }
  }
}
