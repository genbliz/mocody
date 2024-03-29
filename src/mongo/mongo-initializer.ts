import { MongoClient, MongoClientOptions, Document } from "mongodb";
import throat from "throat";
const concurrency = throat(1);

interface IDbOptions {
  uri: string;
  databaseName: string;
  options?: MongoClientOptions;
}

export class MocodyInitializerMongo {
  private _mongoClient: MongoClient | null | undefined;
  private readonly _inits: IDbOptions;

  constructor(inits: IDbOptions) {
    this._inits = inits;
  }

  private async getInstance() {
    if (!this._mongoClient) {
      this._mongoClient = new MongoClient(this._inits.uri, this._inits.options);
      await this._mongoClient.connect();
    }
    return this._mongoClient;
  }

  async getNewSession() {
    const client = await concurrency(() => this.getInstance());
    return client.startSession();
  }

  async getCustomCollectionInstance<T = any>(collectionName: string) {
    const client = await concurrency(() => this.getInstance());
    const col = client.db(this._inits.databaseName).collection<T & Document>(collectionName);
    return col;
  }

  async close() {
    if (this._mongoClient) {
      await this._mongoClient.close();
      this._mongoClient = null;
    }
  }
}
