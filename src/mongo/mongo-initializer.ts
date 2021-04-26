import { MongoClient, MongoClientOptions, Collection } from "mongodb";
import throat from "throat";
const concurrency = throat(1);

interface IDbOptions {
  uri: string;
  databaseName: string;
  collectionName: string;
  options?: MongoClientOptions;
}

export class MocodyInitializerMongo {
  private _mongoClient!: MongoClient | null;
  private readonly _inits: IDbOptions;

  constructor(inits: IDbOptions) {
    this._inits = inits;
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

  async getCollectionInstance<T = any>() {
    const client = await concurrency(() => this.getInstance());
    const col: Collection<T> = client.db(this._inits.databaseName).collection<T>(this._inits.collectionName);
    return col;
  }

  async getCustomCollectionInstance<T = any>(collectionName: string) {
    const client = await concurrency(() => this.getInstance());
    const col: Collection<T> = client.db(this._inits.databaseName).collection<T>(collectionName);
    return col;
  }

  async close() {
    if (this._mongoClient) {
      await this._mongoClient.close();
      this._mongoClient = null;
    }
  }
}
