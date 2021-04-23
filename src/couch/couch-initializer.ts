import { LoggingService } from "../helpers/logging-service";
import type { IMocodyCoreEntityModel } from "../core/base-schema";
import Nano from "nano";

import throat from "throat";
const concurrency = throat(1);

type IBaseDef<T> = Omit<T & IMocodyCoreEntityModel, "">;

interface IOptions {
  //http://admin:mypassword@localhost:5984
  couchConfig: {
    /**
     * eg: ```127.0.0.1, localhost, example.com```
     */
    host: string;
    password?: string;
    username?: string;
    databaseName: string;
    port?: number;
    /**
     * default: ```http```
     */
    protocol?: "http" | "https";
  };
}

export class MocodyInitializerCouch {
  private _databaseInstance!: Nano.ServerScope;
  private _documentScope!: Nano.DocumentScope<any>;

  private readonly couchConfig: IOptions["couchConfig"];
  // private readonly sqliteConfig: IOptions["sqliteConfig"];
  // readonly sqliteSplitDb: boolean;

  constructor({ couchConfig }: IOptions) {
    this.couchConfig = couchConfig;
  }

  private getFullDbUrl(config: IOptions["couchConfig"]) {
    //http://admin:mypassword@localhost:5984
    const protocol = config?.protocol || "http";
    const dbUrlPart: string[] = [`${protocol}://`];

    if (config?.username && config.password) {
      dbUrlPart.push(config.username);
      dbUrlPart.push(`:${config.password}@`);
    }

    dbUrlPart.push(config.host);

    if (config?.port) {
      dbUrlPart.push(`:${config.port}`);
    }
    return dbUrlPart.join("");
  }

  async deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    const path = ["_index", ddoc, "json", name].join("/");
    const result: { ok: boolean } = await this._databaseInstance.relax({
      db: this.couchConfig.databaseName,
      method: "DELETE",
      path,
      content_type: "application/json",
    });
    // DELETE /{db}/_index/{designdoc}/json/{name}
    return result;
  }

  async getIndexes() {
    type IIndexList = {
      indexes: {
        ddoc: string;
        name: string;
        type: string;
        def: {
          fields: {
            [field: string]: "asc" | "desc";
          }[];
        };
      }[];
      total_rows: number;
    };
    const instance = await this.getInstance();
    const result: IIndexList = await instance.request({
      db: this.couchConfig.databaseName,
      method: "GET",
      path: "_index",
      content_type: "application/json",
    });
    LoggingService.log({ indexes: result });
    return result;
    //GET /{db}/_index
  }

  async getDocInstance<T>(): Promise<Nano.DocumentScope<IBaseDef<T>>> {
    if (!this._documentScope) {
      const n = await this.getInstance();
      const db = n.db.use<IBaseDef<T>>(this.couchConfig.databaseName);
      this._documentScope = db;
    }
    return this._documentScope;
  }

  async checkDatabaseExists(databaseName?: string) {
    const instance = await this.getInstance();
    const checkDbExistResult = await instance.request({
      db: databaseName || this.couchConfig.databaseName,
      method: "HEAD",
      content_type: "application/json",
    });
    LoggingService.log(JSON.stringify({ checkDbExistResult }, null, 2));
    return checkDbExistResult;
  }

  async createDatabase() {
    const instance = await this.getInstance();
    return instance.db.create(this.couchConfig.databaseName, { partitioned: true });
  }

  async getInstance() {
    return await concurrency(() => this.getInstanceBase());
  }

  private async getInstanceBase() {
    if (!this._databaseInstance) {
      this._databaseInstance = Nano(this.getFullDbUrl(this.couchConfig));
    }
    return await Promise.resolve(this._databaseInstance);
  }
}
