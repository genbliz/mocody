import { LoggingService } from "../helpers/logging-service";
import type { IMocodyCoreEntityModel } from "../core/base-schema";
import Nano from "nano";
import throat from "throat";
import { MocodyGenericError } from "../helpers/errors";
const concurrency = throat(1);

type IBaseDef<T> = Omit<T & IMocodyCoreEntityModel, "">;

interface IBasisOptions {
  //http://admin:mypassword@localhost:5984
  authType: "basic";
  couchConfig: {
    host: string;
    password: string;
    username: string;
    databaseName: string;
    port?: number;
    protocol?: "http" | "https";
  };
  indexes?: { indexName: string; fields: string[] }[];
}

interface ICookieOptions {
  //http://admin:mypassword@localhost:5984
  authType: "cookie";
  couchConfig: {
    host: string;
    databaseName: string;
    port?: number;
    protocol?: "http" | "https";
    cookie: string;
    headers?: Record<string, string>;
  };
  indexes?: { indexName: string; fields: string[] }[];
}

interface IProxyOptions {
  //http://admin:mypassword@localhost:5984
  authType: "proxy";
  couchConfig: {
    host: string;
    databaseName: string;
    port?: number;
    protocol?: "http" | "https";
    //
    proxy: {
      username: string;
      roles: string[];
      token: string;
    };
  };
  indexes?: { indexName: string; fields: string[] }[];
}

type IOptions = IProxyOptions | ICookieOptions | IBasisOptions;

export class MocodyInitializerCouch {
  private _databaseInstance!: Nano.ServerScope | null;
  private _documentScope!: Nano.DocumentScope<any> | null;

  private readonly baseConfig: IOptions;

  constructor(baseConfig: IOptions) {
    this.baseConfig = baseConfig;
  }

  async deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    const path = ["_index", ddoc, "json", name].join("/");
    const instance = await this.getInstance();
    const result: { ok: boolean } = await instance.relax({
      db: this.baseConfig.couchConfig.databaseName,
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
      db: this.baseConfig.couchConfig.databaseName,
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
      const serverScope = await this.getInstance();
      const db = serverScope.db.use<IBaseDef<T>>(this.baseConfig.couchConfig.databaseName);

      try {
        if (this.baseConfig?.indexes?.length) {
          for (const indexItem of this.baseConfig.indexes) {
            await this.couch_createIndex({ ...indexItem, dbx: db });
          }
        }
      } catch (error) {
        LoggingService.error(error);
      }

      this._documentScope = db;
    }
    return this._documentScope;
  }

  private async couch_createIndex({
    indexName,
    fields,
    dbx,
  }: {
    indexName: string;
    fields: string[];
    dbx?: Nano.DocumentScope<IBaseDef<any>>;
  }) {
    const instance = dbx || (await this.getDocInstance());
    const result = await instance.createIndex({
      index: { fields },
      name: indexName,
      ddoc: indexName,
      type: "json",
      partitioned: true,
    });
    LoggingService.log(result);
    return {
      id: result.id,
      name: result.name,
      result: result.result,
    };
  }

  async checkDatabaseExists(databaseName?: string) {
    const instance = await this.getInstance();
    const checkDbExistResult = await instance.request({
      db: databaseName || this.baseConfig.couchConfig.databaseName,
      method: "HEAD",
      content_type: "application/json",
    });
    LoggingService.log(JSON.stringify({ checkDbExistResult }, null, 2));
    return checkDbExistResult;
  }

  async createDatabase() {
    const instance = await this.getInstance();
    return instance.db.create(this.baseConfig.couchConfig.databaseName, { partitioned: true });
  }

  async getInstance() {
    return await concurrency(() => this.getInstanceBase());
  }

  private async getInstanceBase() {
    if (!this._databaseInstance) {
      // http://admin:mypassword@localhost:5984
      // http://username:password@hostname:port

      const { host, port } = this.baseConfig.couchConfig;
      const protocol = this.baseConfig.couchConfig.protocol || "http";

      if (this.baseConfig.authType === "basic") {
        const { password, username } = this.baseConfig.couchConfig;

        const pw = encodeURIComponent(password);
        const uname = encodeURIComponent(username);

        const url = `${protocol}://${uname}:${pw}@${host}:${port}`;
        this._databaseInstance = Nano({ url });
      } else if (this.baseConfig.authType === "proxy") {
        const { roles, username, token } = this.baseConfig.couchConfig.proxy;

        const headers: Record<string, string> = {};
        headers["X-Auth-CouchDB-UserName"] = username;
        headers["X-Auth-CouchDB-Roles"] = roles.join(",");
        headers["X-Auth-CouchDB-Token"] = token;

        const url = `${protocol}://${host}:${port}`;
        this._databaseInstance = Nano({ url, requestDefaults: { headers } });
      } else if (this.baseConfig.authType === "cookie") {
        const url = `${protocol}://${host}:${port}`;
        this._databaseInstance = Nano({ url, cookie: this.baseConfig.couchConfig.cookie });
      } else {
        throw new MocodyGenericError("Invalid auth type definition");
      }
    }
    return await Promise.resolve(this._databaseInstance);
  }
}
