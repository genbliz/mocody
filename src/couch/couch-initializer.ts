import { LoggingService } from "../helpers/logging-service";
import type { IMocodyCoreEntityModel } from "../core/base-schema";
import Nano from "nano";
import throat from "throat";
import { MocodyGenericError } from "../helpers/errors";
const concurrency = throat(1);

type IBaseDef<T> = Omit<T & IMocodyCoreEntityModel, "">;

type IFindOptions = {
  featureEntity: string;
  skip: number | undefined;
  limit: number | undefined;
  selector: Nano.MangoSelector;
  fields: string[] | undefined;
  use_index: string;
  sort?: {
    [propName: string]: "asc" | "desc";
  }[];
};

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
    headers?: { [key: string]: string };
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
    proxyHeaders: { [key: string]: string };
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

  async findPartitionedDocs({ featureEntity, selector, fields, use_index, sort, limit, skip }: IFindOptions) {
    const db01 = await this.getDocInstance();
    return await db01.partitionedFind(featureEntity, {
      selector: { ...selector },
      fields,
      use_index,
      sort: sort?.length ? sort : undefined,
      limit,
      skip,
      // bookmark: paramOption?.pagingParams?.nextPageHash,
    });
  }

  async getById({ nativeId }: { nativeId: string }) {
    try {
      const db01 = await this.getDocInstance();
      return await db01.get(nativeId);
    } catch (error) {
      type IErrorById = {
        error: "not_found";
        reason: "missing";
        statusCode: 404;
      };

      const error01 = error as IErrorById;

      if (error01?.error === "not_found" || error01?.statusCode === 404) {
        return null;
      }

      LoggingService.error(error);
      throw error;
    }
  }

  async getManyByIds({ nativeIds }: { nativeIds: string[] }) {
    const db01 = await this.getDocInstance();
    const dataList = await db01.list({
      keys: nativeIds,
      include_docs: true,
    });
    return dataList;
  }

  async createDoc({ validatedData }: { validatedData: any }) {
    const db01 = await this.getDocInstance();
    return await db01.insert(validatedData);
  }

  async updateDoc({ docRev, validatedData }: { docRev: string; validatedData: any }) {
    const db01 = await this.getDocInstance();
    return await db01.insert({ ...validatedData, _rev: docRev });
  }

  async getList({ featureEntity, size, skip }: { featureEntity: string; size?: number | null; skip?: number | null }) {
    const db01 = await this.getDocInstance();
    const data = await db01.list({
      include_docs: true,
      startkey: featureEntity,
      endkey: `${featureEntity}\ufff0`,
      inclusive_end: true,
      limit: size ?? undefined,
      skip: skip ?? undefined,
    });
    return data;
  }

  async deleteById({ docRev, nativeId }: { docRev: string; nativeId: string }) {
    const db01 = await this.getDocInstance();
    return await db01.destroy(nativeId, docRev);
  }

  private async getDocInstance<T>(): Promise<Nano.DocumentScope<IBaseDef<T>>> {
    if (!this._documentScope) {
      const serverScope = await this.getInstance();
      const db = serverScope.db.use<IBaseDef<T>>(this.baseConfig.couchConfig.databaseName);

      try {
        if (this.baseConfig?.indexes?.length) {
          for (const indexItem of this.baseConfig.indexes) {
            await this.createIndex({ ...indexItem, dbx: db });
          }
        }
      } catch (error) {
        LoggingService.error(error);
      }

      this._documentScope = db;
    }
    return this._documentScope;
  }

  async createIndex({
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
    LoggingService.logAsString({ checkDbExistResult });
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
        //
      } else if (this.baseConfig.authType === "proxy") {
        const proxyHeaders = this.baseConfig.couchConfig.proxyHeaders;

        const url = `${protocol}://${host}:${port}`;
        this._databaseInstance = Nano({ url, requestDefaults: { headers: proxyHeaders } });
        //
      } else if (this.baseConfig.authType === "cookie") {
        //
        const url = `${protocol}://${host}:${port}`;
        this._databaseInstance = Nano({ url, cookie: this.baseConfig.couchConfig.cookie });
      } else {
        throw new MocodyGenericError("Invalid auth type definition");
      }
    }
    return await Promise.resolve(this._databaseInstance);
  }
}
