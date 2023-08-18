import Nano from "nano";
import throat from "throat";
import PouchDB from "pouchdb";
import pouchdbFind from "pouchdb-find";

import { LoggingService } from "../helpers/logging-service";
import type { IMocodyCoreEntityModel } from "../core/base-schema";
import { MocodyGenericError } from "../helpers/errors";
import { UtilService } from "../helpers/util-service";

const concurrency = throat(1);

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

type IBaseDef<T> = Omit<T & IMocodyCoreEntityModel, "">;

type ILocalFirstConfig = {
  configType: "LOCAL_FIRST";
  sqliteDbFilePath: string;
  couchConfig?: {
    host: string;
    password: string;
    username: string;
    databaseName: string;
    port?: number;
    protocol?: "http" | "https";
  };
  liveSync?: boolean;
  indexes?: { indexName: string; fields: string[] }[];
};

type IRemoteFirstConfig = {
  configType: "REMOTE_FIRST";
  pouchConfig: {
    /**
     * eg: ```127.0.0.1, localhost, example.com```
     */
    host: string;
    password?: string;
    username?: string;
    databaseName: string;
    port: number;
    /**
     * default: ```http```
     */
    protocol?: "http" | "https";
    cookie?: string;
  };
  indexes?: { indexName: string; fields: string[] }[];
};

type IOptions = ILocalFirstConfig | IRemoteFirstConfig;

export class MocodyInitializerPouch {
  private _databaseInstance!: Nano.ServerScope | null;
  private _pouchInstance!: PouchDB.Database<IBaseDef<any>> | null;
  private _documentScope!: Nano.DocumentScope<any> | null;

  private readonly baseConfig: IOptions;

  constructor(baseConfig: IOptions) {
    this.baseConfig = baseConfig;
  }

  async deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    if (this.isLocalFirst()) {
      return await this.pouch_deleteIndex({ ddoc, name });
    }
    // DELETE /{db}/_index/{designdoc}/json/{name}
    const path = ["_index", ddoc, "json", name].join("/");
    const instance = await this.getInstance();
    const config = this.getRemoteFirstConfig();

    const result: { ok: boolean } = await instance.relax({
      db: config.pouchConfig.databaseName,
      method: "DELETE",
      path,
      content_type: "application/json",
    });
    return result;
  }

  private async pouch_deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.deleteIndex({ ddoc, name });
  }

  async getIndexes() {
    if (this.isLocalFirst()) {
      return await this.pouch_getIndexes();
    }

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

    const config = this.getRemoteFirstConfig();

    const instance = await this.getInstance();
    const result: IIndexList = await instance.request({
      db: config.pouchConfig.databaseName,
      method: "GET",
      path: "_index",
      content_type: "application/json",
    });
    LoggingService.log({ indexes: result });
    return result;
    //GET /{db}/_index
  }

  private async pouch_getIndexes() {
    const db01 = await this.pouch_getDbInstance();
    return await db01.getIndexes();
  }

  async createIndex({ indexName, fields }: { indexName: string; fields: string[] }) {
    if (this.isLocalFirst()) {
      return await this.pouch_createIndex({ indexName, fields });
    }
    return this.couch_createIndex({ indexName, fields });
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
      index: {
        fields: fields,
      },
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

  private async pouch_createIndex({
    indexName,
    fields,
    dbx,
  }: {
    indexName: string;
    fields: string[];
    dbx?: PouchDB.Database<IBaseDef<unknown>>;
  }) {
    const db01 = dbx || (await this.pouch_getDbInstance());
    return await new Promise((resolve, reject) => {
      return db01.createIndex(
        {
          index: {
            fields: fields,
            name: indexName,
            ddoc: indexName,
            type: "json",
          },
        },
        (error, result) => {
          if (error) {
            reject(error);
          } else {
            resolve(result);
          }
        },
      );
    });
  }

  async getDocInstance<T>(): Promise<Nano.DocumentScope<IBaseDef<T>>> {
    if (!this._documentScope) {
      const config = this.getRemoteFirstConfig();

      const serverScope = await this.getInstance();
      const db = serverScope.db.use<IBaseDef<T>>(config.pouchConfig.databaseName);

      try {
        if (config?.indexes?.length) {
          for (const indexItem of config.indexes) {
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

  async getById({ nativeId }: { nativeId: string }) {
    if (this.isLocalFirst()) {
      return await this.pouch_getById({ nativeId });
    }
    const db01 = await this.getDocInstance();
    return await db01.get(nativeId);
  }

  private async pouch_getById({ nativeId }: { nativeId: string }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.get(nativeId);
  }

  async getManyByIds({ nativeIds }: { nativeIds: string[] }) {
    if (this.isLocalFirst()) {
      return await this.pouch_getManyByIds({ nativeIds });
    }
    const db01 = await this.getDocInstance();
    const dataList = await db01.list({
      keys: nativeIds,
      include_docs: true,
    });
    return dataList;
  }

  private async pouch_getManyByIds({ nativeIds }: { nativeIds: string[] }) {
    const db01 = await this.pouch_getDbInstance();
    const dataList = await db01.allDocs({
      keys: nativeIds,
      include_docs: true,
    });
    return dataList;
  }

  async createDoc({ validatedData }: { validatedData: any }) {
    if (this.isLocalFirst()) {
      return await this.pouch_createDoc({ validatedData });
    }
    const db01 = await this.getDocInstance();
    return await db01.insert(validatedData);
  }

  private async pouch_createDoc({ validatedData }: { validatedData: any }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.put(validatedData);
  }

  async updateDoc({ docRev, validatedData }: { docRev: string; validatedData: any }) {
    if (this.isLocalFirst()) {
      return await this.pouch_updateDoc({ validatedData, docRev });
    }
    const db01 = await this.getDocInstance();
    return await db01.insert({ ...validatedData, _rev: docRev });
  }

  private async pouch_updateDoc({ docRev, validatedData }: { docRev: string; validatedData: any }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.put({ ...validatedData, _rev: docRev });
  }

  async getList({ featureEntity, size, skip }: { featureEntity: string; size?: number | null; skip?: number | null }) {
    if (this.isLocalFirst()) {
      return await this.pouch_getList({
        featureEntity,
        size,
        skip,
      });
    }
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

  private async pouch_getList({
    featureEntity,
    size,
    skip,
  }: {
    featureEntity: string;
    size?: number | null;
    skip?: number | null;
  }) {
    const db01 = await this.pouch_getDbInstance();
    const data = await db01.allDocs({
      include_docs: true,
      startkey: featureEntity,
      endkey: `${featureEntity}\ufff0`,
      inclusive_end: true,
      limit: size ?? undefined,
      skip: skip ?? undefined,
    });
    return data;
  }

  async findPartitionedDocs({ featureEntity, selector, fields, use_index, sort, limit, skip }: IFindOptions) {
    if (this.isLocalFirst()) {
      return await this.pouch_findPartitionedDocs({
        featureEntity,
        selector,
        fields,
        use_index,
        sort,
        limit,
        skip,
      });
    }
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

  private async pouch_findPartitionedDocs({
    featureEntity,
    selector,
    fields,
    use_index,
    sort,
    limit,
    skip,
  }: IFindOptions) {
    const db01 = await this.pouch_getDbInstance();

    const paramsQ = {
      selector: { ...selector },
      fields,
      use_index,
      sort: sort?.length ? sort : undefined,
      limit,
      skip,
    };

    LoggingService.logAsString(paramsQ);

    return await db01.find(paramsQ);
  }

  async deleteById({ docRev, nativeId }: { docRev: string; nativeId: string }) {
    const db01 = await this.getDocInstance();
    return await db01.destroy(nativeId, docRev);
  }

  async getInstance() {
    return await concurrency(() => this.getInstanceBase());
  }

  private async getInstanceBase() {
    if (this.baseConfig?.configType !== "REMOTE_FIRST") {
      throw new MocodyGenericError("Remote database uri not defined");
    }
    const { pouchConfig } = this.baseConfig;

    if (!pouchConfig?.databaseName) {
      throw new MocodyGenericError("Remote database config not defined");
    }

    if (!this._databaseInstance) {
      // http://username:password@hostname:port

      const { host, port, cookie } = pouchConfig;
      const protocol = pouchConfig.protocol || "http";

      if (pouchConfig.password && pouchConfig.username) {
        const pw = encodeURIComponent(pouchConfig.password);
        const uname = encodeURIComponent(pouchConfig.username);

        const url = `${protocol}://${uname}:${pw}@${host}:${port}`;
        this._databaseInstance = Nano({ url, cookie });
      } else {
        const url = `${protocol}://${host}:${port}`;
        this._databaseInstance = Nano({ url, cookie });
      }
    }
    return await Promise.resolve(this._databaseInstance);
  }

  async pouch_getDbInstance() {
    return await concurrency(() => this.pouch_getDbInstanceBase());
  }

  private isLocalFirst() {
    return this.baseConfig?.configType === "LOCAL_FIRST";
  }

  private getRemoteFirstConfig() {
    if (this.baseConfig?.configType !== "REMOTE_FIRST") {
      throw new MocodyGenericError("REMOTE_FIRST not configured");
    }
    if (!this.baseConfig?.pouchConfig) {
      throw new MocodyGenericError("couchDbUri not defined");
    }
    return this.baseConfig;
  }

  private async pouch_getDbInstanceBase<T>() {
    if (!this._pouchInstance) {
      if (this.baseConfig?.configType !== "LOCAL_FIRST") {
        throw new MocodyGenericError("LOCAL_FIRST not configured");
      }
      if (!this.baseConfig?.sqliteDbFilePath) {
        throw new MocodyGenericError("sqliteDbFilePath not defined");
      }

      const { sqliteDbFilePath, liveSync, indexes } = this.baseConfig;

      PouchDB.plugin(require("pouchdb-adapter-node-websql"));
      PouchDB.plugin(pouchdbFind);

      this._pouchInstance = new PouchDB(sqliteDbFilePath, { adapter: "websql" });

      try {
        if (indexes?.length) {
          for (const indexItem of indexes) {
            await this.pouch_createIndex({ ...indexItem, dbx: this._pouchInstance });
          }
        }
      } catch (error) {
        LoggingService.error(error);
      }

      if (this.baseConfig.couchConfig) {
        const { password, protocol, username, host, port } = this.baseConfig.couchConfig;

        const pw = encodeURIComponent(password);
        const uname = encodeURIComponent(username);

        const couchDbSyncUri = `${protocol}://${uname}:${pw}@${host}:${port}`;

        if (liveSync) {
          this._pouchInstance
            .sync(couchDbSyncUri, {
              live: true,
              retry: true,
            })
            .on("change", (change) => {
              // yo, something changed!
              LoggingService.log({ replication_change: change });
            })
            .on("paused", (info) => {
              // replication was paused, usually because of a lost connection
              LoggingService.log({ replication_info: info });
            });
        } else {
          this._pouchInstance.sync(couchDbSyncUri).on("error", (err) => {
            // totally unhandled error (shouldn't happen)
            LoggingService.log({ replication_error: err });
          });
        }
      }
      await UtilService.waitUntilMilliseconds(800);
    }
    return await Promise.resolve(this._pouchInstance as PouchDB.Database<IBaseDef<T>>);
  }
}
