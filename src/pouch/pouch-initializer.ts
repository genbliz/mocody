import Nano from "nano";
import throat from "throat";
import PouchDB from "pouchdb";
import nodAdapter from "pouchdb-adapter-node-websql";
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
  couchDbSyncUri?: string;
  liveSync?: boolean;
  indexes?: { indexName: string; fields: string[] }[];
};

type IRemoteFirstConfig = {
  configType: "REMOTE_FIRST";
  couchDbUri: string;
  databaseName: string;
  indexes?: { indexName: string; fields: string[] }[];
};

interface IOptions {
  //http://admin:mypassword@localhost:5984
  pouchConfig: ILocalFirstConfig | IRemoteFirstConfig;
}

export class MocodyInitializerPouch {
  private _databaseInstance!: Nano.ServerScope | null;
  private _pouchInstance!: PouchDB.Database<IBaseDef<any>> | null;
  private _documentScope!: Nano.DocumentScope<any> | null;

  private readonly pouchConfig: IOptions["pouchConfig"];

  constructor({ pouchConfig }: IOptions) {
    this.pouchConfig = pouchConfig;
  }

  async deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    if (this.isLocalFirst()) {
      return await this.pouch_deleteIndex({ ddoc, name });
    }
    // DELETE /{db}/_index/{designdoc}/json/{name}
    const path = ["_index", ddoc, "json", name].join("/");
    const instance = await this.getInstance();
    const { databaseName } = this.getRemoteFirstConfig();

    const result: { ok: boolean } = await instance.relax({
      db: databaseName,
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

    const { databaseName } = this.getRemoteFirstConfig();

    const instance = await this.getInstance();
    const result: IIndexList = await instance.request({
      db: databaseName,
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
      const { databaseName, indexes } = this.getRemoteFirstConfig();

      const serverScope = await this.getInstance();
      const db = serverScope.db.use<IBaseDef<T>>(databaseName);

      try {
        if (indexes?.length) {
          for (const indexItem of indexes) {
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
    return await db01.find({
      selector: { ...selector },
      fields,
      use_index,
      sort: sort?.length ? sort : undefined,
      limit,
      skip,
    });
  }

  async deleteById({ docRev, nativeId }: { docRev: string; nativeId: string }) {
    const db01 = await this.getDocInstance();
    return await db01.destroy(nativeId, docRev);
  }

  async getInstance() {
    return await concurrency(() => this.getInstanceBase());
  }

  private async getInstanceBase() {
    if (this.pouchConfig?.configType !== "REMOTE_FIRST") {
      throw new MocodyGenericError("Remote database uri not defined");
    }
    const { couchDbUri } = this.pouchConfig;

    if (!couchDbUri) {
      throw new MocodyGenericError("Remote database uri not defined");
    }

    if (!this._databaseInstance) {
      this._databaseInstance = Nano(couchDbUri);
    }
    return await Promise.resolve(this._databaseInstance);
  }

  async pouch_getDbInstance() {
    return await concurrency(() => this.pouch_getDbInstanceBase());
  }

  private isLocalFirst() {
    return this.pouchConfig?.configType === "LOCAL_FIRST";
  }

  private getRemoteFirstConfig() {
    if (this.pouchConfig?.configType !== "REMOTE_FIRST") {
      throw new MocodyGenericError("REMOTE_FIRST not configured");
    }
    if (!this.pouchConfig?.couchDbUri) {
      throw new MocodyGenericError("couchDbUri not defined");
    }
    return this.pouchConfig;
  }

  private async pouch_getDbInstanceBase<T>() {
    if (!this._pouchInstance) {
      if (this.pouchConfig?.configType !== "LOCAL_FIRST") {
        throw new MocodyGenericError("LOCAL_FIRST not configured");
      }
      if (!this.pouchConfig?.sqliteDbFilePath) {
        throw new MocodyGenericError("sqliteDbFilePath not defined");
      }

      const { sqliteDbFilePath, couchDbSyncUri, liveSync, indexes } = this.pouchConfig;

      PouchDB.plugin(nodAdapter);
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

      if (couchDbSyncUri) {
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
