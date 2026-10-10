import type Nano from "nano";
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

type IOptions = {
  connType: "LOCAL_FIRST" | "REMOTE_FIRST";
  localSqliteDbFilePath?: string;
  remoteConnectionUrl?: string;
  liveSync?: boolean;
  indexes?: { indexName: string; fields: string[] }[];
};

export class MocodyInitializerPouch<T extends IMocodyCoreEntityModel> {
  private _pouchInstance: PouchDB.Database<T> | undefined;
  private readonly _docMetaFieldsObJ = this.docMetaFieldsObject();
  private readonly baseConfig: IOptions;

  constructor(baseConfig: IOptions) {
    this.baseConfig = baseConfig;
  }

  async deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    return await this.pouch_deleteIndex({ ddoc, name });
  }

  private async pouch_deleteIndex({ ddoc, name }: { ddoc: string; name: string }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.deleteIndex({ ddoc, name });
  }

  async getIndexes() {
    return await this.pouch_getIndexes();
  }

  private async pouch_getIndexes() {
    const db01 = await this.pouch_getDbInstance();
    return await db01.getIndexes();
  }

  async createIndex({ indexName, fields }: { indexName: string; fields: string[] }) {
    return await this.pouch_createIndex({ indexName, fields });
  }

  private async pouch_createIndex({
    indexName,
    fields,
    dbx,
  }: {
    indexName: string;
    fields: string[];
    dbx?: PouchDB.Database<T>;
  }) {
    const db01 = dbx || (await this.pouch_getDbInstance());
    await db01.createIndex({
      index: {
        fields: fields,
        name: indexName,
        ddoc: indexName,
        type: "json",
      },
    });
  }

  async getById({ nativeId }: { nativeId: string }) {
    const doc = await this.pouch_getById({ nativeId });
    return this.toBasicDoc_RemovePouchMeta(doc);
  }

  async pouch_getById({ nativeId }: { nativeId: string }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.get(nativeId);
  }

  async getManyByIds({ nativeIds }: { nativeIds: string[] }) {
    const results = await this.pouch_getManyByIds({ nativeIds });
    const dataList: T[] = [];
    for (const item of results.rows) {
      if (item && "doc" in item && item.doc) {
        const doc01 = this.toBasicDoc_RemovePouchMeta(item.doc);
        dataList.push(doc01);
      }
    }
    return dataList;
  }

  async pouch_getManyByIds({ nativeIds }: { nativeIds: string[] }) {
    const db01 = await this.pouch_getDbInstance();
    const dataList = await db01.allDocs({
      keys: nativeIds,
      include_docs: true,
    });
    return dataList;
  }

  async createDoc({ validatedData }: { validatedData: any }) {
    return await this.pouch_createDoc({ validatedData });
  }

  private async pouch_createDoc({ validatedData }: { validatedData: any }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.put(validatedData);
  }

  async updateDoc({ docRev, validatedData }: { docRev: string; validatedData: any }) {
    return await this.pouch_updateDoc({ validatedData, docRev });
  }

  private async pouch_updateDoc({ docRev, validatedData }: { docRev: string; validatedData: any }) {
    const db01 = await this.pouch_getDbInstance();
    return await db01.put({ ...validatedData, _rev: docRev });
  }

  async pouch_getList({ featureEntity, size, skip }: { featureEntity: string; size?: number | null; skip?: number | null }) {
    const db01 = await this.pouch_getDbInstance();
    const data01 = await db01.allDocs({
      include_docs: true,
      startkey: featureEntity,
      endkey: `${featureEntity}\ufff0`,
      inclusive_end: true,
      limit: size ?? undefined,
      skip: skip ?? undefined,
    });
    return data01;
  }

  async findPartitionedDocs({ featureEntity, selector, fields, use_index, sort, limit, skip }: IFindOptions) {
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

  private async pouch_findPartitionedDocs({ featureEntity, selector, fields, use_index, sort, limit, skip }: IFindOptions) {
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

  async deleteById({ nativeId }: { nativeId: string }) {
    const db01 = await this.pouch_deleteById({ nativeId });
    return db01;
  }

  async pouch_deleteById({ nativeId }: { nativeId: string }) {
    const db01 = await this.pouch_getDbInstance();
    const doc = await this.pouch_getById({ nativeId });
    const response = await db01.remove({
      _id: doc._id,
      _rev: doc._id,
    });
    return response;
  }

  async pouch_getDbInstance() {
    return await concurrency(() => this.pouch_getDbInstanceBase());
  }

  private async pouch_getDbInstanceBase() {
    if (this._pouchInstance) return this._pouchInstance;

    const {
      //
      liveSync,
      indexes,
      connType,
      localSqliteDbFilePath,
      remoteConnectionUrl,
    } = this.baseConfig;

    const isLocalFirst = connType === "LOCAL_FIRST";
    const isRemoteFirst = connType === "REMOTE_FIRST";

    if (isLocalFirst) {
      if (!localSqliteDbFilePath) {
        throw new MocodyGenericError("localSqliteDbFilePath not defined");
      }

      PouchDB.plugin(pouchdbFind);
      this._pouchInstance = new PouchDB(localSqliteDbFilePath, { adapter: "nodesqlite" });
    } else {
      if (!remoteConnectionUrl) {
        throw new MocodyGenericError("couchRemoteConnectionUrl not defined");
      }

      PouchDB.plugin(pouchdbFind);
      this._pouchInstance = new PouchDB(remoteConnectionUrl);
    }

    try {
      if (indexes?.length) {
        for (const indexItem of indexes) {
          await this.pouch_createIndex({ ...indexItem, dbx: this._pouchInstance });
        }
      }
    } catch (error) {
      LoggingService.error(error);
    }

    if (isLocalFirst && remoteConnectionUrl) {
      if (liveSync) {
        this._pouchInstance
          .sync(remoteConnectionUrl, {
            live: true,
            retry: true,
          })
          .on("change", (change) => {
            LoggingService.log({ replication_change: change });
          })
          .on("paused", (info) => {
            // replication was paused, usually because of a lost connection
            LoggingService.log({ replication_paused: info });
          })
          .on("error", (err) => {
            // totally unhandled error (shouldn't happen)
            LoggingService.log({ replication_error: err });
          });
      } else {
        this._pouchInstance.sync(remoteConnectionUrl).on("error", (err) => {
          // totally unhandled error (shouldn't happen)
          LoggingService.log({ replication_error: err });
        });
      }
    } else if (isRemoteFirst && localSqliteDbFilePath) {
      if (liveSync) {
        this._pouchInstance
          .sync(localSqliteDbFilePath, {
            live: true,
            retry: true,
          })
          .on("change", (change) => {
            LoggingService.log({ replication_change: change });
          })
          .on("paused", (info) => {
            // replication was paused, usually because of a lost connection
            LoggingService.log({ replication_paused: info });
          })
          .on("error", (err) => {
            // totally unhandled error (shouldn't happen)
            LoggingService.log({ replication_error: err });
          });
      } else {
        this._pouchInstance.sync(localSqliteDbFilePath).on("error", (err) => {
          // totally unhandled error (shouldn't happen)
          LoggingService.log({ replication_error: err });
        });
      }
    }

    await UtilService.waitUntilMilliseconds(800);

    return await Promise.resolve(this._pouchInstance);
  }

  private toBasicDoc_RemovePouchMeta(doc: PouchDB.Core.Document<T> | T | Partial<T>) {
    const cleanDoc = {} as T;
    if (!doc) return cleanDoc;
    for (const key in doc) {
      if (Object.prototype.hasOwnProperty.call(doc, key) && !this._docMetaFieldsObJ[key]) {
        cleanDoc[key] = doc[key];
      }
    }
    return cleanDoc;
  }

  private docMetaFieldsObject() {
    const docMetaFields = ["_conflicts", "_rev", "_revs_info", "_revisions", "_attachments"];
    const fld: Record<string, string> = {};
    docMetaFields.forEach((f) => {
      fld[f] = f;
    });
    return fld;
  }
}
