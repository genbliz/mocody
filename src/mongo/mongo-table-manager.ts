import { IMocodyCoreEntityModel } from "../core/base-schema";
import { LoggingService } from "./../helpers/logging-service";
import type { IMocodyIndexDefinition } from "../type";
import type { MocodyInitializerMongo } from "./mongo-initializer";

interface ITableOptions<T> {
  mongoDb: () => MocodyInitializerMongo;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

export class MongoManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly mongoDb: () => MocodyInitializerMongo;
  private readonly tableFullName: string;
  private readonly secondaryIndexOptions: IMocodyIndexDefinition<T>[];

  constructor({
    mongoDb,
    secondaryIndexOptions,
    tableFullName,
    partitionKeyFieldName,
    sortKeyFieldName,
  }: ITableOptions<T>) {
    this.mongoDb = mongoDb;
    this.tableFullName = tableFullName;
    this.partitionKeyFieldName = partitionKeyFieldName;
    this.sortKeyFieldName = sortKeyFieldName;
    this.secondaryIndexOptions = secondaryIndexOptions;
    this._trickLinter();
  }

  private _trickLinter() {
    if (this.partitionKeyFieldName && this.sortKeyFieldName && this.tableFullName) {
      //
    }
  }

  private _mocody_getInstance() {
    return this.mongoDb().getCustomCollectionInstance(this.tableFullName);
  }

  async mocody_createIndex({ indexName, fields }: { indexName: string; fields: string[] }): Promise<string> {
    const db = await this._mocody_getInstance();
    const indexObject: Record<string, 1 | -1> = {};
    fields.forEach((key) => {
      indexObject[key] = 1;
    });
    const result = await db.createIndex(indexObject, { name: indexName });
    LoggingService.log(result);
    return result;
  }

  async mocody_clearAllIndexes() {
    const db = await this._mocody_getInstance();
    const indexes = await this.mocody_getIndexes();
    const dropedIndexes: Record<string, any> = {};
    if (indexes) {
      for (const [name, value] of Object.entries(indexes)) {
        if (name !== "_id_") {
          await db.dropIndex(name);
          dropedIndexes[name] = value;
        }
      }
    }
    return {
      indexes,
      dropedIndexes,
    };
  }

  async mocody_getIndexes() {
    const db = await this._mocody_getInstance();
    return await db.indexes();
  }

  async mocody_createTTL(): Promise<string> {
    const db = await this._mocody_getInstance();
    const fieldName: keyof IMocodyCoreEntityModel = "dangerouslyExpireAtTTL";
    return await db.createIndex({ [fieldName]: 1 }, { expireAfterSeconds: 1 });
  }

  async mocody_createDefinedIndexes(): Promise<string[]> {
    const results: string[] = [];
    if (this.secondaryIndexOptions?.length) {
      for (const indexOption of this.secondaryIndexOptions) {
        if (indexOption.indexName) {
          const resultData = await this.mocody_createIndex({
            fields: [indexOption.partitionKeyFieldName, indexOption.sortKeyFieldName] as any[],
            indexName: indexOption.indexName,
          });
          LoggingService.log(resultData);
          results.push(resultData);
        }
      }
    }
    return results;
  }
}
