import { LoggingService } from "./../helpers/logging-service";
import type { IFuseIndexDefinition } from "./../type/types";
import type { FuseInitializerMongo } from "./mongo-initializer";

interface ITableOptions<T> {
  mongoDb: () => FuseInitializerMongo;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

interface IIndexModel {
  v: string;
  key: Record<string, 1 | -1>;
  name: string;
  ns: string;
}

export class MongoManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly mongoDb: () => FuseInitializerMongo;
  private readonly tableFullName: string;
  private readonly secondaryIndexOptions: IFuseIndexDefinition<T>[];

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
    return this.mongoDb().getDbInstance();
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
    const dropedIndexes: IIndexModel[] = [];
    if (indexes?.length) {
      for (const index01 of indexes) {
        if (index01.name !== "_id_") {
          await db.dropIndex(index01.name);
          dropedIndexes.push(index01);
        }
      }
    }
    return {
      indexes,
      dropedIndexes,
    };
  }

  async mocody_getIndexes(): Promise<IIndexModel[]> {
    const db = await this._mocody_getInstance();
    const indexes: IIndexModel[] = await db.indexes();
    return indexes;
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
