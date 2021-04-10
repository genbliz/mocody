import { LoggingService } from "./../helpers/logging-service";
import type { IFuseIndexDefinition } from "./../type/types";
import type { FuseInitializerCouch } from "./couch-initializer";

interface ITableOptions<T> {
  couchDb: () => FuseInitializerCouch;
  secondaryIndexOptions: IFuseIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

export class CouchManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly couchDb: () => FuseInitializerCouch;
  private readonly tableFullName: string;
  private readonly secondaryIndexOptions: IFuseIndexDefinition<T>[];

  constructor({
    couchDb,
    secondaryIndexOptions,
    tableFullName,
    partitionKeyFieldName,
    sortKeyFieldName,
  }: ITableOptions<T>) {
    this.couchDb = couchDb;
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
    return this.couchDb();
  }

  async mocody_createIndex({ indexName, fields }: { indexName: string; fields: string[] }) {
    const result = await this._mocody_getInstance()
      .getDocInstance()
      .createIndex({
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

  async mocody_clearAllIndexes() {
    const indexes = await this._mocody_getInstance().getIndexes();
    if (indexes?.indexes?.length) {
      const deletedIndexes: any[] = [];
      for (const index of indexes.indexes) {
        if (index?.type !== "special") {
          deletedIndexes.push(index);
          await this._mocody_getInstance().deleteIndex({
            ddoc: index.ddoc,
            name: index.name,
          });
        }
      }
      return {
        deletedIndexes,
      };
    }
    return {
      deletedIndexes: [],
    };
  }

  mocody_getIndexes() {
    return this._mocody_getInstance().getIndexes();
  }

  mocody_createDatabase() {
    return this._mocody_getInstance().createDatabase();
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
          results.push(resultData.result);
        }
      }
    }
    return results;
  }
}
