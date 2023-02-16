import { LoggingService } from "../helpers/logging-service";
import type { IMocodyIndexDefinition } from "../type";
import type { MocodyInitializerPouch } from "./pouch-initializer";

interface ITableOptions<T> {
  pouchDb: () => MocodyInitializerPouch;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

export class PouchManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly pouchDb: () => MocodyInitializerPouch;
  private readonly tableFullName: string;
  private readonly secondaryIndexOptions: IMocodyIndexDefinition<T>[];

  constructor({
    pouchDb,
    secondaryIndexOptions,
    tableFullName,
    partitionKeyFieldName,
    sortKeyFieldName,
  }: ITableOptions<T>) {
    this.pouchDb = pouchDb;
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
    return this.pouchDb();
  }

  async mocody_createIndex({ indexName, fields }: { indexName: string; fields: string[] }) {
    const instance = await this._mocody_getInstance().getDocInstance();
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

  async mocody_clearAllIndexes() {
    const indexes = await this._mocody_getInstance().getIndexes();
    LoggingService.log({ indexes });
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

  async mocody_getIndexes() {
    return await this._mocody_getInstance().getIndexes();
  }

  async mocody_createDatabase() {
    return await this._mocody_getInstance().createDatabase();
  }

  async mocody_createDefinedIndexes(): Promise<string[]> {
    const results: string[] = [];
    if (this.secondaryIndexOptions?.length) {
      for (const indexOption of this.secondaryIndexOptions) {
        if (indexOption?.indexName) {
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
