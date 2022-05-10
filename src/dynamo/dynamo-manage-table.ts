import { IMocodyCoreEntityModel } from "../core/base-schema";
import { IMocodyIndexDefinition } from "../type";
import { UtilService } from "../helpers/util-service";
import { LoggingService } from "../helpers/logging-service";
import {
  TableDescription,
  ProjectionType,
  DescribeTableCommandInput,
  UpdateTableCommandInput,
  UpdateTimeToLiveCommandInput,
  CreateTableCommandInput,
  ListTablesCommandInput,
  UpdateTableCommand,
  CreateTableCommand,
  DescribeTableCommand,
  UpdateTimeToLiveCommand,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";
import { MocodyInitializerDynamo } from "./dynamo-initializer";

interface ITableOptions<T> {
  dynamoDb: () => MocodyInitializerDynamo;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  tableFullName: string;
  partitionKeyFieldName: string;
  sortKeyFieldName: string;
}

export class DynamoManageTable<T> {
  private readonly partitionKeyFieldName: string;
  private readonly sortKeyFieldName: string;
  private readonly dynamoDb: () => MocodyInitializerDynamo;
  private readonly tableFullName: string;
  private readonly secondaryIndexOptions: IMocodyIndexDefinition<T>[];

  constructor({
    dynamoDb,
    secondaryIndexOptions,
    tableFullName,
    partitionKeyFieldName,
    sortKeyFieldName,
  }: ITableOptions<T>) {
    this.dynamoDb = dynamoDb;
    this.tableFullName = tableFullName;
    this.partitionKeyFieldName = partitionKeyFieldName;
    this.sortKeyFieldName = sortKeyFieldName;
    this.secondaryIndexOptions = secondaryIndexOptions;
  }

  private _tbl_getLocalVariables() {
    return {
      partitionKeyFieldName: this.partitionKeyFieldName,
      sortKeyFieldName: this.sortKeyFieldName,
      //
      tableFullName: this.tableFullName,
      secondaryIndexOptions: this.secondaryIndexOptions,
    } as const;
  }

  private _tbl_dynamoDb() {
    return this.dynamoDb();
  }

  async mocody_tbl_getListOfTablesNamesOnline() {
    const params: ListTablesCommandInput = {
      Limit: 99,
    };
    const dynamo = await this._tbl_dynamoDb().getInstance();
    const listOfTables = await dynamo.send(new ListTablesCommand(params));
    return listOfTables?.TableNames;
  }

  async mocody_updateTTL({ isEnabled }: { isEnabled: boolean }) {
    const { tableFullName } = this._tbl_getLocalVariables();
    const fieldName: keyof IMocodyCoreEntityModel = "dangerouslyExpireAtTTL";
    const params: UpdateTimeToLiveCommandInput = {
      TableName: tableFullName,
      TimeToLiveSpecification: {
        AttributeName: fieldName,
        Enabled: isEnabled,
      },
    };
    const dynamo = await this._tbl_dynamoDb().getInstance();
    const result = await dynamo.send(new UpdateTimeToLiveCommand(params));
    if (result?.TimeToLiveSpecification) {
      return result.TimeToLiveSpecification;
    }
    return null;
  }

  async mocody_tbl_getTableInfo() {
    try {
      const { tableFullName } = this._tbl_getLocalVariables();

      const params: DescribeTableCommandInput = {
        TableName: tableFullName,
      };
      const dynamo = await this._tbl_dynamoDb().getInstance();
      const result = await dynamo.send(new DescribeTableCommand(params));
      if (result?.Table?.TableName === tableFullName) {
        return result.Table;
      }
      return null;
    } catch (error: any) {
      LoggingService.log({ "@allGetTableInfoBase": "", error: error?.message });
      return null;
    }
  }

  async mocody_tbl_checkTableExists() {
    const result = await this.mocody_tbl_getTableInfo();
    if (!result) {
      return false;
    }
    if (result?.GlobalSecondaryIndexes) {
      //
    }
    return true;
  }

  private async _allUpdateGlobalSecondaryIndexBase({
    secondaryIndexOptions,
    existingTableInfo,
  }: {
    secondaryIndexOptions: IMocodyIndexDefinition<T>[];
    existingTableInfo: TableDescription;
  }): Promise<TableDescription[] | null> {
    try {
      const existingIndexNames: string[] = [];
      const staledIndexNames: string[] = [];
      const allIndexNames: string[] = [];
      const newSecondaryIndexOptions: IMocodyIndexDefinition<T>[] = [];

      const updateResults: TableDescription[] = [];

      if (existingTableInfo?.GlobalSecondaryIndexes?.length) {
        existingTableInfo?.GlobalSecondaryIndexes.forEach((indexInfo) => {
          if (indexInfo.IndexName) {
            existingIndexNames.push(indexInfo.IndexName);
          }
        });
      }

      secondaryIndexOptions?.forEach((newIndexInfo) => {
        allIndexNames.push(newIndexInfo.indexName);
        const indexExists = existingIndexNames.includes(newIndexInfo.indexName);
        if (!indexExists) {
          newSecondaryIndexOptions.push(newIndexInfo);
        }
      });

      existingIndexNames.forEach((indexName) => {
        const existsInList = allIndexNames.includes(indexName);
        if (!existsInList) {
          staledIndexNames.push(indexName);
        }
      });

      if (!(newSecondaryIndexOptions.length || staledIndexNames.length)) {
        return null;
      }

      let canUpdate = false;

      const { tableFullName } = this._tbl_getLocalVariables();

      LoggingService.log({
        secondaryIndexOptions: secondaryIndexOptions.length,
        newSecondaryIndexOptions: newSecondaryIndexOptions.length,
        staledIndexNames: staledIndexNames.length,
        tableName: tableFullName,
      });

      if (newSecondaryIndexOptions.length) {
        canUpdate = true;
        let indexCount = 0;
        for (const indexOption of newSecondaryIndexOptions) {
          const params: UpdateTableCommandInput = {
            TableName: tableFullName,
            GlobalSecondaryIndexUpdates: [],
          };
          indexCount++;

          const creationParams = this._getGlobalSecondaryIndexCreationParams({
            secondaryIndexOptions: [indexOption],
          });

          const indexName = creationParams.xGlobalSecondaryIndex[0].IndexName;

          params.AttributeDefinitions = [...creationParams.xAttributeDefinitions];

          creationParams.xGlobalSecondaryIndex.forEach((gsi) => {
            params.GlobalSecondaryIndexUpdates?.push({
              Create: {
                ...gsi,
              },
            });
          });

          const dynamo = await this._tbl_dynamoDb().getInstance();
          const result = await dynamo.send(new UpdateTableCommand(params));
          if (result?.TableDescription) {
            updateResults.push(result?.TableDescription);
          }

          LoggingService.log(
            [
              //
              `Creating Index: '${indexName}'`,
              `on table '${tableFullName}' started:`,
              new Date().toTimeString(),
            ].join(" "),
          );

          if (indexCount !== newSecondaryIndexOptions.length) {
            await UtilService.waitUntilMunites(5);
          }
        }
      }

      if (staledIndexNames.length) {
        if (canUpdate) {
          await UtilService.waitUntilMunites(4);
        }
        canUpdate = true;
        let indexCount = 0;

        for (const indexName of staledIndexNames) {
          const params: UpdateTableCommandInput = {
            TableName: tableFullName,
            GlobalSecondaryIndexUpdates: [],
          };

          indexCount++;

          params.GlobalSecondaryIndexUpdates?.push({
            Delete: {
              IndexName: indexName,
            },
          });

          const dynamo = await this._tbl_dynamoDb().getInstance();
          const result = await dynamo.send(new UpdateTableCommand(params));
          if (result?.TableDescription) {
            updateResults.push(result?.TableDescription);
          }

          LoggingService.log(
            [
              //
              `Deleting Index: '${indexName}'`,
              `on table '${tableFullName}' started:`,
              new Date().toTimeString(),
            ].join(" "),
          );

          if (indexCount !== staledIndexNames.length) {
            await UtilService.waitUntilMunites(1);
          }
        }
      }

      if (!canUpdate) {
        return null;
      }

      if (updateResults.length) {
        LoggingService.log({
          "@allCreateGlobalSecondaryIndexBase": "",
          updateResults,
        });
        return updateResults;
      }
      return null;
    } catch (error: any) {
      LoggingService.log({
        "@allCreateGlobalSecondaryIndexBase": "",
        error: error?.message,
      });
      return null;
    }
  }

  async mocody_createOrUpdateDefinedIndexes() {
    const { secondaryIndexOptions } = this._tbl_getLocalVariables();

    const existingTableInfo = await this.mocody_tbl_getTableInfo();
    if (existingTableInfo) {
      if (secondaryIndexOptions?.length) {
        const result01 = await this._allUpdateGlobalSecondaryIndexBase({
          secondaryIndexOptions,
          existingTableInfo,
        });
        LoggingService.log(result01);
        return result01;
      }
    }
    LoggingService.log("Table does not exists");
    return null;
  }

  async mocody_tbl_createTableIfNotExists() {
    const { secondaryIndexOptions } = this._tbl_getLocalVariables();

    const existingTableInfo = await this.mocody_tbl_getTableInfo();
    if (existingTableInfo) {
      if (secondaryIndexOptions?.length) {
        await this._allUpdateGlobalSecondaryIndexBase({
          secondaryIndexOptions,
          existingTableInfo,
        });
      } else if (existingTableInfo.GlobalSecondaryIndexes?.length) {
        await this._allUpdateGlobalSecondaryIndexBase({
          secondaryIndexOptions: [],
          existingTableInfo,
        });
      }
      return null;
    }
    return await this.mocody_tbl_createTable();
  }

  private _getGlobalSecondaryIndexCreationParams({
    secondaryIndexOptions,
  }: {
    secondaryIndexOptions: IMocodyIndexDefinition<T>[];
  }) {
    const { tableFullName } = this._tbl_getLocalVariables();
    const params: CreateTableCommandInput = {
      KeySchema: [], //  make linter happy
      AttributeDefinitions: [],
      TableName: tableFullName,
      GlobalSecondaryIndexes: [],
    };

    const attributeDefinitionsNameList: string[] = [];

    secondaryIndexOptions.forEach((sIndex) => {
      const {
        indexName,
        partitionKeyFieldName: keyFieldName,
        sortKeyFieldName: sortFieldName,
        dataType,
        projectionFieldsInclude,
      } = sIndex;
      //
      const _keyFieldName = keyFieldName as string;
      const _sortFieldName = sortFieldName as string;

      if (!attributeDefinitionsNameList.includes(_keyFieldName)) {
        attributeDefinitionsNameList.push(_keyFieldName);
        params?.AttributeDefinitions?.push({
          AttributeName: _keyFieldName,
          AttributeType: dataType,
        });
      }

      if (!attributeDefinitionsNameList.includes(_sortFieldName)) {
        attributeDefinitionsNameList.push(_sortFieldName);
        params?.AttributeDefinitions?.push({
          AttributeName: _sortFieldName,
          AttributeType: dataType,
        });
      }

      let projectionFields = (projectionFieldsInclude || []) as string[];
      let projectionType: ProjectionType = "ALL";

      if (projectionFields?.length) {
        // remove frimary keys from include
        projectionFields = projectionFields.filter((field) => {
          return field !== _keyFieldName && field !== _sortFieldName;
        });
        if (projectionFields.length === 0) {
          // only keys was projceted
          projectionType = "KEYS_ONLY";
        } else {
          // only keys was projceted
          projectionType = "INCLUDE";
        }
      }

      params.GlobalSecondaryIndexes?.push({
        IndexName: indexName,
        Projection: {
          ProjectionType: projectionType,
          NonKeyAttributes: projectionType === "INCLUDE" ? projectionFields : undefined,
        },
        KeySchema: [
          {
            AttributeName: _keyFieldName,
            KeyType: "HASH",
          },
          {
            AttributeName: _sortFieldName,
            KeyType: "RANGE",
          },
        ],
      });
    });
    return {
      xAttributeDefinitions: params.AttributeDefinitions || [],
      xGlobalSecondaryIndex: params.GlobalSecondaryIndexes || [],
    };
  }

  async mocody_tbl_createTable() {
    const { partitionKeyFieldName, sortKeyFieldName, tableFullName, secondaryIndexOptions } =
      this._tbl_getLocalVariables();

    const params: CreateTableCommandInput = {
      AttributeDefinitions: [
        {
          AttributeName: partitionKeyFieldName,
          AttributeType: "S",
        },
        {
          AttributeName: sortKeyFieldName,
          AttributeType: "S",
        },
      ],
      KeySchema: [
        {
          AttributeName: partitionKeyFieldName,
          KeyType: "HASH",
        },
        {
          AttributeName: sortKeyFieldName,
          KeyType: "RANGE",
        },
      ],
      BillingMode: "PAY_PER_REQUEST",
      TableName: tableFullName,
    };

    params.Tags = [
      {
        Key: "tablePrefix",
        Value: tableFullName,
      },
      {
        Key: `DDBTableGroupKey-${tableFullName}`,
        Value: tableFullName,
      },
    ];

    if (secondaryIndexOptions?.length) {
      const creationParams = this._getGlobalSecondaryIndexCreationParams({
        secondaryIndexOptions,
      });
      if (creationParams.xAttributeDefinitions?.length) {
        const existAttrDefNames = params.AttributeDefinitions?.map((def) => def.AttributeName);
        creationParams.xAttributeDefinitions.forEach((def) => {
          const alreadyDefined = existAttrDefNames?.includes(def.AttributeName);
          if (!alreadyDefined) {
            params.AttributeDefinitions?.push(def);
          }
        });
      }
      params.GlobalSecondaryIndexes = [...creationParams.xGlobalSecondaryIndex];
    }

    LoggingService.log({
      "@allCreateTableBase, table: ": tableFullName,
    });

    const dynamo = await this._tbl_dynamoDb().getInstance();
    const result = await dynamo.send(new CreateTableCommand(params));

    if (result?.TableDescription) {
      LoggingService.log(
        [
          `@allCreateTableBase,`,
          `Created table: '${result?.TableDescription.TableName}'`,
          new Date().toTimeString(),
        ].join(" "),
      );
      return result.TableDescription?.TableName;
    }
    return null;
  }

  async mocody_tbl_deleteGlobalSecondaryIndex(indexName: string) {
    const { tableFullName } = this._tbl_getLocalVariables();

    const params: UpdateTableCommandInput = {
      TableName: tableFullName,
      GlobalSecondaryIndexUpdates: [
        {
          Delete: {
            IndexName: indexName,
          },
        },
      ],
    };
    const dynamo = await this._tbl_dynamoDb().getInstance();
    const result = await dynamo.send(new UpdateTableCommand(params));
    if (result?.TableDescription) {
      return result.TableDescription?.TableName;
    }
    return null;
  }
}
