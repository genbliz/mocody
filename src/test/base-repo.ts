import { DynamoDataOperation } from "./../dynamo/dynamo-data-operation";
import type { IMocodyIndexDefinition } from "../type";
import { MyDynamoConnection } from "./dynamo-conn";
import type { ISchemaMap } from "../helpers/base-joi-helper";

interface IBaseRepoOptions<T> {
  schemaSubDef: ISchemaMap;
  featureEntityValue: string;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
}

export abstract class BaseRepository<T> extends DynamoDataOperation<T> {
  constructor({ schemaSubDef, secondaryIndexOptions, featureEntityValue }: IBaseRepoOptions<T>) {
    super({
      dynamoDbInitializer: () => MyDynamoConnection.getDynamoConnection(),
      baseTableName: "mocody_dynamo_test_table_01",
      schemaDef: { ...schemaSubDef },
      secondaryIndexOptions,
      featureEntityValue: featureEntityValue,
      strictRequiredFields: [],
      dataKeyGenerator: () => Date.now().toString(),
    });
  }
}
