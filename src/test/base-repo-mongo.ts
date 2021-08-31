import { UtilService } from "./../helpers/util-service";
import { MongoDataOperation } from "./../mongo/mongo-data-operation";
import type { IMocodyIndexDefinition } from "../type";
import Joi from "joi";
import { MongoConnection } from "./mongo-conn";

interface IBaseRepoOptions<T> {
  schemaSubDef: Joi.SchemaMap;
  featureEntityValue: string;
  secondaryIndexOptions: IMocodyIndexDefinition<T>[];
}

export abstract class BaseRepository<T> extends MongoDataOperation<T> {
  constructor({ schemaSubDef, secondaryIndexOptions, featureEntityValue }: IBaseRepoOptions<T>) {
    super({
      mongoDbInitializer: () => MongoConnection.getConnection(),
      baseTableName: "mongo_test_table_01",
      schemaDef: { ...schemaSubDef },
      secondaryIndexOptions,
      featureEntityValue: featureEntityValue,
      strictRequiredFields: [],
      dataKeyGenerator: () => UtilService.getRandomString(30),
    });
  }
}
