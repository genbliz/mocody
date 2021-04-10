import { DynamoDB, DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";

export class MocodyInitializerDynamo {
  private _dynamoDb: DynamoDB;
  private readonly _inits: DynamoDBClientConfig;

  constructor(inits: DynamoDBClientConfig) {
    this._inits = inits;
    this._dynamoDb = new DynamoDB(inits);
  }

  getInstance() {
    if (!this._dynamoDb) {
      this._dynamoDb = new DynamoDB(this._inits);
    }
    return this._dynamoDb;
  }
}
