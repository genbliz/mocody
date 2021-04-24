import { DynamoDB, DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";
import throat from "throat";
const concurrency = throat(1);

export class MocodyInitializerDynamo {
  private _dynamoDb!: DynamoDB | null;
  private readonly _inits: DynamoDBClientConfig;

  constructor(inits: DynamoDBClientConfig) {
    this._inits = inits;
  }

  async getInstance() {
    return await concurrency(() => this.getInstanceBase());
  }

  private async getInstanceBase() {
    if (!this._dynamoDb) {
      this._dynamoDb = new DynamoDB(this._inits);
    }
    return await Promise.resolve(this._dynamoDb);
  }
}
