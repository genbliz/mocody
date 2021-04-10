import { FuseInitializerDynamo } from "./../dynamo/dynamo-initializer";
import { LoggingService } from "../helpers/logging-service";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

class DynamoConnectionBase {
  private _dynamoDbClient: DynamoDBClient;
  private _dynamoConn!: FuseInitializerDynamo;

  constructor() {
    const region = "us-west-2";
    this._dynamoDbClient = new DynamoDBClient({
      apiVersion: "2012-08-10",
      region,
    });
    LoggingService.log(`Initialized DynamoDb, region: ${region}`);
  }

  dynamoDbClientInst() {
    return this._dynamoDbClient;
  }

  getDynamoConnection() {
    if (!this._dynamoConn) {
      this._dynamoConn = new FuseInitializerDynamo({
        region: "us-east-2",
      });
      console.log({ getDynamoConnection_INITIALIZED: true });
    } else {
      // console.log({ getDynamoConnection_RE_USED: true });
    }
    return this._dynamoConn;
  }
}

export const MyDynamoConnection = new DynamoConnectionBase();
