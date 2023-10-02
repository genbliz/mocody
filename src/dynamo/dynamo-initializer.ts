import {
  DynamoDBClientConfig,
  DynamoDBClient,
  PutItemCommandInput,
  PutItemCommand,
  GetItemCommandInput,
  GetItemCommand,
  TransactWriteItemsCommandInput,
  TransactWriteItemsCommand,
  BatchWriteItemCommandInput,
  BatchWriteItemCommand,
  BatchGetItemCommandInput,
  BatchGetItemCommand,
  DeleteItemCommandInput,
  DeleteItemCommand,
  ExecuteStatementCommandInput,
} from "@aws-sdk/client-dynamodb";

import { ExecuteStatementCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

import throat from "throat";
const concurrency = throat(1);

export class MocodyInitializerDynamo {
  private client: DynamoDBClient | null | undefined;
  private readonly _config: DynamoDBClientConfig;

  constructor(inits: DynamoDBClientConfig) {
    this._config = inits;
  }

  async getInstance() {
    return await concurrency(() => this.getInstanceBase());
  }

  async executeStatement(param: ExecuteStatementCommandInput){
    const client = await this.getInstance();
    const docClient = DynamoDBDocumentClient.from(client);
    const command = new ExecuteStatementCommand(param);
    const response = await docClient.send(command);
    return response
  }

  async putItem(params: PutItemCommandInput) {
    const client = await this.getInstance();
    return await client.send(new PutItemCommand(params));
  }

  async getItem(params: GetItemCommandInput) {
    const client = await this.getInstance();
    return await client.send(new GetItemCommand(params));
  }

  async transactWriteItems(params: TransactWriteItemsCommandInput) {
    const client = await this.getInstance();
    return await client.send(new TransactWriteItemsCommand(params));
  }

  async batchWriteItem(params: BatchWriteItemCommandInput) {
    const client = await this.getInstance();
    return await client.send(new BatchWriteItemCommand(params));
  }

  async batchGetItem(params: BatchGetItemCommandInput) {
    const client = await this.getInstance();
    return await client.send(new BatchGetItemCommand(params));
  }

  async deleteItem(params: DeleteItemCommandInput) {
    const client = await this.getInstance();
    return await client.send(new DeleteItemCommand(params));
  }

  private async getInstanceBase() {
    if (!this.client) {
      this.client = new DynamoDBClient(this._config);
    }
    return await Promise.resolve(this.client);
  }
}
