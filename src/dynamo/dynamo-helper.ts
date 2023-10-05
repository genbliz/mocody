import { UtilService } from "./../helpers/util-service";

type IDictionaryAttr = { [key: string]: any };
type IQueryConditions = {
  xExpressionAttributeValues: IDictionaryAttr;
  xExpressionAttributeNames: IDictionaryAttr;
  xFilterExpression: string;
};

export function getDynamoRandomKeyOrHash(prefix: `:${string}` | `#${string}`) {
  return [prefix, UtilService.getRandomString(6)].join("").toLowerCase();
}

export class QueryConditionBuilder {
  private readonly expressAttrVal: IDictionaryAttr;
  private readonly expressAttrName: IDictionaryAttr;
  private readonly expressAttrNameInverse: IDictionaryAttr;
  private readonly filterExpressionValue01: string[];

  constructor() {
    this.expressAttrVal = {};
    this.expressAttrName = {};
    this.expressAttrNameInverse = {};
    this.filterExpressionValue01 = [];
  }

  addValue(queryval: any) {
    const attrKeyHash = getDynamoRandomKeyOrHash(":");
    this.expressAttrVal[attrKeyHash] = queryval;
    return attrKeyHash;
  }

  addName(fieldName: string) {
    if (!(fieldName && typeof fieldName === "string")) {
      throw new Error("Invalid field name::: Must be a string");
    }
    if (this.expressAttrNameInverse[fieldName]) {
      return this.expressAttrNameInverse[fieldName];
    }
    const attrKeyHash = getDynamoRandomKeyOrHash("#");
    this.expressAttrName[attrKeyHash] = fieldName;
    this.expressAttrNameInverse[fieldName] = attrKeyHash;
    return attrKeyHash;
  }

  addFilter(filter: string) {
    if (filter && typeof filter === "string") {
      this.filterExpressionValue01.push(filter);
    }
  }

  getRawFilterExpression() {
    return this.filterExpressionValue01;
  }

  getResult() {
    const result: IQueryConditions = {
      xExpressionAttributeValues: {
        ...this.expressAttrVal,
      },
      xExpressionAttributeNames: {
        ...this.expressAttrName,
      },
      xFilterExpression: this.filterExpressionValue01.join(" "),
    };
    return result;
  }
}
