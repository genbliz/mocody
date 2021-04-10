import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

class MocodyUtilBase {
  mocody_marshallFromJson(jsonData: Record<string, any>) {
    const marshalled = marshall(jsonData, {
      convertClassInstanceToMap: true,
      convertEmptyValues: true,
      removeUndefinedValues: true,
    });
    return marshalled;
  }

  mocody_unmarshallToJson(dynamoData: Record<string, any>) {
    return unmarshall(dynamoData);
  }
}

export const MocodyUtil = new MocodyUtilBase();
