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

  getProjectionFields<T = any>({
    excludeFields,
    fields,
    entityFields,
  }: {
    excludeFields?: (keyof T)[];
    fields?: (keyof T)[];
    entityFields: (keyof T)[] | string[];
  }) {
    type TProj = keyof T;

    if (!fields?.length && !excludeFields?.length) {
      return undefined;
    }

    const fields01 = fields?.length ? Array.from(new Set([...fields])) : ([] as TProj[]);
    const excludeFields01 = excludeFields?.length ? Array.from(new Set([...excludeFields])) : ([] as TProj[]);

    if (!excludeFields01.length) {
      if (fields01.length) {
        return fields01;
      }
      return undefined;
    }

    let baseFields: TProj[] = [];

    if (fields01.length) {
      baseFields = [...fields01];
    } else {
      baseFields = [...(entityFields as any[])];
    }
    const fieldSet01 = new Set<TProj>();

    baseFields.forEach((field) => {
      if (!excludeFields01.includes(field)) {
        fieldSet01.add(field);
      }
    });

    if (fieldSet01.size > 0) {
      return [...fieldSet01];
    }
    return undefined;
  }
}

export const MocodyUtil = new MocodyUtilBase();
