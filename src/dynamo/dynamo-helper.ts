import { UtilService } from "./../helpers/util-service";

export function getDynamoRandomKeyOrHash(prefix: `:${string}` | `#${string}`) {
  return [prefix, UtilService.getRandomString(6)].join("").toLowerCase();
}
