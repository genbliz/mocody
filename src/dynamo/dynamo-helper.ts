import { UtilService } from "./../helpers/util-service";

const getRandom = () => [Math.round(Math.random() * 999)].join("");

export function getDynamoRandomKeyOrHash(prefix: `:${string}` | `#${string}`) {
  return [
    //
    prefix,
    UtilService.getRandomString(4),
    getRandom().slice(0, 2).padStart(2, "0"),
  ]
    .join("")
    .toLowerCase();
}
