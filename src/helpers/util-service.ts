import { randomUUID } from "node:crypto";
import { customAlphabet } from "nanoid";
import lodash from "lodash";

class UtilServiceBase {
  convertHexadecimalToNumber(hexString: string) {
    return parseInt(hexString, 16);
  }

  /**
   * Returns a random number between min (inclusive) and max (exclusive)
   */
  getRandomDecimal(min: number, max: number) {
    return Math.random() * (max - min) + min;
  }

  /**
   * Returns a random integer between min (inclusive) and max (inclusive)
   * Using Math.round() will give you a non-uniform distribution!
   */
  getRandomInt(min: number, max: number) {
    const minVal = Math.ceil(min);
    const maxVal = Math.floor(max);
    return Math.floor(Math.random() * (maxVal - minVal + 1)) + minVal;
  }

  getRandomString(count: number) {
    const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
    const nanoid01 = customAlphabet(alphabet, count);
    return nanoid01();
  }

  camelCaseToSentenceCase(text: string) {
    const result = text.replace(/([A-Z])/g, " $1");
    const rst = result.charAt(0).toUpperCase() + result.slice(1);
    return rst.trim();
  }

  toTitleCase(text: string) {
    return text.replace(/\w\S*/g, (txt) => {
      return txt[0].toUpperCase() + txt.slice(1).toLowerCase();
    });
  }

  isValidPhoneNumber(_str: string) {
    if (isNaN(Number(_str))) {
      return false;
    }
    const isNum = /^\d+$/.test(_str);
    return isNum;
  }

  convertObjectPlainObject<T = any>(objData: T) {
    const objDataPlain: T = JSON.parse(JSON.stringify(objData));
    return objDataPlain;
  }

  removeDuplicateString(items: string[]) {
    if (!Array.isArray(items)) {
      return [];
    }
    const unique = {} as any;
    items.forEach((i) => {
      if (!unique[i]) {
        unique[i] = true;
      }
    });
    return Object.keys(unique);
  }

  objectHasAnyProperty(obj: any): boolean {
    if (obj && typeof obj === "object") {
      return Object.keys(obj).length > 0;
    }
    return false;
  }

  waitUntilMilliseconds(ms: number) {
    return new Promise<void>((resolve, reject) => {
      setTimeout(() => {
        resolve();
      }, ms);
    });
  }

  waitUntilSeconds(seconds: number) {
    return new Promise<void>((resolve) => {
      setTimeout(
        () => {
          resolve();
        },
        Math.round(seconds * 1000),
      );
    });
  }

  waitUntilMunites(munites: number) {
    return new Promise<void>((resolve) => {
      setTimeout(
        () => {
          resolve();
        },
        Math.round(munites * 60 * 1000),
      );
    });
  }

  generateDynamoTableKey(date?: any) {
    const _now = date ? new Date(date) : new Date();
    const key = [
      `${_now.getFullYear()}`,
      `${_now.getMonth() + 1}`.padStart(2, "0"),
      `${_now.getDate()}`.padStart(2, "0"),
      "-",
      `${_now.getHours()}`.padStart(2, "0"),
      `${_now.getMinutes()}`.padStart(2, "0"),
      `${_now.getSeconds()}`.padStart(2, "0"),
      "-",
      randomUUID(),
    ];
    return key.join("");
  }

  encodeStringToBase64(str: string) {
    return Buffer.from(str).toString("base64");
  }

  decodeStringFromBase64(str: string) {
    return Buffer.from(str, "base64").toString();
  }

  isNumeric(n: string | number | null | undefined) {
    if (n === null || typeof n === "undefined" || typeof n === "boolean") {
      return false;
    }
    const nn = String(n);
    if (nn.trim() && !isNaN(Number(nn)) && isFinite(Number(nn)) && !isNaN(parseFloat(nn))) {
      return true;
    }
    return false;
  }

  isNumericInteger(n: string | number | null | undefined) {
    const nn = String(n);
    const numberOnly = /^\d+$/.test(nn);
    if (!numberOnly) {
      return false;
    }
    if (!this.isNumeric(n)) {
      return false;
    }
    const mInt = parseInt(Number(n).toString());
    if (mInt >= 1) {
      return true;
    }
    return false;
  }

  getEpochTime(date: string | Date) {
    /* https://www.epochconverter.com/ */
    const epoc = Math.floor(new Date(date).getTime() / 1000.0);
    return epoc;
  }

  encodeBase64(str: string) {
    return Buffer.from(JSON.stringify(str)).toString("base64");
  }

  decodeBase64(str: string) {
    return Buffer.from(str, "base64").toString();
  }

  groupOneBy<T>(dataList: T[], fn: (dt: T) => string | number) {
    const aggr: Record<string, T> = {};
    if (dataList?.length) {
      dataList.forEach((data) => {
        const groupId = fn(data);
        if (aggr[groupId] === undefined) {
          aggr[groupId] = data;
        }
      });
    }
    return aggr;
  }

  groupBy<T>(dataList: T[], fn: (dt: T) => string | number) {
    const aggr: Record<string, T[]> = {};
    if (dataList?.length) {
      dataList.forEach((data) => {
        const groupId = fn(data);
        if (!aggr[groupId]) {
          aggr[groupId] = [];
        }
        aggr[groupId]?.push(data);
      });
    }
    return aggr;
  }

  orderBy<T>(dataList: T[], fn: (dt: T) => string | number, order?: "asc" | "desc") {
    return lodash.orderBy(dataList, (f) => fn(f), order || "asc");
  }

  pickFromObject<T = Record<string, any>>({ dataObject, pickKeys }: { dataObject: T; pickKeys: (keyof T)[] }): T {
    if (!(dataObject && typeof dataObject === "object")) {
      return dataObject;
    }
    if (Array.isArray(dataObject)) {
      return dataObject;
    }
    const chosenDataObject: any = {};
    const allKeys = Object.keys(dataObject);
    const allKeysGrouped = this.groupOneBy(allKeys, (f) => f);

    pickKeys.forEach((key) => {
      if (allKeysGrouped[key as string] !== undefined) {
        chosenDataObject[key] = dataObject[key];
      }
    });
    return chosenDataObject;
  }

  deleteKeysFromObject<T = Record<string, any>>({ dataObject, delKeys }: { dataObject: T; delKeys: (keyof T | string)[] }): T {
    if (!(dataObject && typeof dataObject === "object")) {
      return dataObject;
    }
    if (Array.isArray(dataObject)) {
      return dataObject;
    }
    const chosenDataObject: any = {};
    Object.keys(dataObject).forEach((key) => {
      if (!delKeys.includes(key)) {
        chosenDataObject[key] = dataObject[key];
      }
    });
    return chosenDataObject;
  }
}

export const UtilService = new UtilServiceBase();
