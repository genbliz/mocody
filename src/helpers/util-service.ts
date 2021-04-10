import { v4 as uuidv4 } from "uuid";

class UtilServiceBase {
  /** generate uuid */

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
    let text = "";
    const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    for (let i = 0; i < count; i++) {
      const random = Math.floor(Math.random() * possible.length);
      text += possible[random];
    }
    return text;
  }

  getRandomAlphabet(count: number) {
    let txt = "";
    const alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    for (let i = 0; i < count; i++) {
      const random = Math.floor(Math.random() * alphabets.length);
      txt += alphabets[random];
    }
    return txt;
  }

  camelCaseToSentenceCase(text: string) {
    const result = text.replace(/([A-Z])/g, " $1");
    const rst = result.charAt(0).toUpperCase() + result.slice(1);
    return rst.trim();
  }

  toTitleCase(text: string) {
    return text.replace(/\w\S*/g, (txt) => {
      return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
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
      setTimeout(() => {
        resolve();
      }, Math.round(seconds * 1000));
    });
  }

  waitUntilMunites(munites: number) {
    return new Promise<void>((resolve) => {
      setTimeout(() => {
        resolve();
      }, Math.round(munites * 60 * 1000));
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
      uuidv4(),
    ];
    return key.join("");
  }

  encodeStringToBase64(str: string) {
    return Buffer.from(str).toString("base64");
  }

  decodeStringFromBase64(str: string) {
    return Buffer.from(str, "base64").toString();
  }

  isNumberic(val: any) {
    return !isNaN(Number(val));
  }
}

export const UtilService = new UtilServiceBase();
