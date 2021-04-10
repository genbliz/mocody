class LoggingServiceBase {
  private mode: boolean;

  constructor() {
    const mode = process.env.FUSE_DYNAMO_COUCH_DEBUG_MODE;
    if (mode === "true") {
      this.mode = true;
    } else {
      this.mode = false;
    }
  }

  log(message: any, ...optionalParams: any[]) {
    try {
      if (this.mode) {
        console.log(message, optionalParams);
      }
    } catch (error) {
      //
    }
  }
}

export const LoggingService = new LoggingServiceBase();
