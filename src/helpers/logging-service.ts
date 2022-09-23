class LoggingServiceBase {
  private mode: boolean;

  constructor() {
    const mode = process.env.MOCODY_DEBUG_MODE;
    if (mode === "true") {
      this.mode = true;
    } else {
      this.mode = false;
    }
  }

  error(message: unknown) {
    try {
      if (this.mode) {
        console.error(message);
      }
    } catch (error) {
      //
    }
  }

  log(message: unknown, ...optionalParams: unknown[]) {
    try {
      if (this.mode) {
        if (optionalParams?.length) {
          console.log(message, optionalParams);
        } else {
          console.log(message);
        }
      }
    } catch (error) {
      //
    }
  }
}

export const LoggingService = new LoggingServiceBase();
