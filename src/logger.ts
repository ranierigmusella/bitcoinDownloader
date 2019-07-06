export class Logger {
  public static info(obj:any)   { 
    console.log("\x1b[36m%s %s\x1b[0m", '[INF]' , obj.toString());
  }
  public static warning(obj:any){ 
    console.log("\x1b[33m%s %s\x1b[0m", '[WAR]' , obj.toString());
  }
  public static error(obj:any)  { 
    console.log("\x1b[31m%s %s\x1b[0m", '[ERR]' , obj.toString());
  }
  public static debug(obj: any) { 
    console.log("\x1b[32m%s %s\x1b[0m", '[DEB]' , obj.toString());
  }

}
