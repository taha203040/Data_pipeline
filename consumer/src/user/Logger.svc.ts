import { Injectable } from "@nestjs/common";
@Injectable()
export class Loggsvc {
    log(msg: string , Any ?:any , any ?:any) {
       console.log('[LOG]', msg)
    }
}