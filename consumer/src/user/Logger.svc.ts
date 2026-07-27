import { Injectable } from "@nestjs/common";
@Injectable()
export class Loggsvc {
    log(msg: string) {
        console.log('[LOG]', msg)
    }
}