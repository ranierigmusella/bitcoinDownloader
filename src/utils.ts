import {createHash, randomBytes} from 'crypto';
import { U64 } from 'n64';
import { MongoStorage } from './mongostorage';
import { Block, BlockChain } from './bitcointypes';
import { Logger } from './logger';
// import { ArangoStorage } from './arangostorage';

export function asciiToHex(str: string): string {
  let hex: string = '';
  for (let i = 0; i < str.length; i++) {
    hex += str.charCodeAt(i).toString(16);
  }
  return hex;
};

export function reverseHex(hexString: string): string {
  let hex = '';
  for (let i = hexString.length - 1; i >= 0; i -= 2) {
    hex += hexString[i - 1] + hexString[i];
  }
  return hex;
}

export function hexToASCII(str: string): string {
  let ascii = '';
  for (let i = 0; i < str.length / 2; i++)
    if (str.substr(i * 2, 2) != '00') ascii += String.fromCharCode(Number.parseInt(str.substr(i * 2, 2), 16));
  return ascii;
};

export function uint8ToHex(arr: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < arr.length; i++) {
    hex += (arr[i] < 16 ? '0' : '') + arr[i].toString(16);
  }
  return hex;
}

export function sha256(data: string | Buffer): Buffer { return new Buffer(createHash('sha256').update(data).digest('latin1'), 'latin1'); };

export function doubleSHA256(data: string | Buffer): Buffer { return sha256(sha256(data)); }

export function sleep(ms: number) { return new Promise(res => setTimeout(res, ms)) };

export function nonceGenerator(): U64 { return U64.readRaw(randomBytes(8), 0) };

/**
 * This function prints the buffer in formatted way
 * @param buff
 * @param showBytes
 */
export function printBuffer(buff: Buffer, showBytes: boolean = false) {
  for (let i = 0; i < Math.floor(buff.byteLength / 8); i++) {
    let s = '';
    for (let y = 0; y < 8; y++) {
      if (showBytes) s += (y + 8 * i) + ': ';
      if (buff[y + 8 * i] < 16) s += '0';
      s += buff[y + 8 * i].toString(16) + ' ';
    }
    console.log(s.trim());
  }
  let s: string = '';
  let offset = Math.floor(buff.byteLength / 8) * 8;
  for (let i = 0; i < buff.byteLength % 8; i++) {
    if (buff[offset + i] < 16) s += '0';
    s += buff[offset + i].toString(16) + ' ';
  }
  console.log(s.trim());
}
/** 
 * Functional for read the blockchain.
*/
export async function chainRoller(fun: (data: Array<Block>, height: number) => Promise<Array<any>>, forward: boolean = true, startHeight?: number, stopHeight?: number, window?: number): Promise<any> {
  let storage = await MongoStorage.getInstance();
  startHeight = startHeight ? startHeight : 0;
  stopHeight = stopHeight ? stopHeight : await Block.getMaxHeight();
  if (forward) {
    if (startHeight > stopHeight) return;
  } else if (startHeight < stopHeight) return;
  window = window ? window : 10000;
  let initWindow = window;
  let height = startHeight;
  let condDoWhile = true;
  let cond = true;
  let heightObj = {};
  let sortObj = forward ? { height: 1 } : { height: -1 };
  let ret = {};
  try{
    do {
      heightObj = forward ? { $gte: height, $lte: stopHeight } : { $lte: height, $gte: stopHeight };
    let ris = <Array<any>>await storage.readFrom('bitcoinBx', { height: heightObj/*, mainChain: true*/ }, { _id: 0 }, sortObj, window);
      [cond, ret, window] = await fun(ris, height);
      condDoWhile = ris.length > 0 && ris[ris.length - 1].height != stopHeight ? true : false;
      if (!window) window = initWindow;
      height += forward ? window - 1 : - window + 1;
    } while (condDoWhile && cond);
    return Array.isArray(ret) ? ret : [ret];
  } catch(err){
    Logger.error('chainroller: ' + err);
    return null;
  }

}

export async function backwardChainRoll(startingHeight?: number): Promise<{ parentHash: string, parentHeight: number, parentPrevBlock: string, childHash: string, childHeight: number }> {
  let res = await chainRoller((data, height) => {
    let ret: {parentHash: string, parentHeight: number, parentPrevBlock: string, childHash: string, childHeight: number} = {parentHash: "", parentHeight: -1, parentPrevBlock: "", childHash: "", childHeight: -1};
    let cond = true;
    for (let i = 0; i < data.length - 1; i++) {
      if (!(data[i].height == data[i + 1].height + 1 && data[i].prev_block == data[i + 1].hash)) {
        ret.parentHash = data[i].hash;
        ret.parentHeight = data[i].height;
        ret.parentPrevBlock = data[i].prev_block;
        ret.childHash = data[i + 1].hash;
        ret.childHeight = data[i + 1].height;
        cond = false;
        break;
      }
    }
    return Promise.resolve([cond, ret]);
  }, false, BlockChain.lastHeightInDB, null, 100);
  return res[0];
}

export async function forwardChainRoll(startingHeight?: number): Promise<{ parentHash: string, parentHeight: number, parentNextBlock: string, childHash: string, childHeight: number }> {
  let res = await chainRoller((data, height) => {
    let ret: {parentHash: string, parentHeight: number, parentNextBlock: string, childHash: string, childHeight: number} = {parentHash: "", parentHeight: -1, parentNextBlock: "", childHash: "", childHeight: -1};
    let cond = true;
    for (let i = 0; i < data.length - 1; i++) {
      if (!(data[i].height + 1 == data[i + 1].height //&& data[i].next_block == data[i + 1].hash
      )) {
        ret.parentHash = data[i].hash;
        ret.parentHeight = data[i].height;
        // ret.parentNextBlock = data[i].next_block;
        ret.childHash = data[i + 1].hash;
        ret.childHeight = data[i + 1].height;
        cond = false;
        break;
      }
    }
    return Promise.resolve([cond, ret]);
  }, true, null, null, 10000);
  return res[0];
}

export class SharableObjects {
  public static readonly MAX_IN_PEERS: number = 16;
  public static readonly nullBuffer = Buffer.alloc(0);
  public static readonly ERROR: number = 0;
  public static readonly MSG_TX: number = 1;
  public static readonly MSG_BLOCK: number = 2;
  public static readonly MSG_FILTERED_BLOCK: number = 3;
  public static readonly MSG_CMPCT_BLOCK: number = 4;
  public static readonly MAX_INV_ENTRIES: number = 50000;
  public static readonly MAX_BLOCK_SIZE: number = Math.pow(2, 20);
  public static readonly magicMainNet = new Buffer('f9beb4d9', 'hex');
  public static readonly magicMainNetString = 'f9beb4d9';
  public static myIPv4: string = '';

} 
