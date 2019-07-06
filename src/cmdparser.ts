import { PeerManager } from './peermanager';
import { Logger } from './logger';
import { EventEmitter } from 'events';
import { MongoStorage, MongoStorageState } from './mongostorage';
import {asciiToHex, hexToASCII, nonceGenerator, doubleSHA256,reverseHex} from './utils';
import { Block, BufferCoin, InventoryEntry, NetAdress, Peer, Tx, VarInt, VarStr, BlockChain } from './bitcointypes';
import { U64, I64 } from 'n64';
import * as prex from 'prex';
import { SharableObjects } from './utils';

class Command { constructor(public cmd: String, public data: Buffer, public peer: Peer) { } }

class CommandsQueque {
  private dataContainer: Array<Command> = new Array<Command>();

  public pushCommand(cmd: String, data: Buffer, peer: Peer) {
    this.dataContainer.push(new Command(cmd, data, peer));
  }
  public shiftCommand(): Command {
    return this.dataContainer.shift();
  }

  public getLength(): number { return this.dataContainer.length; }

  public empty() { this.dataContainer = new Array<Command>(); }

  public hasBlock(): boolean {
    for(let el of this.dataContainer){
      if(el.cmd == 'block')
        return true;
    }
    return false;
	}
}

export class CmdParser extends EventEmitter {

  public blockLock = new prex.Semaphore(1);
  public commandsQueque = new CommandsQueque();
  public version = 60002;

  private parseCycle = false;

  constructor() {
    super();
  }

  /**
   * Empty the data container of the commands.
   */
  public emptyCommandQueque() {
    this.commandsQueque.empty();
  }

  /**
   * Parse a received message from a peer
   * @param buff The buffer to parse
   * @param peer The peer that has sent this data
   */
  public async parse(buff: Buffer, peer: Peer) {
    let cmdArray: Array<string> = buff.toString('hex').split(SharableObjects.magicMainNetString);
    cmdArray.shift();
    let offset: number = 0, nextOffset: number = 0;

    // Parse the command header
    for (let i = 0; i < cmdArray.length; i++) {
      try {
        offset = nextOffset;
        let cmp = buff.slice(offset, offset + 4).compare(SharableObjects.magicMainNet);
        if (cmp == 0) {
          let payloadlength = buff.readUInt32LE(offset + 16);
          nextOffset += 24 + payloadlength;
          cmp = doubleSHA256(buff.slice(offset + 24, offset + 24 + payloadlength)).slice(0, 4).compare(buff.slice(offset + 20, offset + 24));
          if (cmp == 0) {
            this.commandsQueque.pushCommand(hexToASCII(cmdArray[i].substr(0, 24)), buff.slice(offset + 24, offset + 24 + payloadlength), peer);
          } else Logger.error('This command [' + hexToASCII(cmdArray[i].substr(0, 24)) + '\n' + cmdArray[i] + '] has invalid payload (checksum verification not passed).');
        }//else console.log('This msg['+ cmdArray[i] +'] is not for the main net!');
      } catch (err) { Logger.warning(err); break; }
    }
    // console.log(peer.getIPV4_Port().ipv4 + ' commands length %i ', cmdArray.length);    
    if (!this.parseCycle) {
      this.parseCycle = true;
      this.parsePayload();
    }
  }

  private async parsePayload() {

    do {
      let cmdData: Command = this.commandsQueque.shiftCommand();
      if (cmdData != null) {
         switch (cmdData.cmd) {
          case 'version':
          this.versionRecived(cmdData);
            // I recived a version msg
            if (!cmdData.peer.connectionRequested) {
              PeerManager.pendingPeers.add(cmdData.peer);
              cmdData.peer.write(this.versionSend(cmdData.peer));
              cmdData.peer.write(this.verackSend());
            }
            cmdData.peer.write(this.verackSend());
            break;
          case 'verack':
            if (PeerManager.pendingPeers.contains(cmdData.peer)) {
              PeerManager.pendingPeers.remove(cmdData.peer);
              PeerManager.addToVerified(cmdData.peer);
              cmdData.peer.write(this.verackSend());
              cmdData.peer.write(this.pingSend(cmdData.peer));
              cmdData.peer.write(this.addrSend());
            }
            break;
          case 'addr':
            let tmp = this.addrReceived(cmdData);
            for (let i = 0; i < tmp.length; i++) {
              if (!PeerManager.verifiedPeers.contains(tmp[i]) && !PeerManager.pendingPeers.contains(tmp[i]))
                PeerManager.availablePeers.add(tmp[i]);
            }

            break;
          case 'inv':
            await this.invReceived(cmdData);
            break;
          case 'getdata':
            console.log('getdata');
            break;
          case 'notfound':
            console.log('notfound');
            break;
          case 'getblocks':
            console.log('getblocks');
            break;
          case 'getheaders':
            if(!PeerManager.isDownloading)
              cmdData.peer.write(await this.getheadersReceived(cmdData));
            break;
          case 'tx':
            try {
              // let tx = await txReceived(cmdData, this);
            } catch (err) { Logger.error('txRecived:\N' + err);}
            break;
          case 'block':
            try {
              await this.blockLock.wait();
              this.blockReceived(cmdData, this.blockLock).then( b => {
                if(b) Logger.info('Block verified: ' + b.height + ' ' + b.hash + ' ' + cmdData.peer.getIPV4_Port().ipv4 + ' ' + new Date().toLocaleTimeString());
                this.blockLock.release();
              });
            } catch (err) {
              Logger.error('blockRecived:\n' + err);
            }
            break;
          case 'headers':
            let msg = await this.headersReceived(cmdData);
            if (msg.byteLength > 0)
              cmdData.peer.write(msg);
            break;
          case 'getaddr':
            cmdData.peer.write(this.getaddrReceived(cmdData));
            break;
          case 'mempool':
            break;
          case 'ping':
            cmdData.peer.write(this.pingRecived(cmdData));
            break;
          case 'pong':
          this.pongRecived(cmdData);
            break;
          case 'reject':
            break;
          case 'alert':
            // Deprecated
            break;
          default:
            Logger.info(cmdData.peer.getIPV4_Port().ipv4 + ' - Command not yet implemented: ' + cmdData.cmd);
            break;
        }
      }
      if (this.commandsQueque.getLength() == 0)
        this.parseCycle = false;
    } while (this.parseCycle);
  }

  public createMessage (command: string, payload?: Buffer): Buffer {
    if (command.length > 12) throw 'Command name too long';
    let commandHex = asciiToHex(command);
    if (!payload) payload = SharableObjects.nullBuffer;
  
    let buff = new BufferCoin();
    buff.writeuint32le(Number.parseInt(reverseHex(SharableObjects.magicMainNetString), 16));  //0xd9b4bef9);
    buff.writeString(commandHex + '0'.repeat(2 * (12 - command.length)), 'hex');
    buff.writeuint32le(payload.byteLength);
    buff.writeString(doubleSHA256(payload).slice(0, 4).toString('hex'), 'hex');
    buff.writeString(payload.toString('hex'), 'hex');
    return buff.write();
  }

  public versionRecived (cmdData: Command) {
    cmdData.peer.clientVersion = cmdData.data.readUInt32LE(0);
    cmdData.peer.services = U64.readRaw(cmdData.data, 4);
    cmdData.peer.timeStamp = I64.readRaw(cmdData.data, 12);
    cmdData.peer.nonce = U64.readRaw(cmdData.data, 72);
    let tmp = VarStr.decodeVarStr(cmdData.data, 80, 'utf8');
    cmdData.peer.userAgent = tmp.str;
    cmdData.peer.lastBlock = cmdData.data.readInt32LE(80 + tmp.numValue + tmp.numByteLength);
  }

  public versionSend(peer: Peer) {
    let time = Math.floor(new Date().getTime() / 1000);
    let nonce = nonceGenerator();
    let buff = new BufferCoin();
  
    buff.writeint32le(this.version);
    buff.writeuint64le(1);
    buff.writeint64le(time);
    buff.writeNetAddress(new NetAdress(time, 1, peer.getIPV4_Port().ipv4, 8333, true));
    buff.writeNetAddress(new NetAdress(time, 1, SharableObjects.myIPv4, 8333, true));
    buff.writeuint64le(nonce);
    buff.writeVarStr(new VarStr('/Rany:0.0.1/', 'ascii'));
    buff.writeuint32le(BlockChain.lastHeightInDB); //Last block received
  
    return this.createMessage('version', buff.write());
  }

  public verackSend(): Buffer {
    return this.createMessage('verack');
  }

  public pingSend(peer: Peer): Buffer {
    let payload = Buffer.allocUnsafe(8);
    peer.noncePing = nonceGenerator();
    peer.noncePing.writeLE(payload, 0);
    return this.createMessage('ping', payload);
  }

  public pingRecived(cmdData: Command): Buffer {
    if (PeerManager.verifiedPeers.contains(cmdData.peer)) {
      let tmp = cmdData.data.slice(0, 9);
      if (tmp.length != 0) cmdData.peer.noncePing = U64.readRaw(tmp, 0);
      else cmdData.peer.noncePing = new U64(0);
      return this.createMessage('pong', tmp);
    } else return SharableObjects.nullBuffer;
  }

  public pongRecived(cmdData: Command) {
    // To change the queque in Peending or another one
    if (PeerManager.verifiedPeers.contains(cmdData.peer)) {
      let tmp = cmdData.data.slice(0, 9);
      let tmp2: U64;
      if (tmp.length != 0) tmp2 = U64.readRaw(tmp, 0);
      else {
        // Older protocol version
        tmp2 = new U64(0);
        cmdData.peer.noncePing = tmp2;
      }
      if (cmdData.peer.noncePing.cmp(tmp2)) {
        // Change the peer state to Verified
        cmdData.peer.timeStamp = new U64(Math.floor(new Date().getTime() / 1000));
      }
    }
  }

  public getaddrSend(): Buffer {
    return this.createMessage('getaddr');
  }

  public getaddrReceived(cmdData: Command): Buffer {
    if (PeerManager.verifiedPeers.contains(cmdData.peer)) {
      let buff = new BufferCoin();
      if (PeerManager.verifiedPeers.size() != 0) {
        buff.writeVarInt(new VarInt(PeerManager.verifiedPeers.size()));
        PeerManager.verifiedPeers.forEach(function (el) {
          buff.writeNetAddress(new NetAdress(el.timeStamp.toNumber(), el.services, el.getIPV4_Port().ipv4, el.getIPV4_Port().port));
        });
      }
      return this.createMessage('addr', buff.write());
    } else return SharableObjects.nullBuffer;
  }

  public addrReceived(cmdData: Command): Array<Peer> {
    if (PeerManager.verifiedPeers.contains(cmdData.peer)) {
      let tmp = new Array<Peer>();
      let varint = VarInt.decodeVarInt(cmdData.data);
      let cond: boolean;
  
      if (varint.num instanceof U64) cond = varint.num.lte(1000);
      else cond = varint.num < 1000;
      if (cond) {
        if (varint.num instanceof U64) varint.num = varint.num.toNumber();
        for (let i = 0; i < varint.num; i++) {
          tmp.push(NetAdress.decodeIPV4(cmdData.data, varint.numByteLength + (i * 30)));
        }
        return tmp;
      }
    } else return new Array<Peer>();
  }

  public addrSend(): Buffer {
    if (PeerManager.verifiedPeers.size() > 0) {
      let buff = new BufferCoin();
      buff.writeVarInt(new VarInt(PeerManager.verifiedPeers.size()));
      PeerManager.verifiedPeers.forEach(peer =>
        buff.writeNetAddress(new NetAdress(peer.timeStamp, peer.services, peer.getIPV4_Port().ipv4, peer.getIPV4_Port().port))
      );
      return this.createMessage('addr', buff.write());
    } else return SharableObjects.nullBuffer;
  }

  public getdataSend(items: Array<{ type: number, hash: string }>): Buffer {
    let b = new BufferCoin();
    b.writeVarInt(new VarInt(items.length));
    for (let i = 0; i < items.length; i++)
      b.writeInventoryEntry(new InventoryEntry(items[i].type, items[i].hash));
    return this.createMessage('getdata', b.write());
  }

  public async invReceived(cmdData: Command) {
    let invArray = new Array<{ type: number, hash: string }>();
    let findObjs = new Array<Array<string>>();
    let newItems = new Array<{ type: number, hash: string }>();
    let promises = new Array<Promise<any>>();
    // Add the collections of messages here 
    let tables = ['bitcoinEx', 'bitcoinTx', 'bitcoinBx'];
    let types = new Array<number>();
  
    // Decode the hashes
    let obj = VarInt.decodeVarInt(cmdData.data);
    for (let i = 0; i < obj.num; i++) {
      invArray.push(InventoryEntry.decodeInvEntry(cmdData.data, obj.numByteLength + (i * 36)));
      while (findObjs.length <= invArray[i].type) findObjs.push(new Array<string>());
      // DEVELOP - I take onlu the blocks
      if (invArray[i].type != 0 && invArray[i].type != 1) findObjs[invArray[i].type].push(invArray[i].hash);
    }
  
    let lastBlock: string = findObjs[2] && findObjs[2].length > 0 && obj.num != 1 ? findObjs[2][findObjs[2].length - 1] : null;
  
    try {
      if (MongoStorage.getState() == MongoStorageState.Connected) {
        let storage = await MongoStorage.getInstance();
        // Let's find the new elements
        findObjs.forEach((elements, index) => {
          if (elements.length > 0) {
            promises.push(storage.readFrom(tables[index], { hash: { $in: findObjs[index] } }, { _id: 0, hash: 1 }));
            types.push(index);
          }
        });
        let res = await Promise.all(promises);
        // Compare what I have in DB with the recived hashes 
        res.forEach((resEl, resInd) => {
          findObjs[types[resInd]].forEach(el => {
            if (res[resInd].indexOf(el) == -1) {
              newItems.push({ type: types[resInd], hash: el });
            }
          });
        });

        if (findObjs[2] && findObjs[2].length > 0 && obj.num != 1) {
          PeerManager.isDownloading = true;
          cmdData.peer.lastBlockRequested = findObjs[2][findObjs[2].length - 1];
          let found = newItems.findIndex(value => { return value.type == 2 && value.hash == lastBlock ? true : false; });
          if (found == -1) {
            newItems.push({ type: 2, hash: lastBlock });
          }
          // In case I recive an inv with only one item inside I have to decide if it is for continum block donwload or a simple new block
          // In the latter case, I will append the hash in the blockRecived function
        } else if(findObjs[2] && findObjs[2].length > 0 && obj.num == 1) 
            cmdData.peer.oneItemInvReceived = true;

        newItems.forEach(item => cmdData.peer.write(this.getdataSend([item])));
      }
    } catch (err) { Logger.error('invRecived:\n' + err); }
  }

  public async txReceived(cmdData: Command, emitter: EventEmitter): Promise<Tx> {
    let tx = Tx.parseTx(cmdData.data).tx;
    await tx.writeToStorage();
    // emitter.emit('Tx', tx);
    return tx;
  }

  public async blockReceived(cmdData: Command, controllerSemaphore: prex.Semaphore): Promise<Block> {
    let b = Block.decodeBlock(cmdData.data);
    if(b.version == 1 && BlockChain.numberBlocksVersionTwo >= 950) return null;
    if(cmdData.peer.oneItemInvReceived) {
      BlockChain.lastNetDetectedHeight = Math.max(b.height, BlockChain.lastNetDetectedHeight);
      cmdData.peer.oneItemInvReceived = false;
    }
    if(!(PeerManager.blockDownloaderPeer != null && PeerManager.blockDownloaderPeer.toString() == cmdData.peer.toString())) {
      return null;
    };
    let workerID = -1;
    try {
      let ris = await Block.getBlockAndHisPredecessor(b);
      // Recived special block for continum chain download 
      if (ris.length == 0) {
        // I don't store the orphan blocks. I will ask for them as normal ones.
        if (cmdData.peer.lastBlockRequestedIsRecived) {
            cmdData.peer.lastBlockRecivedTime = new Date().getTime();
            cmdData.peer.write(await this.getblocksSend(b.prev_block));
            cmdData.peer.lastBlockRequestedIsRecived = false;
            if (b.version > 1)
              Logger.info('Actual BlockChain completation: ' + (BlockChain.lastHeightInDB/BlockChain.lastNetDetectedHeight * 100).toFixed(2) + '%');
        }
        // controllerSemaphore.release();
        return null;
      }
      cmdData.peer.lastBlockRecivedTime = new Date().getTime();
      // Block already in DB.
      if (ris.length > 1 && ris[1].hash == b.hash) {
        // controllerSemaphore.release();
        return null;
      }
      if (b.prev_block == ris[0].hash) {
        cmdData.peer.isDownloadingBlocksArray[0] = true;
        (<any>b).verified = false;
        b.height = ris[0].height + 1;
        let consensus = await BlockChain.consensus(b, Block.copy(ris[0]));
        if(consensus.success){
          await BlockChain.writeToChain(b, ris[0]);
        }
        else {
          Logger.warning(consensus.message);
          cmdData.peer.isDownloadingBlocksArray[0] = false;
          return null;
        }
        if (cmdData.peer.lastBlockRequested == b.hash) cmdData.peer.lastBlockRequestedIsRecived = true;
        BlockChain.lastHeightInDB = Math.max(b.height, BlockChain.lastHeightInDB);
        cmdData.peer.isDownloadingBlocksArray[0] = false;
        return b;
      } else {
        cmdData.peer.isDownloadingBlocksArray[0] = false;
        return null;
      }
    } catch (err) {
        Logger.error('blockReceived:\n' + err);
        if(workerID > -1) {
          cmdData.peer.isDownloadingBlocksArray[0] = false;
        }
        return null;
    }
  }
    
    public async getblocksSend(stopHash: string, height?: number): Promise<Buffer> {
      try {
        let ris = await Block.getBlockLocator(height ? height : BlockChain.lastHeightInDB);
        let buff = new BufferCoin();
        buff.writeuint32le(this.version);
        buff.writeVarInt(new VarInt(ris.length));
        for (let i = 0; i < ris.length; i++)
          buff.writeString(reverseHex(ris[i]), 'hex');
        buff.writeString(reverseHex(stopHash), 'hex');
        return this.createMessage('getblocks', buff.write());
      } catch (err) { Logger.error('getblocksSend:\n' + err); return null; }
    }

    public async getheadersSend(stopHash: string): Promise<Buffer> {
      try {
        let ris = await Block.getBlockLocator(BlockChain.lastHeightInDB);
        let buff = new BufferCoin();
        buff.writeuint32le(this.version);
        buff.writeVarInt(new VarInt(ris.length));
        for (let i = 0; i < ris.length; i++)
          buff.writeString(reverseHex(ris[i]), 'hex');
        buff.writeString(reverseHex(stopHash), 'hex');
        return this.createMessage('getheaders', buff.write());
      } catch (err) { Logger.error('getheadersSend:\n' + err); return null; }
    }

    public async getheadersReceived(cmdData: Command): Promise<Buffer> {
      try{
        let hashNumber = VarInt.decodeVarInt(cmdData.data, 4);
        let offset = 4 + hashNumber.numByteLength;
        let hashes = new Array<string>();
    
        for (let i = 0; i < hashNumber.num; i++) {
          hashes.push(reverseHex(cmdData.data.toString('hex', offset, offset + 32)));
          offset += 32;
        }
        let hash_stop = reverseHex(cmdData.data.toString('hex', offset, offset + 32));
        if(hash_stop != '0'.repeat(32))
          hashes.push(hash_stop);
        let res = await Block.read({ hash: hashes[hashes.length - 1] , mainChain: true }, { _id: 0, height: 1 }, { height: -1 }, 1);
        // If the last hash isn't in the main chain or I haven't it, I have to scan the last common block and start from it
        if(res.length < 1)
          res = await Block.read({ hash: { $in: hashes }, mainChain: true }, { _id: 0, height: 1 }, { height: -1 }, 2000);
        if (res.length < 1) res[0] = {height : BlockChain.lastHeightInDB};
        let max = res[0].height + 2000 <= BlockChain.lastHeightInDB ? res[0].height + 2000 : BlockChain.lastHeightInDB;
        let blocks = await Block.read({ height: { $gt:  res[0].height, $lte: max }, mainChain: true }, 
          { _id: 0 , hash:1 , height: 1, version: 1, prev_block: 1, merkle_root: 1, timestamp: 1, bits: 1, nonce:1 }, 
          { height: 1 }, 2000);
        let buffBlcoks = new BufferCoin();
        let num = 0;
        for(let i = 0; i < blocks.length && blocks[i].hash != hash_stop; i++){
            blocks[i].txns = [];
            buffBlcoks.writeString(Block.copy(blocks[i]).serializeBlockHeader().toString('hex'), 'hex');
            buffBlcoks.writeString('00', 'hex');
            num++;
        }
        return this.createMessage('headers', Buffer.concat([new VarInt(num).numVarInt, buffBlcoks.write()], 2));
      } catch(err) { Logger.error('getheadersReceived:\n' + err); return null;}
    }

    public async headersReceived(cmdData: Command): Promise<Buffer> {
      if (MongoStorage.getState() == MongoStorageState.Connected) {
        let storage = await MongoStorage.getInstance();
        let numHeaders = VarInt.decodeVarInt(cmdData.data);
        let offset = numHeaders.numByteLength;
        let newItems = new Array<{ type: number, hash: string }>();
        let findObjs = new Array<string>();
    
        for (let i = 0; i < numHeaders.num; i++) {
          let b = Block.decodeBlockHeader(cmdData.data, offset);
          findObjs.push(b.hash);
          offset += 81;
        }
    
        let res = await storage.readFrom('bitcoinBx', { hash: { $in: findObjs }, mainChain: true }, { _id: 0, hash: 1 });
        findObjs.forEach(hash => {
          if (res.indexOf(hash) == -1)
            newItems.push({ type: 2, hash: hash });
        });
        if (newItems.length > 0) {
          // return getdataSend(newItems);
          // I can try to start a process getheaaders/header/getdata/block but i like to do it with the IBD basic process
          // Here is possible to specify what you want to do with the headers.
          return SharableObjects.nullBuffer;
        } else return SharableObjects.nullBuffer;
      } else return SharableObjects.nullBuffer;
    }
}