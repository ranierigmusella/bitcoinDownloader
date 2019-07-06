import { MongoStorage, MongoStorageState } from './mongostorage';
import { EventEmitter } from 'events';
import { Logger } from './logger';
import { doubleSHA256, reverseHex, SharableObjects } from './utils';
import { U64, I64 } from 'n64';
import { Socket } from 'net';
import { CmdParser } from './cmdparser';
import * as Collections from 'typescript-collections';
import * as math from 'mathjs';
import * as Cluster from 'cluster';

class Istr { public constructor(public istr: string, public value: any) { } }

export class BufferCoin {
	private dataContainer: Array<Istr> = new Array<Istr>();
	public bufferSize: number = 0;

	public writeuint8(num: number) {
		this.dataContainer.push(new Istr('uint8le', num));
		this.bufferSize += 1;
	}

	public writeuint16le(num: number) {
		this.dataContainer.push(new Istr('uint16le', num));
		this.bufferSize += 2;
	}

	public writeuint32le(num: number) {
		this.dataContainer.push(new Istr('uint32le', num));
		this.bufferSize += 4;
	}

	public writeint32le(num: number) {
		this.dataContainer.push(new Istr('int32le', num));
		this.bufferSize += 4;
	}

	public writeuint64le(num: number | U64) {
		this.dataContainer.push(new Istr('uint64le', num));
		this.bufferSize += 8;
	}

	public writeint64le(num: number | I64) {
		this.dataContainer.push(new Istr('int64le', num));
		this.bufferSize += 8;
	}

	public writeNetAddress(address: NetAdress) {
		this.dataContainer.push(new Istr('netaddress', address));
		this.bufferSize += address.addr.length;
	}

	public writeVarInt(varint: VarInt) {
		this.dataContainer.push(new Istr('varint', varint));
		this.bufferSize += varint.numVarInt.length;
	}

	public writeVarStr(varstr: VarStr) {
		this.dataContainer.push(new Istr('varstr', varstr));
		this.bufferSize += varstr.length.numVarInt.length + (varstr.str.length / (varstr.enc == 'hex' ? 2 : 1));
	}

	public writeString(str: string, encoding: string) {
		this.dataContainer.push(new Istr('string', { str: str, enc: encoding }));
		if (encoding == 'hex') this.bufferSize += str.length / 2;
		else this.bufferSize += str.length;
	}

	public writeInventoryEntry(inv: InventoryEntry) {
		this.dataContainer.push(new Istr('inv', inv));
		this.bufferSize += inv.buff.byteLength;
	}

	/**
	 *	Writes the entire queque to a buffer.
	 *
	 */
	public write(): Buffer {
		let offset: number = 0;
		let buff = Buffer.allocUnsafe(this.bufferSize);

		this.dataContainer.forEach(istr => {
			switch (istr.istr) {
				case 'uint8le':
					buff.writeUInt8(istr.value, offset);
					offset += 1;
					break;
				case 'uint16le':
					buff.writeUInt16LE(istr.value, offset);
					offset += 2;
					break;
				case 'uint32le':
					buff.writeUInt32LE(istr.value, offset);
					offset += 4;
					break;
				case 'int32le':
					buff.writeInt32LE(istr.value, offset);
					offset += 4;
					break;
				case 'uint64le':
					if (istr.value instanceof U64) istr.value.writeLE(buff, offset);
					else new U64(istr.value).writeLE(buff, offset);
					offset += 8;
					break;
				case 'int64le':
					if (istr.value instanceof I64) istr.value.writeLE(buff, offset);
					else new I64(istr.value).writeLE(buff, offset);
					offset += 8;
					break;
				case 'netaddress':
					buff.write((<NetAdress>istr.value).addr.toString('hex'), offset, (<NetAdress>istr.value).addr.length, 'hex');
					offset += (<NetAdress>istr.value).addr.length;
					break;
				case 'varint':
					buff.write((<VarInt>istr.value).numVarInt.toString('hex'), offset, (<VarInt>istr.value).numVarInt.length, 'hex');
					offset += (<VarInt>istr.value).numVarInt.length;
					break;
				case 'varstr':
					buff.write((<VarStr>istr.value).length.numVarInt.toString('hex'), offset, (<VarStr>istr.value).length.numVarInt.length, 'hex');
					offset += (<VarStr>istr.value).length.numVarInt.length;
					buff.write((<VarStr>istr.value).str, offset, (<VarStr>istr.value).str.length, (<VarStr>istr.value).enc);
					offset += (<VarStr>istr.value).str.length / ((<VarStr>istr.value).enc == 'hex' ? 2 : 1);
					break;
				case 'string':
					buff.write(istr.value.str, offset, istr.value.str.length, istr.value.enc);
					if (istr.value.enc == 'hex') offset += istr.value.str.length / 2;
					else offset += istr.value.str.length;
					break;
				case 'inv':
					buff.write((<InventoryEntry>istr.value).buff.toString('hex'), offset, (<InventoryEntry>istr.value).buff.length, 'hex');
					offset += (<InventoryEntry>istr.value).buff.length;
					break;
			}
		});
		return buff;
	}
}

export class NetAdress {
	public addr: Buffer = null;
	public constructor(public timeInSeconds: number, public services: number, public ipv4: string, public port: number, isForVersionPacket: boolean = false) {
		let buff = new BufferCoin();

		if (!isForVersionPacket) buff.writeuint32le(timeInSeconds);
		buff.writeuint64le(services);
		buff.writeString('00000000000000000000FFFF' + NetAdress.ipv4ToHex(ipv4), 'hex');
		buff.writeString(port.toString(16), 'hex');
		this.addr = buff.write();
	}

	public static decodeIPV4(buff: Buffer, offset?: number): Peer {
		if (!offset) offset = 0;

		let time: number = buff.readUInt32LE(offset);
		let version: U64 = U64.readRaw(buff, offset + 4);
		let ipv4: string = NetAdress.hextoIPV4(buff.toString('hex', offset + 24, offset + 24 + 4));
		let port = buff.readUInt16BE(offset + 28);
		let p = new Peer(ipv4, port);
		p.clientVersion = version.toNumber();
		p.timeStamp = time;

		return p;
	}

	public static ipv4ToHex(ipv4: string): string {
		let octet: string[] = ipv4.split('.');
		let res: string[] = new Array<string>();

		octet.forEach(function (el) {
			let tmp = Number(el).toString(16);
			if (tmp.length < 2) res.push('0' + tmp);
			else res.push(tmp);
		});
		return res.reduce((pv, cv, ci, a) => {
			return pv + cv;
		});
	}

	public static hextoIPV4(hex: string): string {
		let s: string = '';
		for (let i = 0; i < 8; i += 2) {
			s += Number.parseInt(hex.substr(i, 2), 16);
			s += '.';
		}
		return s.substr(0, s.length - 1);
	}
}

export class VarInt {
	public numVarInt: Buffer = null;
	public constructor(public num: number) {
		if (num < 0xfd) {
			this.numVarInt = new Buffer(1);
			this.numVarInt.writeUInt8(num, 0);
		} else if (num <= 0xffff) {
			this.numVarInt = new Buffer(3);
			this.numVarInt.writeUInt8(0xfd, 0);
			this.numVarInt.writeUInt16LE(num, 1);
		} else if (num <= 0xffffffff) {
			this.numVarInt = new Buffer(5);
			this.numVarInt.writeUInt8(0xfe, 0);
			this.numVarInt.writeUInt32LE(num, 1);
		} else {
			this.numVarInt = new Buffer(9);
			this.numVarInt.writeUInt8(0xff, 0);
			new U64(num).writeLE(this.numVarInt, 1);
		}
	}

	public static decodeVarInt(buff: Buffer, offset?: number): { num: number | U64, numByteLength: number } {
		let numValue: number;
		let numLength: number = 1;

		if (!offset) offset = 0;
		numValue = buff.readUInt8(offset);
		switch (numValue) {
			case 0xfd:
				numValue = buff.readUInt16LE(offset + 1);
				numLength = 3;
				break;
			case 0xfe:
				numValue = buff.readUInt32LE(offset + 1);
				numLength = 5;
				break;
			case 0xff:
				numValue = U64.readRaw(buff, offset + 1);
				numLength = 9;
				break;
			default:
				break;
		}
		return { num: numValue, numByteLength: numLength };
	}
}

export class VarStr {
	public length: VarInt = null;
	public constructor(public str: string, public enc: string) {
		this.length = new VarInt(str.length / (enc == 'hex' ? 2 : 1));
	}

	public static decodeVarStr(buff: Buffer, offset?: number, encoding: string = 'utf8') {
		let numValue: number;
		let numByteLength: number;

		if (!offset) offset = 0;
		numValue = buff.readUInt8(offset);
		switch (numValue) {
			case 0xfd:
				numValue = buff.readUInt16LE(offset + 1);
				numByteLength = 3;
				break;
			case 0xfe:
				numValue = buff.readUInt32LE(offset + 1);
				numByteLength = 5;
				break;
			case 0xff:
				numValue = U64.readRaw(buff, offset + 1);
				numByteLength = 9;
				break;
			default: numByteLength = 1;
				break;
		}
		let str = buff.toString(encoding, offset + numByteLength, offset + numByteLength + numValue);
		return { numValue, numByteLength, str };
	}

}

export class InventoryEntry {
	public buff: Buffer = null;
	constructor(type: number, hash: string | Buffer) {
		let b: BufferCoin = new BufferCoin();
		b.writeuint32le(type);
		if (hash instanceof Buffer) b.writeString(reverseHex(hash.toString('hex')), 'hex');
		else b.writeString(reverseHex(hash), 'hex');
		this.buff = b.write();
	}

	public static decodeInvEntry(buff: Buffer, offset?: number): { type: number, hash: string } {
		let obj: { type: number, hash: string } = { type: 0, hash: '' };
		if (!offset) offset = 0;
		obj.type = buff.readUInt32LE(offset);
		obj.hash = reverseHex(buff.toString('hex', offset + 4, offset + 4 + 32));
		return obj;
	}
}

export enum PeerState {
	Unconnected = 1
	, Connected
}

export class Peer extends EventEmitter {
	private socket: Socket;
	public cmdParser = new CmdParser();
	public state = PeerState.Unconnected;
	public clientVersion: number;
	public timeStamp: I64;
	public nonce: U64;
	public noncePing: U64;
	public userAgent: string;
	public lastBlock: number;
	public services: number;

	public lastBlockRequested = '';
	public lastBlockRequestedIsRecived = false;
	public oneItemInvReceived = false;
	public lastBlockRecivedTime: number = null;
	public connectionRequested = false;
	public isDownloadingBlocksArray = [];

	private ipv4: string;
	private port: number;
	private tmpDataBuffer: Buffer = Buffer.alloc(0);

	public constructor(ipv4: string, port: number, socket?: Socket) {
		super();
		this.connectionRequested = true;
		if(socket) this.socket = socket;
		else this.socket = new Socket();

		this.ipv4 = ipv4;
		this.port = port;

		this.socket.addListener('error', err => {
			this.disconnectionOperations();
		});
		this.socket.addListener('close', () => {
			this.disconnectionOperations();
			this.emit('closed', this);
		});
		this.socket.addListener('end', () => {
			this.disconnectionOperations();
			this.emit('ended', this);
		});
		this.socket.addListener('data', (data: Buffer) => {
			this.tmpDataBuffer = Buffer.concat([this.tmpDataBuffer, data], this.tmpDataBuffer.byteLength + data.byteLength);
			if(this.tmpDataBuffer.byteLength >= Math.pow(2, 21)) this.disconnect();
			let cond = true;
			while (cond) {
				let x = this.tmpDataBuffer.slice(0, 4).compare(SharableObjects.magicMainNet);
				if (x == 0 && this.tmpDataBuffer.byteLength > 20) {
					let payloadlength = this.tmpDataBuffer.readUInt32LE(16);
					if (this.tmpDataBuffer.byteLength >= 24 + payloadlength) {
						this.cmdParser.parse(Buffer.from(this.tmpDataBuffer.slice(0, 24 + payloadlength)), this);
						this.tmpDataBuffer = Buffer.from(this.tmpDataBuffer.slice(24 + payloadlength, this.tmpDataBuffer.byteLength));
					} else cond = false;
				} else cond = false;
			}
		});
	}

	public connect() {
		this.socket.connect(this.port, this.ipv4, () => {
			this.state = PeerState.Connected;
			this.emit('connected', this);
			this.socket.write(this.cmdParser.versionSend(this));
		});
		this.emit('connecting', this);
	}
	
	public disconnect() {
		this.disconnectionOperations();
		this.socket.destroy();
		this.emit('closed', this);
	}

	public emptyCommandQueque(){
		this.cmdParser.emptyCommandQueque();
	}

	public getIPV4_Port(): { ipv4: string, port: number } {
		let obj: { ipv4: string, port: number } = { ipv4: null, port: 0 };
		obj.ipv4 = this.socket && this.socket.remoteAddress != null ? this.socket.remoteAddress : this.ipv4;
		obj.port = this.socket && this.socket.remotePort != null ? this.socket.remotePort : this.port;
		return obj;
	}

	public write(data: Buffer) {
		if (this.state == PeerState.Connected)
			this.socket.write(data);
	}

	private disconnectionOperations() {
		this.state = PeerState.Unconnected;
		this.cmdParser.emptyCommandQueque();
	}

	public isDownloadingBlocks(): boolean{
		let ris = false;
		for(let el of this.isDownloadingBlocksArray)
			ris = ris || (el ? el : false);
		return ris;
	}

	public toString(): string {
		// Needed for the comparison in the Set collection
		return this.getIPV4_Port().ipv4 + ':' + this.getIPV4_Port().port;
	}
}

export class Tx {
	public hash: string = '';
	public version: number;
	public flag: number;
	public tx_in_count: number;
	public tx_in: Array<{ previous_output: { hash: string, index: number }, signature_script: string, sequence: number }>;
	public tx_out_count: number;
	public tx_out: Array<{ value: I64, pk_script: string }>;
	public tx_witnesses: Array<string>;
	public lock_time: number;

	constructor() {
		this.tx_in = new Array<{ previous_output: { hash: string, index: number }, signature_script: string, sequence: number }>();
		this.tx_out = new Array<{ value: I64, pk_script_length: number, pk_script: string }>();
		this.tx_witnesses = new Array<string>();
	}

	static parseTx(buff: Buffer, offset?: number): { tx: Tx, byteLength: number } {
			try{
				if (!offset) offset = 0;
				let startOffset = offset;
				let tx = new Tx();

				tx.version = buff.readInt32LE(offset);
				offset += 4;
				tx.flag = buff.toString('hex', offset, offset + 2) == '0001' ? 1 : 0;
				if (tx.flag == 1) offset += 2; else tx.flag = 0;
				let tmp = VarInt.decodeVarInt(buff, offset);
				let tmp2 = VarStr.decodeVarStr(buff, offset); // just for later
				tx.tx_in_count = tmp.num;
				offset += tmp.numByteLength;
				for (let i = 0; i < tx.tx_in_count; i++) {
					tx.tx_in.push({ previous_output: { hash: '', index: -1 }, signature_script: '', sequence: -1 });
					tx.tx_in[i].previous_output.hash = reverseHex(buff.toString('hex', offset, offset + 32));
					tx.tx_in[i].previous_output.index = buff.readUInt32LE(offset + 32);
					tmp2 = VarStr.decodeVarStr(buff, offset + 36, 'hex');
					tx.tx_in[i].signature_script = tmp2.str;
					tx.tx_in[i].sequence = buff.readUInt32LE(offset + 36 + tmp2.numByteLength + tmp2.numValue);
					offset += 36 + tmp2.numByteLength + tmp2.numValue + 4;
				}

				tmp = VarInt.decodeVarInt(buff, offset);
				tx.tx_out_count = tmp.num;
				offset += tmp.numByteLength;
				for (let i = 0; i < tx.tx_out_count; i++) {
					tx.tx_out.push({ value: new I64(0), pk_script: '' });
					tx.tx_out[i].value = I64.readRaw(buff, offset);
					tmp2 = VarStr.decodeVarStr(buff, offset + 8, 'hex');
					tx.tx_out[i].pk_script = tmp2.str;
					offset += 8 + tmp2.numByteLength + tmp2.numValue;
				}

				if (tx.flag == 1) {
					tmp = VarInt.decodeVarInt(buff, offset);
					offset += tmp.numByteLength;
					for (let i = 0; i < tmp.num; i++) {
						let tmp3 = VarInt.decodeVarInt(buff, offset);
						tx.tx_witnesses.push(buff.toString('hex', offset + tmp3.numByteLength, offset + tmp3.numByteLength + tmp3.num));
						offset += tmp3.numByteLength + tmp3.num;
					}
				}

				tx.lock_time = buff.readUInt32LE(offset);
				offset += 4;
				tx.hash = reverseHex(doubleSHA256(buff.slice(startOffset, offset)).toString('hex'));
				return { tx: tx, byteLength: offset - startOffset };
		} catch(e){ console.log('parseTx' + e); return null; }
	}

	public serializeTx(): Buffer {
		let buff = new BufferCoin();
		buff.writeint32le(this.version);
		if (this.flag == 1) buff.writeString('0001', 'hex');
		buff.writeVarInt(new VarInt(this.tx_in_count));
		for (let i = 0; i < this.tx_in_count; i++) {
			buff.writeString(reverseHex(this.tx_in[i].previous_output.hash), 'hex');
			buff.writeuint32le(this.tx_in[i].previous_output.index);
			buff.writeVarStr(new VarStr(this.tx_in[i].signature_script, 'hex'));
			buff.writeuint32le(this.tx_in[i].sequence);
		}

		buff.writeVarInt(new VarInt(this.tx_out_count));
		for (let i = 0; i < this.tx_out_count; i++) {
			buff.writeint64le(this.tx_out[i].value);
			buff.writeVarStr(new VarStr(this.tx_out[i].pk_script, 'hex'));
		}

		if (this.flag == 1) {
			buff.writeVarInt(new VarInt(this.tx_witnesses.length));
			for (let i = 0; i < this.tx_witnesses.length; i++) {
				buff.writeVarInt(new VarInt(this.tx_witnesses[i].length));
				buff.writeVarStr(new VarStr(this.tx_witnesses[i], 'hex'));
			}
		}

		buff.writeuint32le(this.lock_time);
		return buff.write();
	}

	public async writeToStorage() {
		let storage = await MongoStorage.getInstance();
		return storage.writeTo('bitcoinTx', [this], [this.hash]);
	}

	public static copy(t: any): Tx {
		let tx = new Tx();
		tx.hash = t.hash;
		tx.version = t.version;
		tx.flag = t.flag;
		tx.tx_in_count = t.tx_in_count;
		tx.tx_in = Array.from(t.tx_in);
		tx.tx_out_count = t.tx_out_count;
		t.tx_out.forEach(item => {
			tx.tx_out.push({
				value: typeof item.value == 'object' ? I64.fromObject(item.value) : I64.fromString(item.value, 16),
				pk_script: item.pk_script
			});
		});
		// tx.tx_out.reverse();
		tx.tx_witnesses = t.tx_witnesses;
		tx.lock_time = t.lock_time;

		return tx;
	}

	public toString(): string {
		return this.hash;
	}
}

export class Block {
	public hash: string = '';
	public height: number = -1;
	public version: number = 0;
	public prev_block: string = '';
	public merkle_root: string = '';
	public timestamp: number = 0;
	public bits: number = 0;
	public nonce: number = 0;
	public txns = new Array<Tx>();
	public mainChain: boolean = false;

	static decodeBlock(buff: Buffer, offset?: number): Block {
		if (!offset) offset = 0;
		let block = Block.decodeBlockHeader(buff, offset);
		let txCount = VarInt.decodeVarInt(buff, offset + 80);
		offset += 80 + txCount.numByteLength;

		for (let i = 0; i < txCount.num; i++) {
			let tmp = Tx.parseTx(buff, offset);
			block.txns.push(tmp.tx);
			offset += tmp.byteLength;
		}

		if (block.version >= 2) {
			let length = Number.parseInt(block.txns[0].tx_in[0].signature_script.slice(0, 2), 16);
			block.height = Number.parseInt(reverseHex(block.txns[0].tx_in[0].signature_script.slice(2, 2 + (length * 2))), 16);
		}

		return block;
	}

	static decodeBlockHeader(buff: Buffer, offset?: number): Block {
		if (!offset) offset = 0;
		let block = new Block();

		block.hash = reverseHex(doubleSHA256(buff.slice(offset, offset + 80)).toString('hex'));
		block.version = buff.readUInt32LE(offset);
		block.prev_block = reverseHex(buff.toString('hex', offset + 4, offset + 36));
		block.merkle_root = reverseHex(buff.toString('hex', offset + 36, offset + 68));
		block.timestamp = buff.readUInt32LE(offset + 68);
		block.bits = buff.readUInt32LE(offset + 72);
		block.nonce = buff.readUInt32LE(offset + 76);

		return block;
	}

	public serializeBlockHeader(): Buffer {
		let buff = new BufferCoin();
		buff.writeint32le(this.version);
		buff.writeString(reverseHex(this.prev_block), 'hex');
		buff.writeString(reverseHex(this.merkle_root), 'hex');
		buff.writeuint32le(this.timestamp);
		buff.writeuint32le(this.bits);
		buff.writeuint32le(this.nonce);
		return buff.write();
	}

	public serializeBx(): Buffer {
		let buff = new BufferCoin();
		buff.writeString(this.serializeBlockHeader().toString('hex'), 'hex');
		buff.writeVarInt(new VarInt(this.txns.length));
		for (let i = 0; i < this.txns.length; i++)
			buff.writeString(this.txns[i].serializeTx().toString('hex'), 'hex');
		return buff.write();
	}

	public static async getBlockLocator(height?: number): Promise<Array<any>> {
		let hashes = await BlockDAO.getBlockLocator(height);
		let blockLocator = new Array<string>();
		for (let i = 0; i < hashes.length; i++)
			blockLocator.push(hashes[i].hash);
		return blockLocator;
	}

	public static async getMaxHeight(): Promise<number> {
		return await BlockDAO.getMaxHeight();
	}

	public static async getBlockAndHisPredecessor(block: Block): Promise<Array<any>>{
		return await BlockDAO.getBlockAndHisPredecessor(block);
	}

	public static copy(block: any): Block {
		let b = new Block();
		b.hash = block.hash;
		b.height = block.height;
		b.version = block.version;
		b.prev_block = block.prev_block;
		b.merkle_root = block.merkle_root;
		b.timestamp = block.timestamp;
		b.bits = block.bits;
		b.nonce = block.nonce;
		// Copy the Txs
		b.txns = new Array<Tx>();
		for(let i = 0; i < block.txns.length; i++)
			b.txns.push(Tx.copy(block.txns[i]));

		b.mainChain = block.mainChain;
		return b;
	}
	
	public static async checkBlockTransactions(block: Block): Promise<{success: boolean, tx: Tx }> {
		let b = Block.copy(block);  
		let txns: Collections.Dictionary<string, Tx> = new Collections.Dictionary<string, Tx>();
		let sumsumDiffs : I64 = new I64();
		let zero = new I64(0);
		let base = new I64('5000000000', 10);
		let blockFee = base.shrn(b.height / 210000);
		let coinbase = b.txns[0];
		let txHashes = new Collections.Set<string>();
		b.txns.shift();
		try{
			// Searching optimizzation
			for(let t of b.txns){
				for(let inTx of t.tx_in) txHashes.add(inTx.previous_output.hash);
			}
			txns = await BlockDAO.queryTx(txHashes.toArray());

			for(let i = 0; i < b.txns.length; i++){
				let tx = b.txns[i];
				let sumInputs: I64 = new I64();
				let sumOutputs: I64 = new I64();
				
				if(tx.serializeTx().byteLength > SharableObjects.MAX_BLOCK_SIZE) return {success: false, tx: tx};

				for(let t of tx.tx_in){
					let ris: Tx = undefined;
					if(txns.getValue(t.previous_output.hash) == undefined){
						ris = b.txns.find((value, index) => {return value.hash == t.previous_output.hash && index < i;});
						if(ris == undefined){
							ris = (await BlockDAO.queryTx([t.previous_output.hash])).getValue(t.previous_output.hash);
							if(ris == undefined) {
								return {success: false, tx: tx};
							}
						}
						txns.setValue(t.previous_output.hash, ris);
					}
					sumInputs.iadd(I64.from(txns.getValue(t.previous_output.hash).tx_out[t.previous_output.index].value));
				}
				for(let out of tx.tx_out) sumOutputs.iadd(I64.from(out.value));
				let diff = sumInputs.sub(sumOutputs);
				if(diff.lt(zero)) return {success: false, tx: tx}; 
				sumsumDiffs.iadd(diff);
			}
			blockFee.iadd(sumsumDiffs);
			if(coinbase.tx_out[0].value.gt(blockFee))
				return {success: false, tx: coinbase};
			else
				return {success: true, tx: null};
		}catch(e){
			Logger.error('checkBlockTransactions: ' + e);
			return {success: false, tx:null};
		}
	}

	public static checkMerkelRoot(block: Block): boolean {
		let cond = true;
		let txns = new Array<Buffer>(); 
		for(let t of block.txns)
			txns.push(new Buffer(reverseHex(t.hash), 'hex'));
		
		while(cond){
			if(txns.length > 1){
				let txns2 = new Array<Buffer>();
				if(txns.length % 2 != 0)
					txns.push(txns[txns.length - 1]);
				for(let i = 0; i < txns.length; i += 2)
					txns2.push(doubleSHA256(Buffer.concat([txns[i], txns[i + 1]])));
				txns = Array.from(txns2);
			} else cond = false;
		} 
		if(reverseHex(txns[0].toString('hex')) == block.merkle_root) return true;
		else return false;
	}

	public checkMerkelRoot(): boolean {
		return Block.checkMerkelRoot(this);
	}

	public static difficulty(block: Block): string {
		//   let pdiff = '0x00000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF';
		let bdiff = '0x00000000FFFF0000000000000000000000000000000000000000000000000000';
	
		let a = Number.parseInt(block.bits.toString(16).slice(0, 2), 16);
		let b = '0x' + block.bits.toString(16).slice(2, block.bits.toString(16).length);
		let target = math.multiply(b, math.pow(2, 8 * (a - 3)));
		return math.divide(bdiff, target).toString();
	}

	public difficulty(): string {
		return Block.difficulty(this);
	}

	public static difficultyCheck(block: Block): {succes: boolean, difficulty: string}
	{
      //   let pdiff = '0x00000000FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF';
		let bdiff = '0x00000000FFFF0000000000000000000000000000000000000000000000000000';

		let a = Number.parseInt(block.bits.toString(16).slice(0, 2), 16);
		let b = '0x' + block.bits.toString(16).slice(2, block.bits.toString(16).length);
		let target = math.multiply(b, math.pow(2, 8 * (a - 3)));
  		return {succes: <boolean> math.smallerEq('0x' + block.hash, target), difficulty: block.difficulty()};
	}
	
	public difficultyCheck(): {succes: boolean, difficulty: string}
	{
		return Block.difficultyCheck(this);
	}

	public static async read(query: any, projection?: any, sort?:any, limit?: any): Promise<Array<any>>{
		return await BlockDAO.read(query, projection, sort, limit);
	}
}

export class BlockDAO {
	public static async getMaxHeight(){
		let mongoStorage = await MongoStorage.getInstance();
		if (MongoStorage.getState() == MongoStorageState.Connected) {
			try {
				let result = await mongoStorage.readFrom('bitcoinBx', {mainChain: true}, { _id: 0, height: 1 }, { height: -1 }, 1);
				return result[0].height;
			} catch (err) { Logger.error('getMaxHeight:\n' + err); return 0; }
		} else return 0;
	}

	public static async getBlockLocator(height?: number): Promise<Array<any>>{
		let mongoStorage = await MongoStorage.getInstance();
		if (MongoStorage.getState() == MongoStorageState.Connected) {
			try {
				// let arangoStorage = await ArangoStorage.getInstance();
				let result = new Array<any>({ lastHeight: 0 });
				if (height) result[0].lastHeight = height;
				else result = [{ lastHeight: await Block.getMaxHeight() }];
				let blockLocatorHeight = new Array<number>();
				let step = 1;
				for (let i = result[0].lastHeight; i > 0; i -= step) {
					blockLocatorHeight.push(i);
					if (blockLocatorHeight.length >= 10) step *= 2;
				}
				blockLocatorHeight.push(0);
				let hashesAndHeight = new Collections.Set<{height: number, hash: string}>(item => item.height.toString());
				let res = await Promise.all([
					mongoStorage.readFrom('bitcoinBx', { height: { $in: blockLocatorHeight }, mainChain: true }, { _id: 0, height: 1, hash: 1 }, { height: -1 })
					// , arangoStorage.blockLocator(blockLocatorHeight)
				]);
				for(let h of res[0]) hashesAndHeight.add(h);
				// for(let h of res[1]) hashesAndHeight.add(h);
				let hashes = hashesAndHeight.toArray();
				hashes.sort((a, b) => { if(a.height <= b.height) return 1; else return -1; });
				return hashes;
			} catch (err) { Logger.error('getBlockLocator:\n' + err); return new Array<any>(); }
		} else { return new Array<any>(); }
	}

	public static async writeToChain(block: any){
		let mongoStorage = await MongoStorage.getInstance();
		if(MongoStorage.getState() == MongoStorageState.Connected){
			try{
				return await mongoStorage.writeTo('bitcoinBx', [block], [block.hash]);
			} catch(e){
				Logger.error('writeToStorage: ' + e)
			}
		}
	}

	public static async update(query: any, expression: any){
		let mongoStorage = await MongoStorage.getInstance();
		try{
			if(MongoStorage.getState() == MongoStorageState.Connected){
				return await mongoStorage.update('bitcoinBx', query, expression);
			} else return null
		}catch(e){
			Logger.error('update: ' + e);
			return null;
		}
	}

	public static async read(query: any, projection?: any, sort?:any, limit?: any): Promise<Array<any>>{
		let mongoStorage = await MongoStorage.getInstance();
		try{
			if(MongoStorage.getState() == MongoStorageState.Connected){
				return await mongoStorage.readFrom('bitcoinBx', query, projection, sort, limit);
			}
		}catch(e){
			Logger.error('read: ' + e);
			return null;
		}
	}

	public static async getBlockAndHisPredecessor(b: Block): Promise<Array<any>>{
		let mongoStorage = await MongoStorage.getInstance();
		try{
			if(MongoStorage.getState() == MongoStorageState.Connected){
				// let arangoStorage = await ArangoStorage.getInstance();
				let risTemp = await Promise.all([
					mongoStorage.readFrom('bitcoinBx', { hash: { $in: [b.prev_block, b.hash] } }, { _id: 0 }, { height: 1 }, 2)
				// , arangoStorage.queryBx(b.hash)
				// , arangoStorage.queryBx(b.prev_block)
				]);
			
				let ris = [];
				if(risTemp[0][0] != null) ris[0] = risTemp[0][0];
					// else if(risTemp[2] != null) ris[0] = risTemp[2];
				if(risTemp[0][1] != null) ris[1] = risTemp[0][1];
					// else if(risTemp[1] != null) ris[1] = risTemp[1];
				return ris;
			} else return [];
		}catch(e){
			Logger.error('getBlockPredecessor: ' + e);
			return [];
		}
	}

	public static async queryTx(hashes: Array<string>): Promise<Collections.Dictionary<string, Tx>>{
		try{
			let mongoStorage: MongoStorage; 
			mongoStorage = await MongoStorage.getInstance();
			if(MongoStorage.getState() == MongoStorageState.Connected){
				let txns: Collections.Dictionary<string, Tx> = new Collections.Dictionary<string, Tx>();
				let txInputHashes = new Collections.Set<string>();

				for(let h of hashes) txInputHashes.add(h);
				let offset = 0;
				for(let i = 0; i < Math.ceil(txInputHashes.size()/10) + 1; i++){
					let txTransactionsMongo = await mongoStorage.readFrom('bitcoinBx', {'txns.hash': {$in: txInputHashes.toArray().slice(offset, offset + 10)}}
						, {_id: 0, 'txns' : { $elemMatch: {'hash': {$in: txInputHashes.toArray().slice(offset, offset + 10)}}}}, {height: 1});
					for(let tx of txTransactionsMongo) {
						if(tx.txns[0].hash != null)
							txns.setValue(tx.txns[0].hash, Tx.copy(tx.txns[0]));
					}
					offset += 10;
				}
				
				return txns;
			}
		} catch(e){
			Logger.error('queryTx: ' + e);
			return new Collections.Dictionary<string, Tx>();
		}
	}

	public static async numberOfBlocskWithVersion(version: number){
		try{
			let storage = await MongoStorage.getInstance();
			if(storage.getState() == MongoStorageState.Connected)
				return (await storage.readFrom('bitcoinBx', {version: 2, mainChain: true}, {height: -1}, null, 1000)).length;
			else return 0;
		} catch(e){
			Logger.error('numberOfBlocskWithVersion: ' + e);
			return 0;
		}
	}

	public static async getLastElevenTimestamps(){
		try{
			let mongoStorage = await MongoStorage.getInstance();
			if(mongoStorage.getState() == MongoStorageState.Connected)
				return await mongoStorage.readFrom('bitcoinBx', {mainChain: true}, {timestamp: 1, _id: 0}, {height: -1}, 11);
			else return [];
		}catch(e){
			Logger.error('getLastElevenTimestamps: ' + e);
			return [];
		}
	}
}

export class BlockChain {

	public static lastHeightInDB: number = 0;
	public static lastNetDetectedHeight: number = -1;
	public static numberBlocksVersionTwo = 0;
	public static lastElevenTimestamps: Array<number> = new Array<number>();

	public static async consensus(block: Block, prevBlock: Block): Promise<{success: boolean, message: string}>{
		if(block.serializeBx().byteLength > SharableObjects.MAX_BLOCK_SIZE) return {success: false, message: "The block byte size is greater than maximum allowed."};
		if(block.txns.length == 0) return {success: false, message: 'No transactions in the block.'};
		let obj = block.difficultyCheck();
		if(!obj.succes) return {success: false, message: 'The hash is greater then target difficulty.'};
		let d = new Date();
		d.setHours(d.getHours() + 2);
		if(d < new Date(block.timestamp * 1000)) return {success: false, message: 'The timestamp is greater the 2h starting from now.'};
		if(block.txns[0].tx_in[0].previous_output.hash != '0'.repeat(64)) return {success: false, message: 'The first transaction isn\'t coinbase'};
		if(block.txns[0].tx_in[0].previous_output.index != 4294967295) return {success: false, message: 'The first transaction index isn\'t correct'};
		if(block.txns[0].tx_in[0].signature_script.length < 4 || block.txns[0].tx_in[0].signature_script.length > 200) return {success: false, message: 'scripSign length isn\'t between 2 and 100'};
	  
		for(let tx of block.txns)
		  if(tx.tx_in_count == 0 || tx.tx_out_count == 0) return {success: false, message: 'Number of Tx in or out must be greater then zero'};
		let c = await Block.checkBlockTransactions(block);
		if(!c.success) {  
		  Logger.warning('Tx non trovata: ' + c.tx.hash);
		  return {success: false, message:'Illegal coins value'};
		}
		
		// This is the first change of difficulty
		if(block.height < 32256 && block.bits != Number.parseInt('1d00ffff', 16)) return {success: false, message: 'Difficulty change not respected.'};
		if(block.height >= 32256 && ((block.height - 32256) % 2016 == 0) && block.height == prevBlock.height) return {success: false, message: 'Difficulty change not respected.'};
		let ts : number;
		if(BlockChain.lastElevenTimestamps.length % 2 == 1)
			ts = BlockChain.lastElevenTimestamps[(BlockChain.lastElevenTimestamps.length - 1)/2];
		else 
			ts = (BlockChain.lastElevenTimestamps[BlockChain.lastElevenTimestamps.length/2] + BlockChain.lastElevenTimestamps[(BlockChain.lastElevenTimestamps.length/2) - 1]) / 2;
		
		if(block.timestamp == ts) return {success: false, message:'Block '+ block.height +'. Timestamp is equal to the last eleven block\'s median'};
		if(!block.checkMerkelRoot()) return {success: false, message: 'Merkel root dosen\'t correspond.'};
	  
		
		BlockChain.lastElevenTimestamps.pop();
		BlockChain.lastElevenTimestamps.push(block.timestamp);
		return {success: true, message: ''};
	}
	  
	public static async writeToChain(block: Block, prevBlock: any): Promise<any> {
		// since block 470881
		try{
			if(prevBlock.mainChain && prevBlock.next_block == null){
				// Block to be added to the main chain
				block.mainChain = true;
				if (block.version == 1)
					block.height = prevBlock.height + 1;
				await Promise.all([BlockDAO.writeToChain(block)
						, BlockDAO.update({hash: prevBlock.hash}, {$set: {next_block: block.hash}})]);
				BlockChain.lastHeightInDB = Math.max(block.height, BlockChain.lastHeightInDB);
			} else 
			if(prevBlock.mainChain && prevBlock.next_block != null){
				// First block of the fork
				block.mainChain = false;
				if (block.version == 1)
					block.height = prevBlock.height + 1;
				let b = <any>block;
				b.blocksFork = [b.prev_block];
				await BlockDAO.writeToChain(b);
				BlockChain.lastHeightInDB = Math.max(b.height, BlockChain.lastHeightInDB);
			} else
			if(!prevBlock.mainChain){
				// last block of a fork
				if (block.version == 1)
				block.height = prevBlock.height + 1;
				let b = <any> block;
				b.blocksFork = Array.from(prevBlock.blocksFork).concat([prevBlock.hash]);
				let orphanChain = [b];
				let ris = new Array<any[]>();
				ris.push(await  BlockDAO.read({hash: {$in: b.blocksFork}}, {_id: 0}, {height: -1}, b.blocksFork.length));
				orphanChain = orphanChain.concat(ris[0]);
				ris.push(await BlockDAO.read({height: {$gte: orphanChain[orphanChain.length - 1].height}, mainChain: true}, {_id: 0}, {height: -1}));
				if(ris[0].length > 0 && ris[0].length == b.blocksFork.length){
					let orphanChainLength = math.sum(orphanChain.map<math.BigNumber>(value => math.bignumber(Block.difficulty(value))));
					let mainChainLength = math.sum(ris[1].map<math.BigNumber>(value => math.bignumber(Block.difficulty(value))));
					if(<boolean> math.largerEq(mainChainLength, orphanChainLength)){
					// The main chain isn't changed.
						block.mainChain = false;
						await Promise.all([BlockDAO.writeToChain(block)
								, BlockDAO.update({hash: prevBlock.hash}, {$set: {next_block: block.hash}})]);
						BlockChain.lastHeightInDB = Math.max(b.height, BlockChain.lastHeightInDB);
					} else {
						let blocksMain = ris[1].map<string>(value => value.hash);
						let blocksFork = Array.from(blocksMain);
						blocksFork.shift();
						blocksMain.pop();
						await Promise.all([
						BlockDAO.update({hash: {$in: b.blocksFork}}, {$unset: {blocksFork: ''}, $set: {mainChain: true}})
						, BlockDAO.update({hash: {$in: blocksMain}}, {$set:{blocksFork: blocksFork.reverse(), mainChain: false}})
						, BlockDAO.update({hash: orphanChain[orphanChain.length - 1].hash}, {$set: {next_block: b.blocksFork[1]}})
						]);
						b.mainChain = true;
						delete b.blocksFork;
						await Promise.all([BlockDAO.writeToChain(b)
								, BlockDAO.update({hash: prevBlock.hash}, {$set: {next_block: block.hash}})]);
						BlockChain.lastHeightInDB = Math.max(b.height, BlockChain.lastHeightInDB);
					}
				} else {
					b.blocksFork.push(prevBlock.hash);
					await BlockDAO.writeToChain(b);
					BlockChain.lastHeightInDB = Math.max(b.height, BlockChain.lastHeightInDB);
				}
			}
		} catch(e){
			Logger.error('writeToChain: ' + e);
		}
	}

	/**
	 * It returns the number of blocks with this version in storage. It also sets the internal state of class BlockChain. 
	 * @param version The block's version to count
	 */
	public static async numberOfBlocskWithVersion(version: number){
		return await BlockDAO.numberOfBlocskWithVersion(version);
	}

	/**
	 * This function return the last eleven block's timestamps from the blockchain.
	 */
	public static async getLastElevenTimestamps(){
		return await BlockDAO.getLastElevenTimestamps();
	}
}

export class WorkerQueque extends EventEmitter {
	private a: Array<Cluster.Worker>;
	private flagAlreadyDone = false;

	public constructor (array: Array<Cluster.Worker>) {
		super();
		this.a = Array.from(array);
		this.a.forEach( w => {
			w.on('message', () => this.push(w));
		});
	}

	public length(): Number {
		return this.a.length;
	}

	public shift(): Promise<Cluster.Worker> {
		return new Promise(resolve => {
			if(this.a.length > 0) {
				resolve(this.a.shift());
			}
			else {
				let fun = () => {
					if(this.flagAlreadyDone){
						this.flagAlreadyDone = false;
						this.removeListener('available', fun);
						resolve(this.a.shift());
					}
				};
				this.on('available', fun);
			}
		});
	}

	public push(worker: Cluster.Worker){
		this.a.push(worker);
		this.flagAlreadyDone = true;
		this.emit('available');
	}
}
