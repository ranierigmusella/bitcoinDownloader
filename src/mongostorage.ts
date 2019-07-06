import { Logger } from './logger';
import * as mongoDB from 'mongodb';
import { Semaphore } from 'prex';
import { Block } from './bitcointypes';

export enum MongoStorageState {
      Unconnected = 1
    , Connected
}

export class MongoStorage {
    private db: mongoDB.Db;
    private client: mongoDB.MongoClient; 
    private static _state = MongoStorageState.Unconnected;
    private static storage: MongoStorage = null;
    private static semaphoreCreation = new Semaphore(1);
    
    private constructor(client: mongoDB.MongoClient){
        this.client = client;
        this.db = client ? client.db('bitcoinDB') : null;
        MongoStorage._state = this.db ? MongoStorageState.Connected : MongoStorageState.Unconnected;
    }

    public static async getInstance(): Promise<MongoStorage>{
        try{
            await MongoStorage.semaphoreCreation.wait();            
            if(MongoStorage._state == MongoStorageState.Connected)
                return MongoStorage.storage;
            else {
                let client = await mongoDB.MongoClient.connect('mongodb://localhost:27017', {connectTimeoutMS: Number.MAX_VALUE, socketTimeoutMS: Number.MAX_VALUE, reconnectTries: Number.MAX_VALUE, autoReconnect: true, keepAlive: Number.MAX_VALUE, poolSize: 30, useNewUrlParser: true});
                MongoStorage.storage = new MongoStorage(client);
            }
        } catch(err){
            Logger.error('DB not connected. All the objects won\'t save');
            MongoStorage.storage = new MongoStorage(null);
        } finally {
            MongoStorage.semaphoreCreation.release();
            return MongoStorage.storage;
        } 
    }

    public async writeTo(table: string, obj: Array<any>, key?:  Array<any>): Promise<any> {
        if(key){
            for(let i = 0; i < obj.length; i++)
                obj[i]._id = key[i];
        }

        if(MongoStorage.getState() == MongoStorageState.Connected){
            try{
                return await this.db.collection(table).insertMany(obj, {ordered: false});
            } catch(mongError) { return mongError; }
        }
    }

    public async update(table: string, query: any, expression: any): Promise<any> {
        if(MongoStorage.getState() == MongoStorageState.Connected){
            try{
                return await this.db.collection(table).updateMany(query, expression);
            } catch(err){ Logger.error('Storage update:\n' + err); }
        }
    }

    public async readFrom(table: string, query?: any, projection?: any, sort?: any, limit?: number): Promise<Array<any>>{
        let q: any = {};
        let p: any = {_id: 0};

        if(query) q = query;
        if(projection) p = projection;
        if(MongoStorage.getState() == MongoStorageState.Connected){
            try{
                let r = await this.db.collection(table).find(q, {projection: p, limit: limit, timeout: false}).addCursorFlag('noCursorTimeout', true);
                if(sort) r = r.sort(sort); 
                return await r.toArray();
            } catch(err) { Logger.error('Storage readFrom:\n' + err); return new Array<any>(); }
        } else return new Array<any>();
    }

    public async createIndexes(table: string, indexes: Array<any>){
        if(MongoStorage.getState() == MongoStorageState.Connected){
            try{
                await this.db.collection(table).createIndexes(indexes);
            } catch(err) { Logger.error('Storage createIndexes:\n' + err); }
        }
    }

    public async init(){
        if(MongoStorage.getState() == MongoStorageState.Connected){
            let genesis_block = new Block();
            genesis_block = Block.decodeBlock(
                new Buffer('0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000',
                'hex'));
            genesis_block.height = 0;
            genesis_block.mainChain = true;
            return await Promise.all([ 
            this.writeTo('bitcoinBx', [genesis_block], [genesis_block.hash])
            ,
            this.createIndexes('bitcoinBx', [   {key: {hash: 1}}, 
                                                {key: {height: -1}}, 
                                                // {key: {hash: 1, height: -1}, unique: true},
                                                // {key: {next_block: 1}},
                                                {key: {version: -1}},
                                                {key: {"txns.hash": 1}},
                                              ])
            ]);
        }
    }

    public static getState(): MongoStorageState { return MongoStorage._state; }

    public getState(): MongoStorageState { return MongoStorage._state; }
}