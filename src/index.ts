import { MongoStorage, MongoStorageState } from './mongostorage';
import * as httpClient from 'request-promise';
import { Block, BlockChain } from './bitcointypes';
import { SharableObjects } from './utils';
import { PeerManager } from './peermanager';
import { Logger } from './logger';

process.on('uncaughtException', err => {
  Logger.error('Fatal exception: \n' + err);
  process.exit(-1);
});

let main = async () => {

  // Insert the genesis block in the DB and the indexes to improve the performance.
  let storage = await MongoStorage.getInstance();
  if(storage.getState() == MongoStorageState.Unconnected) process.exit(-1);
  Logger.info('DB connected.');
  await storage.init();
  // This allow to ask the DB only once about the max current block height 
  Logger.info('Getting the last height in DB.');
  let heights = await Promise.all([
    Block.getMaxHeight()
  ]);

  BlockChain.lastHeightInDB = heights[0];
  Logger.info('Last height in DB: ' + BlockChain.lastHeightInDB);
  let timestamps: Array<any> = null;
  [BlockChain.numberBlocksVersionTwo, timestamps] = await Promise.all([
    BlockChain.numberOfBlocskWithVersion(2)
    , BlockChain.getLastElevenTimestamps()]);
  for(let i = 1; i <= timestamps.length; i++) BlockChain.lastElevenTimestamps.push(timestamps[timestamps.length - i].timestamp);
  
  // Getting my external IP
  try{
    httpClient.get('http://checkip.dyndns.org/')
    .then(body => {
      let r = new RegExp(/\d{1,3}[.]\d{1,3}[.]\d{1,3}[.]\d{1,3}/);
      SharableObjects.myIPv4 = r.exec(body) != null ? r.exec(body)[0] : '0.0.0.0';
    })
    .catch(e => {
      SharableObjects.myIPv4 = '0.0.0.0';
    });
  }catch(e){
  }
  PeerManager.bootstrap();
}
main();
