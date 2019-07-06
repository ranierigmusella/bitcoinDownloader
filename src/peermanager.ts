import * as DNS from 'dns';
import { Peer, BlockChain } from './bitcointypes';
import { Logger } from './logger';
import { Set } from 'typescript-collections';
import { SharableObjects } from './utils';
import { setInterval } from 'timers';
import { Semaphore } from 'prex';

let resolve4Async = function(ipV4: string): Promise<Array<string>> {
  return new Promise((resolve) => {
    DNS.resolve4(ipV4, function(err, add){
      if(err) resolve(new Array<string>());
        else resolve(add);
    });
  });
}

export class PeerManager {
  public static readonly availablePeers = new Set<Peer>();
  public static readonly connectingPeers = new Set<Peer>();  
  public static readonly pendingPeers = new Set<Peer>();  
  public static readonly verifiedPeers = new Set<Peer>();

  public static blockDownloaderPeer: Peer = null;
  private static disconnectedSem = new Semaphore(1);
  public static isDownloading: boolean = true;
  
  public static async discover(){
    let dnsServers:string[] = [  'bitseed.xf2.org'
                                ,'dnsseed.bluematt.me'
                                ,'seed.bitcoin.sipa.be'
                                ,'dnsseed.bitcoin.dashjr.org'
                                ,'seed.bitcoinstats.com'
                              ];

    let addresses: Set<string> = new Set<string>();

    for(let i = 0; i < dnsServers.length; i++){
      try{
        let ad = await resolve4Async(dnsServers[i]);
      if(ad.length > 0)
        ad.forEach(item => addresses.add(item));
      } catch(err) { Logger.error('bootstrap: \n' + err); }
    }

    addresses.forEach( ad => {
      let p = new Peer(ad, 8333);
      if(!PeerManager.connectingPeers.contains(p) &&
         !PeerManager.pendingPeers.contains(p) &&
         !PeerManager.verifiedPeers.contains(p)
      ){
        p.addListener('closed', PeerManager.listnerClosed);
        p.addListener('error', PeerManager.listenerError);
        p.addListener('connected', PeerManager.listenerConnected);   
        p.addListener('connecting', PeerManager.listenerConnecting);         
        PeerManager.availablePeers.add(p);
      }
    });

    /**
     * Function that controls if the peer who is asked for the blocks is downloading or not the chain. 
     */
    // await this.controlPeers();
    setInterval(this.controlDownloadingPeer, 30000);
  }

  private static async controlDownloadingPeer() {
    try{
      await PeerManager.disconnectedSem.wait();
      if(BlockChain.lastHeightInDB == BlockChain.lastNetDetectedHeight){
        PeerManager.isDownloading = false;
      } else {
        PeerManager.isDownloading = true;
      }
      if(!PeerManager.blockDownloaderPeer && PeerManager.verifiedPeers.size() == 0){
        await PeerManager.bootstrap();
      }
      else if(!PeerManager.blockDownloaderPeer && PeerManager.verifiedPeers.size() > 0){
        PeerManager.blockDownloaderPeer = PeerManager.verifiedPeers.toArray()[0];
        PeerManager.verifiedPeers.remove(PeerManager.blockDownloaderPeer);
        PeerManager.blockDownloaderPeer.write(await PeerManager.blockDownloaderPeer.cmdParser.getblocksSend('0'.repeat(64)));
        Logger.info('New downloading Peer ' + PeerManager.blockDownloaderPeer.getIPV4_Port().ipv4 + ' ' + new Date().toLocaleTimeString());
        PeerManager.blockDownloaderPeer.lastBlockRecivedTime = new Date().getTime();
      }
      else if(
        PeerManager.isDownloading
        && PeerManager.blockDownloaderPeer 
        && !PeerManager.blockDownloaderPeer.isDownloadingBlocks()
        && !PeerManager.blockDownloaderPeer.cmdParser.commandsQueque.hasBlock()
        && PeerManager.blockDownloaderPeer.lastBlockRecivedTime 
        && (new Date().getTime() - PeerManager.blockDownloaderPeer.lastBlockRecivedTime) > 60000
        && BlockChain.lastHeightInDB !== BlockChain.lastNetDetectedHeight
      ){
        PeerManager.blockDownloaderPeer.emptyCommandQueque();
        PeerManager.blockDownloaderPeer.disconnect();
        PeerManager.blockDownloaderPeer = null;
        if(PeerManager.verifiedPeers.size() > 0){
          PeerManager.blockDownloaderPeer = PeerManager.verifiedPeers.toArray()[0];
          PeerManager.verifiedPeers.remove(PeerManager.blockDownloaderPeer);
          Logger.info('New downloading Peer ' + PeerManager.blockDownloaderPeer.getIPV4_Port().ipv4 + ' ' + new Date().toLocaleTimeString());
          PeerManager.blockDownloaderPeer.write(await  PeerManager.blockDownloaderPeer.cmdParser.getblocksSend('0'.repeat(64)));
          PeerManager.blockDownloaderPeer.lastBlockRecivedTime = new Date().getTime();  
        }
      }

      PeerManager.disconnectedSem.release();
    } catch(e){
      Logger.error('Peermanager periodic check function:\n' + e);
      PeerManager.disconnectedSem.release();
    }
  }

  public static async bootstrap(){
   
    await PeerManager.discover();
    let peers = PeerManager.availablePeers.toArray();
    for(let i = 0; i < SharableObjects.MAX_IN_PEERS && i < peers.length ; i++){
      PeerManager.availablePeers.remove(peers[i]);
      PeerManager.connectingPeers.add(peers[i]);
      peers[i].connect();
    }
  }

  private static async listnerClosed(peer: Peer){
    await PeerManager.disconnectedSem.wait();
    
    PeerManager.verifiedPeers.remove(peer);
    PeerManager.pendingPeers.remove(peer);
    PeerManager.connectingPeers.remove(peer);

    if(PeerManager.blockDownloaderPeer && PeerManager.blockDownloaderPeer.toString() == peer.toString())
      PeerManager.blockDownloaderPeer = null;
    if(PeerManager.availablePeers.isEmpty())
      await PeerManager.discover();
    let peers = PeerManager.availablePeers.toArray();
    if((PeerManager.connectingPeers.size() + PeerManager.pendingPeers.size() + PeerManager.verifiedPeers.size()) < SharableObjects.MAX_IN_PEERS 
        && !PeerManager.availablePeers.isEmpty()){
        PeerManager.availablePeers.remove(peers[0]);
        PeerManager.connectingPeers.add(peers[0]);
        peers[0].connect();
    }
    PeerManager.disconnectedSem.release();
  }

  private static listenerError(args: Array<any>){
  }

  private static listenerConnected(peer: Peer){
    PeerManager.pendingPeers.add(peer);
    PeerManager.connectingPeers.remove(peer);
  }

  private static listenerConnecting(peer: Peer){
  }

  public static async addToVerified(peer: Peer){
    if(PeerManager.blockDownloaderPeer == null && peer != null){
      PeerManager.blockDownloaderPeer = peer;
      PeerManager.blockDownloaderPeer.write(await  PeerManager.blockDownloaderPeer.cmdParser.getblocksSend('0'.repeat(64)));
      Logger.info('New downloading Peer ' + PeerManager.blockDownloaderPeer.getIPV4_Port().ipv4 + ' ' + new Date().toLocaleTimeString());
      PeerManager.blockDownloaderPeer.lastBlockRecivedTime = new Date().getTime();
    } else PeerManager.verifiedPeers.add(peer);
  }

}