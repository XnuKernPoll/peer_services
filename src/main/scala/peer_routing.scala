package peer_services

import topologies._ 
import com.google.common.cache._
import com.twitter.finagle, com.twitter.util 
import util.Var, finagle.Service

object ServiceCache {
  
  def loader[Req, Rep](cli: finagle.Client[Req, Rep])  = new CacheLoader[String, Service[Req, Rep] ] {
    def load(key: String) = cli.newService(key) 
  }

  def evictionListener[Req, Rep]= new RemovalListener[String, Service[Req, Rep] ] {
    def onRemoval(e: RemovalNotification[String, Service[Req, Rep]] ) = e.getValue().close()  
  }

  def build[Req, Rep](cli: finagle.Client[Req, Rep], max_size: Int) = {
    CacheBuilder.newBuilder().maximumSize(max_size).removalListener(evictionListener[Req, Rep]).build( loader(cli) )
  }


}


class RouterState(state: PeerState, f: Int = 3) {

  var table = PreferenceList.partition(neighborhood.neighbors, Node.rcompare, f)
  val localShards = Var( claim ) 

  private def sampleShards = Var.sample(localShards)

  def rebalance: Unit = {
    val p = PreferenceList.partition(neighborhood.neighbors, Node.rcompare, f)
    synchronized { table = p }
    val c =  claim 
    if ( sampleShards != c ) { localShards() = c}
  }


  def claim = table.filter(x => x.contains(neighborhood.my_node) )

  def join(n: Node) = {
    state.join(n)
    rebalance
  }

  def leave(n: Node) = { state.leave(n); rebalance }

  def become(neighb: Neighborhood) = {
    state.become(neighb)
    rebalance
  }

  def neighborhood = state.peers
}
