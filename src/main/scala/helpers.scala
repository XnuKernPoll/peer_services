package peer_services
import topologies._ 
import com.google.common.cache._
import com.twitter.finagle, com.twitter.util 
import util.Var, finagle.{Service, SimpleFilter}
import finagle.Http
import finagle.http.{Request, Response, Cookie}

/** This is a pretty useful, albeit simple module, due to the fact that clustering in finagle is made in a way that, 
  you connect to an external service discovery source, via resolvers and then there is a load balancer in front of the cluster, without much thought of the case that you are implementing your own member management system, with finagle, or are implementing replication directly with finagle. 
 
*/
object ServiceCache {
  type Cache[Req, Rep] = LoadingCache[String, Service[Req, Rep] ]
  
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



/** An object for thread safe, shared mutable state, for the Neighborhood type in topologies   */
class RouterState(state: PeerState, f: Int = 3) {

  var table = PreferenceList.partition(neighborhood.neighbors, Node.rcompare, f)
  var localShards = claim

  /** It **/
  def rebalance: Unit = {
    val p = PreferenceList.partition(neighborhood.neighbors, Node.rcompare, f)
    synchronized { table = p }
    val c =  claim 
    if ( localShards != c ) synchronized { localShards = c}
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
