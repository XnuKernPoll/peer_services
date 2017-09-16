package peer_services

import topologies._ 
import com.google.common.cache._
import com.twitter.finagle, com.twitter.util 
import util.Var, finagle.{Service, SimpleFilter}
import finagle.Http
import finagle.http.{Request, Response, Cookie}


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

object RoutingFilters {
  import com.google.common.hash._ 

  type HttpService = Service[Request, Response]
  type HttpFilter = SimpleFilter[Request, Response]

  def addKey(getKey: Request => Array[Byte]) = new SimpleFilter[Request, Response] {
    def apply(req: Request, next: HttpService) = {
      val k = ( HashCode.fromBytes(getKey(req) )  ).toString()
      next(req)
    }
  }


  class Dynamo(state: RouterState, cli: Http.Client) extends HttpFilter {
    val conn_cache = ServiceCache.build(cli, 50)
    val local = state.neighborhood.my_node

    def apply(req: Request, next: HttpService) = {
      val k = req.headerMap.get("Routing-Key").map (keyString => HashCode.fromString(keyString).asBytes() )

      k match  {
        case Some(kh) =>

          val shard = PreferenceList.route(state.table, kh)
          if ( state.table.contains(shard) ) {
            val conns = shard.filterNot(x => x == local) map { k => conn_cache.get(k.key) }
            conns.foreach {host => host(req)}
            next(req)
          } else {
            val hosts = shard.map(x => x.key).mkString(",")
            ( conn_cache.get(hosts) )(req)

          }

        case None => next(req)
      }

    }

  }



}





class RouterState(state: PeerState, f: Int = 3) {

  var table = PreferenceList.partition(neighborhood.neighbors, Node.rcompare, f)
  var localShards = claim


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
