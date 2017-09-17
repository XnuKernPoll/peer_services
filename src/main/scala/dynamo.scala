package peer_services.dynamo 
import com.twitter.util, com.twitter.finagle

import topologies._
import peer_services._
import finagle.http.{Request, Response}, finagle.{Http,SimpleFilter, Service, Failure}
import util.{Future}
import scodec.bits.BitVector
import util.Base64UrlSafeStringEncoder

/** A shared, thread safe mutable PNCounter, will probably rename this and implement version vectors as well */
class ReplicaState(pnc: PNCounter) {
  var clock = pnc
  def incr = {
    val nc = clock.incr
    synchronized {clock = nc}; nc
  }

  def decr = {
    val nc = clock.decr
    synchronized {clock = nc}; nc
  }

  def become(c: PNCounter) = { synchronized {clock = c}; c }  

}


trait OP
case object READ extends OP
case object WRITE extends OP
case object DELETE extends OP


/** A module for replication */
object TXFilters {
  
  import RoutingFilters.{HttpService, HttpFilter}
  type extractOP = Request => OP

  def embedClock(req: Request, pnc: PNCounter): Request = {
    val clockPL = PNCounter.codec.encode(pnc).toOption.map(x => x.toByteArray) 
    val plString = Base64UrlSafeStringEncoder.encode(clockPL.get)
    req.headerMap += ("replica-version" -> plString)
    req 
  }


  def extractClock(req: Request): Option[PNCounter] = req.headerMap.get("replica-version").flatMap { x =>
    val s = Base64UrlSafeStringEncoder.decode(x)
    val buf = BitVector(s)
    PNCounter.codec.decode(buf).toOption.map(y => y.value)
  }
  


  def hasClock(req: Request): Boolean = req.headerMap.contains("replica-version")
  

  def replica(lstate: ReplicaState, opType: extractOP) = new HttpFilter {
    def apply(req: Request, next: HttpService) = {
      val c = extractClock(req)
      (opType(req), c) match {

        case (READ, _) => next(req)

        case (DELETE, Some(cv) ) if (cv.delete > lstate.clock.delete) =>
          lstate.become( PNCounter.merge(cv, lstate.clock) ) 
          next(req)
        case (WRITE, Some(cv) ) if (cv.add > lstate.clock.add ) =>
          lstate.become( PNCounter.merge(cv, lstate.clock) )
          next(req)

        case _ => Future.exception{ Failure.rejected("remote peer's clock is not synchronized") } 
      }
    }
  }



  def replicationRequest(req: Request, lstate: ReplicaState, opType: extractOP) = opType(req) match {
      case READ => embedClock(req, lstate.clock)
      case DELETE => embedClock( req, lstate.decr) 
      case WRITE => embedClock(req, lstate.incr)  
  }


}




object RoutingFilters {
  import com.google.common.hash._ 
  import TXFilters._
  type HttpService = Service[Request, Response]
  type HttpFilter = SimpleFilter[Request, Response]

  def addKey(getKey: Request => Array[Byte]) = new SimpleFilter[Request, Response] {
    def apply(req: Request, next: HttpService) = {
      val k = Base64UrlSafeStringEncoder.encode( getKey(req) )
      req.headerMap += ("Routing-Key" -> k)
      next(req)
    }
  }

  def routingKey(req: Request): Option[Array[Byte] ] = req.headerMap.get("Routing-Key").map(ks => Base64UrlSafeStringEncoder.decode(ks) ) 
  
  class Writes(routes: RouterState, counter: ReplicaState, getOP: extractOP, conn_cache: ServiceCache.Cache[Request, Response]) extends HttpFilter {
    val replicaFilter = TXFilters.replica(counter, getOP)
    val local = routes.neighborhood.my_node

    def apply(req: Request, next: HttpService) = {
      val txINIT = hasClock(req)
      val k = routingKey(req)

      k match {
        case Some(hash) if (txINIT != true)  =>
          val shard = PreferenceList.route(routes.table, hash)
          if ( routes.localShards.contains(shard) ) {
            val rmsg = replicationRequest(req, counter, getOP)
            val hosts = shard.filterNot(x => x == local).map(x => x.key)
            hosts.map(h => conn_cache.get(h) ).foreach {node => node(rmsg) }
            next(req)
          } else { conn_cache.get( shard.map(x => x.key).mkString(",")  )(req)  }

        case Some(hash) if (txINIT == true) => replicaFilter(req, next)

        case None => Future.exception{ Failure.rejected("Lacked Necessary headers") } 
      }
    }

  }



  class Reads(routes: RouterState, conn_cache: ServiceCache.Cache[Request, Response]) extends HttpFilter {
    def apply(req: Request, next: HttpService) = {
      val key = routingKey(req)
      key match {
        case Some(hash) =>
          val shard = PreferenceList.route(routes.table, hash)
          if ( routes.localShards.contains(shard) ) { next(req) } else { conn_cache.get( shard.map(x => x.key).mkString(",") )(req)  }

        case None => Future.exception{ Failure.rejected("Lacked Necessary headers") } 
      }
    }
  }

  class DynamoBehavior(routes: RouterState, cli: Http.Client, getOP: extractOP) extends HttpFilter {
    val conns = ServiceCache.build(cli, 60)
    val ctr = new ReplicaState( PNCounter(routes.neighborhood.my_node.key, 0, 0) )
    val writes = new Writes(routes, ctr, getOP, conns)
    val reads = new Reads(routes, conns)


    def apply(req: Request, next: HttpService) = {
      getOP(req) match {
        case READ => reads(req, next)
        case WRITE => writes(req, next)
        case DELETE => writes(req, next)
      }
    }

  }



}

