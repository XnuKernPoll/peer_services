package peer_services.membership

import com.twitter.concurrent._
import com.twitter.io.Buf , scodec.bits.BitVector

import topologies._
import Buf.ByteArray.Shared
import store_lib.storage.util.BV

import com.twitter.util.Future
import com.twitter.finagle.mux.{Request, Response}
import com.twitter.finagle.{Path, Service}

/** The Server side of a simple member management service. */
object PeerManager {
  trait Verb

  type Neighbor = (PNCounter, Node)
  case object JOIN extends Verb
  case object LEAVE extends Verb
  case object LIST extends Verb

  case class Event(verb: Verb, from: Option[Node])

  def handler(ps: PeerState, e: Future[Event]) = {
    e.map {x => x match {
      case Event(JOIN, Some(n)) =>
        ps.join(n)
        Buf.Utf8( s" ${n.key} joined" ) 

      case Event(LEAVE, Some(n) ) =>
        ps.leave(n)
        Buf.Utf8( s"${n.key} left" )

      case Event(LIST, _) =>
        println(ps.peers.neighbors)
        val e = Node.listCodec.encode(ps.peers.neighbors).toOption.map(x => BV.toBuf(x) )
        e.get

      case _ =>
        println("bitch")
        Buf.Utf8("unable to handle request")
    }
    }
  }

  def extract(req: Request) = {
    val buf = BitVector( Shared.extract( req.body )  )
    Future { Node.codec.decode(buf).toOption.map{x => x.value } }
  }

  def server(peer_state: PeerState) = Service.mk[Request, Response] { req =>
    val event = req.destination match {
      case Path.Utf8("membership", "join") => extract(req).map(node => Event(JOIN, node) )
      case Path.Utf8("membership", "leave") => extract(req).map(node => Event(LEAVE, node) )
      case Path.Utf8("membership", "list") => extract(req).map(n => Event(LIST, n) ) 
    }
    handler(peer_state, event).map( buf => Response(buf) )
  }
}

/** client api of a simple member management service */
object MembershipClient {

  def join(node: Node, conn: Service[Request, Response]): Future[Unit] = {
    val path = Path.Utf8("membership", "join")
    val buf = Node.codec.encode(node).toOption.map {x => Shared(x.toByteArray) }
    Future { Request(path, buf.get) }.flatMap {req => conn(req) }.map(x => ()) 
  }


  def leave(node: Node, conn: Service[Request, Response]): Future[Unit] = {
    val path = Path.Utf8("membership", "leave")
    val buf = Node.codec.encode(node).toOption.map {x => Shared(x.toByteArray) }
    Future { Request(path, buf.get) }.flatMap {req => conn(req) }.map(x => ()) 
  }

  def list(conn: Service[Request, Response]): Future[ List[Node] ] = {
    val path = Path.Utf8("membership", "list")
    val req = Request(path, Buf.Empty)


    conn(req).map { rep =>
      val buf =  BitVector( Shared.extract( rep.body )  )
      val n = Node.listCodec.decode(buf).toOption.map(x => x.value)
      n.get
    }


   }

  def bootstrap(node: Node, conn: Service[Request, Response]) = for {
    () <- join(node,conn)
    peers <- list(conn)
    ring <- Future { Neighborhood(node, Node.sortPeers(peers) ) }
  } yield ring


}
