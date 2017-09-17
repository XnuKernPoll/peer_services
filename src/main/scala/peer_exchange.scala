package peer_services.exchange 
import topologies._

import com.twitter.finagle, com.twitter.{util, io}
import finagle.mux._, finagle.{Service, Failure, Path, Mux}
import util.{ Future, Duration }
import scala.util.Random

import scodec.bits._
import store_lib.storage.util.BV


/** The server merges the clients and it's own Neighborhood, then updates it's state, and returns the new merged Neighborhood to the client */
class Server(p: PeerState) extends Service[Request, Response] {
  def apply(req: Request) = req.destination match {

    case Path.Utf8("exchange") =>

      val buf = BV.fromBuf(req.body)
      val rightNeighbor = Future{ Neighborhood.codec.decode(buf).toOption.map(x => x.value).get }

      val result = rightNeighbor.map { x =>
        val stateChange = Neighborhood.merge(p.peers, x)
        p.become(stateChange)
        ( Neighborhood.codec.encode( stateChange ) ).toOption.map {x => BV.toBuf(x) }
      }

      result.flatMap { n =>
        n match {case Some(buf) => Future { Response(buf) }; case None => Future.exception{ Failure.rejected("Could not complete merge") }   } 
      }


    case _ => Future.exception( Failure.rejected("no such route") )

  }
}


/** 
a module that allows you to merge PeerState / Neighborhood state, with a remote peer, and an antientropy service to maintain routing table state. 
*/
object Client {

  def exchange(conn: Service[Request, Response], neighborhood: Neighborhood): Future[Neighborhood] = {
    val path = Path.Utf8("exchange")

    val payload = Future {
      val pl = Neighborhood.codec.encode(neighborhood).toOption.map {x => BV.toBuf(x) }
      pl.get
    }


    val rep = payload.flatMap { b => conn( Request(path, b) )  }

    rep.map {response =>
      val buf = BV.fromBuf( response.body )
      Neighborhood.codec.decode(buf).toOption.map { x => x.value }.get 
    }

  }

  def sampler(peer_state: PeerState, cli: Mux.Client) = {

    val node = Random.shuffle( peer_state.peers.neighbors).head
    val c = cli.newService(node.key)
    val rep = exchange(c, peer_state.peers)

    rep onSuccess { neighbors => peer_state.become(neighbors); c.close() }
    rep onFailure { e => peer_state.leave(node); c.close()  } 
  }


  def antiEntropy(peer_state: PeerState, interval: Duration) = {
    val t = new util.JavaTimer()
    val client = Mux.client 
    t.schedule(interval) { sampler(peer_state, client) }  
  }


}
