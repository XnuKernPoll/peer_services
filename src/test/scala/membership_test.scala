package peer_services

import org.scalatest._
import peer_services.membership._
import topologies._
import com.google.common.hash._
import org.scalatest.Matchers._ 
import com.twitter.util.{Await, Future}
import java.nio.charset.StandardCharsets.UTF_8


//Node codec doesn't work 

class MembershipSuite extends FunSuite {

  val sha1 = Hashing.sha1()
  def keyHasher(bucket: String) =  sha1.hashString(bucket, UTF_8).asBytes()

  val nodes = List("node1", "node2", "node3", "node4", "node5").map(x => Node(x, keyHasher(x) ) )

  val nbrhd = Neighborhood(nodes(0), nodes)
  val pstate = new PeerState(nbrhd)

  val server = PeerManager.server(pstate)
  val nn = Node("node6", keyHasher("node6") ) 

  
  test ("List Peers") {
    val l = Await.result( MembershipClient.list(server)).map(x => x.key)
    l should contain theSameElementsAs nodes.map(x => x.key)

  } 
 

  test ("Join") {
    Await.result( MembershipClient.join(nn, server) )
    assert( pstate.peers.neighbors.map(x => x.key).contains(nn.key)  )
  }



 
  test ("Leave") {
    Await.result(MembershipClient.leave(nn, server) )
    val cond = Await.result(MembershipClient.list(server) ).map(x => x.key).contains(nn.key)
    assert(cond != true) 
  }
  


}
