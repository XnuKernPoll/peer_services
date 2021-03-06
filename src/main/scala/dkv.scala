package peer_services.kv
import peer_services.dynamo._, com.twitter.finagle
import finagle.http.{Request, Response, Method}, finagle.http.path._
import peer_services._
import com.google.common.hash._
import java.nio.charset.StandardCharsets.UTF_8
import finagle.Http
import store_lib.server.Handlers._

object Modules {
  def sha1 = Hashing.sha1()

  val bucketOPS: PartialFunction[(Method, Path), OP] = {
    case Method.Post -> root / "api" / "v1" / "buckets" => WRITE 
    case Method.Delete -> root / "api" / "v1" / "buckets"/ p => DELETE  
  }

  val kvOPS: PartialFunction[ (Method, Path), OP] = {
    case Method.Put -> root / "api" / "v1" / "kv" / b / k => WRITE
    case Method.Get -> root / "api" / "v1" / "kv" / b / k => READ
    case Method.Delete -> root / "api" / "v1" / "kv" / b / k => DELETE
    case Method.Get -> root / "api" / "v1" / "kv" / b => READ 
  }

  def getOP(req: Request) = {
    val data = (req.method, Path(req.path) )
    val nf = ( bucketOPS orElse kvOPS)
    nf(data)
  }


  def keyHasher(bucket: String) =  sha1.hashString(bucket, UTF_8).asBytes() 
  
  def kvKey(req: Request) = (req.method, Path(req.path)) match {
    case Method.Put -> root / "api" / "v1" / "kv" / b / k => keyHasher(b)
    case Method.Get -> root / "api" / "v1" / "kv" / b / k => keyHasher(b)
    case Method.Delete -> root / "api" / "v1" / "kv" / b / k => keyHasher(b)
    case Method.Get -> root / "api" / "v1" / "kv" / b  => keyHasher(b)
  }

  def getKey(req: Request) = if (req.path == "/api/v1/buckets") keyHasher(req.contentString) else kvKey(req)

 
  def init_server(state: RouterState, rsrcs: Resources) = {
    val cli = Http.client
    val svc = Server(rsrcs)
    val dynFilt = new RoutingFilters.DynamoBehavior(state, cli, getOP)
    dynFilt andThen svc
  }

}
