package e2m

import reactivemongo.api.MongoConnection.ParsedURI
import reactivemongo.api.{AsyncDriver, Collection, DB, MongoConnection}

import scala.util.{Failure, Success}
import scala.util.Try
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

import reactivemongo.api.bson.{BSONDocument, BSONElement}
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.bson.collection.BSONCollection
import reactivemongo.api.collections.GenericCollection

import reactivemongo.api._

class MongoExportReactive(database: String,
                          collection: String,
                          clear: Boolean = false,
                          host: Option[String] = None,
                          port: Option[Int] = None,
                          user: Option[String] = None,
                          password: Option[String] = None) {
  require ((user.isEmpty && password.isEmpty) || (user.nonEmpty && password.nonEmpty))

  val hostStr: String = host.getOrElse("localhost")
  val portStr: String = port.getOrElse(27017).toString
  val usrPswStr: String = user match
    case Some(usr) => s"$usr:${password.get}@"
    case None => ""

  // mongodb://[username:password@]host[:port][,...hostN[:port]][/[database][?parameter_list]]
  val mongoUri: String = s"mongodb://$usrPswStr$hostStr:$portStr"
  val driver: AsyncDriver = new AsyncDriver()
  val parsedUri: Future[ParsedURI] = MongoConnection.fromString(mongoUri)
  val futureConnection: Future[MongoConnection] = parsedUri.flatMap(driver.connect)
  val db: Future[DB] = futureConnection.flatMap(_.database(database))
  val col:Future[BSONCollection] = {
    if clear then
      val col1 = db.map(dbx => dbx.collection(collection))
      col1.map(_.drop())
    db.map(dbx => dbx.collection(collection))
  }

  val duration: FiniteDuration = scala.concurrent.duration.FiniteDuration(1L, java.util.concurrent.TimeUnit.MINUTES)
  given FiniteDuration = duration


  def close(): Future[Any] = futureConnection.flatMap(_.close())

  def addDocument(doc: Seq[(String, Any)]): Try[Unit] = {

    val document: BSONDocument = doc.foldLeft(BSONDocument()) {
      case (bson, field) =>
        field._2 match {
          case num: Int => bson ++ BSONElement(field._1, num)
          case other => bson ++ BSONElement(field._1, other.toString)
        }
    }

    Try {
      col.onComplete {
        case Success(coll) =>
          val writeRes: Future[WriteResult] = coll.insert.one(document)
          writeRes.onComplete {
            case Failure(exception) => throw new Exception(exception)
            case Success(_) => ()
          }
        case Failure(exception) =>
          println(s"exception=$exception")
      }
    }
  }
}

object MongoExportReactive extends App {
  val database = "teste_db"
  val collection = "col_db"
  val host = Some("localhost")
  val port = Some(8080)

  val mExp = new MongoExportReactive(database, collection, host = host, port = port, clear = true)
  val doc = Seq("nome" -> "Heitor", "hora" -> 15, "msg" -> "Hello!")

  mExp.addDocument(doc) match {
    case Success(_) => println("written!")
    case Failure(exception) => println(exception)
  }

  mExp.close()
}
