package e2m

import bruma.iterator.IsisRecordIterator
import bruma.master
import bruma.master.{Field, Master, MasterFactory, Record, Subfield}

import java.util
import scala.annotation.tailrec
import scala.collection.{immutable, mutable}
import scala.util.{Failure, Success, Try}
import scala.jdk.CollectionConverters.*

case class I2M_Parameters(
  isisMaster: String,
  database: String,
  collection: String,
  host: Option[String],
  port: Option[Int],
  user: Option[String],
  password: Option[String],
  fieldTags: Option[Set[Int]],
  from: Option[Int],
  to: Option[Int],
  convTags: Option[Map[Integer, String]],
  clear: Boolean,
  bulkWrite: Boolean
)

class Isis2Mongo {
  def exportRecords(parameters: I2M_Parameters): Try[Unit] = {
    Try {
      val master: Master = MasterFactory.getInstance(parameters.isisMaster).open()
      val mExport: MongoExport = new MongoExport(parameters.database, parameters.collection, parameters.clear,
                                                 parameters.host, parameters.port, parameters.user, parameters.password)
      val last = master.getControlRecord.getNxtmfn - 1
      val from: Int = parameters.from.getOrElse(1)
      val to1: Int = parameters.to.getOrElse(master.getControlRecord.getNxtmfn - 1)
      val to2: Int = Math.min(to1, last)
      require(from <= to2)

      val mtags: Option[util.Map[Integer, String]] = parameters.convTags.map(tags => tags.asJava)
      mtags.foreach(tags => master.setTags(tags))
      exportRecords(master, mExport, from, to2, parameters.fieldTags, parameters.bulkWrite)
      master.close()
    }
  }

  @tailrec
  private def exportRecords(master: Master,
                            mExport: MongoExport,
                            from: Int,
                            last: Int,
                            fieldTags: Option[Set[Int]],
                            bulkWrite: Boolean): Unit = {
    val bufferSize: Int = if (bulkWrite) 10000 else 1
    if (from <= last) {
      if (bulkWrite) {
        println(s"+++$from")
        val to: Int = Math.min(from + bufferSize - 1, last)
        val docs: Seq[String] = getDocuments(master, from, to, fieldTags)
        if (docs.isEmpty) ()
        else {
          mExport.insertDocuments(docs) match {
            case Success(_) => ()
            case Failure(exception) => println(s"---Insert Documents Error. msg=${exception.toString}")
          }
        }
      } else {
        getDocument(master, from, last, fieldTags) match {
          case Some(doc) =>
            if (from % 1000 == 0) println(s"+++$from")
            mExport.insertDocument(doc) match {
              case Success(_) => ()
              case Failure(exception) => println(s"---Insert Document Error. mfn=$from msg=${exception.toString}")
            }
          case None => println(s"---Deleted document. mfn=$from")
        }
      }
      val nextId = from + bufferSize
      exportRecords(master, mExport, nextId, last, fieldTags, bulkWrite)
    }
  }

  @tailrec
  private def getDocument(master: Master,
                          mfn: Int,
                          last: Int,
                          fieldTags: Option[Set[Int]]): Option[String] = {
    if (mfn <= last) {
      Try(master.getRecord(mfn)) match {
        case Success(record) =>
          if (record.isActive) {
            val record2 = fieldTags match {
              case Some(tags) =>
                val rec2 = new Record().setMfn(record.getMfn)
                record.getFields.asScala.filter(fld => tags.contains(fld.getId)).foreach(tag => rec2.addField(tag))
                rec2
              case None => record
            }
            Some(record2.toJSON3) //record2.toJSON
          } else None
        case Failure(exception) =>
          println(s"---Record read error. mfn=$mfn msg=${exception.toString}")
          getDocument(master, mfn + 1, last, fieldTags)
      }
    } else None
  }

  private def getDocuments(master: Master,
                           from: Int,
                           _to: Int,
                           fieldTags: Option[Set[Int]]): Seq[String] = {
    require (from <= _to)

    (from to _to).foldLeft(Seq[String]()) {
      case (seq:Seq[String], mfn) =>
        val record: Record = master.getRecord(mfn)
        if (record.isActive) {
          val record2: Record = fieldTags match {
            case Some(tags) =>
              val rec2 = new Record().setMfn(record.getMfn)
              record.getFields.asScala.filter(fld => tags.contains(fld.getId)).foreach(tag => rec2.addField(tag))
              rec2
            case None => record
          }
          seq :+ toJson(record2)  //record2.toJSON3
        } else seq
    }
  }

  private def toJson(record: Record): String = {
    val fields: Map[Int, Seq[Map[Char, Seq[String]]]] = processFields(record)

    val x: String = getFields(fields)
    x
  }

  private def getFields(flds: Map[Int, Seq[Map[Char, Seq[String]]]]): String = {
    val flds1 = flds.toSeq.map {
      case (id, seq) =>
        val seq2: Seq[String] = seq.map(m => getSubfields(m))
        s"\"$id\":[${seq2.mkString(",")}]"
    }
    s"{${flds1.mkString(",")}}"
  }

  private def getSubfields(subs: Map[Char, Seq[String]]): String = {
    val subs1: Seq[String] = subs.toSeq.map {
      case (id, seq) =>
        val seq2: Seq[String] = seq.map(s => s"\"$s\"")
        s"\"$id\":[${seq2.mkString(",")}]"
    }
    val x = subs1.mkString(",")
    x
  }

  private def processFields(record: Record): Map[Int, Seq[Map[Char, Seq[String]]]] = {
    val tags: Set[Integer] = record.getRecordTags.asScala.toSet
    val fields1: Set[(Integer, Seq[Field])] = tags.map(tag => (tag, record.getFieldList(tag).asScala.toList))

    fields1.map {
      case (tag, flds) => (tag.toInt, flds.map(fld => processField(fld)))
    }.toMap
  }

  private def processField(field: Field): Map[Char, Seq[String]] = {
    val subFields: Seq[Subfield] = field.getSubfields.asScala.toSeq
    val subFields2: Seq[(Char, String)] = subFields.map(sub => (sub.getId, sub.getContent))

    subFields2.foldLeft(Map[Char, Seq[String]]()) {
      case (map, (id, content)) =>
        val seq: Seq[String] = map.getOrElse(id, Seq[String]()):+ content
        map + (id -> seq)
    }
  }
}

object Isis2Mongo {
  private def usage(): Unit = {
    System.err.println("Insert Isis documentos into a MongoDB collection")
    System.err.println("usage: Isis2Mongo <options>")
    System.err.println("Options:")
    System.err.println("-isisMaster=<path> - path to the Isis master file")
    System.err.println("-database=<name>   - MongoDB database name")
    System.err.println("-collection=<name> - MongoDB database collection name")
    System.err.println("[-host=<name>]     - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port=<number>]   - MongoDB server port number. Default value is 27017")
    System.err.println("[-user=<name>])    - MongoDB user name")
    System.err.println("[-password=<pwd>]  - MongoDB user password")
    System.err.println("[-fieldTags=<tag1>,<tag2>,<tag3>...] - record field tags that should be exported. Default behaviour is to export all fields")
    System.err.println("[-from=<mfn>]      - initial record's master file number to be exported. Default value is '1'")
    System.err.println("[-to=<mfn>         - last record's master file number to be exported. Default value is 'nxtmfn - 1'")
    System.err.println("[-convTags=<tag>:<name>>,<tag>:<name>] - convert numeric field tags into MongoDB document fields")
    System.err.println("[--clear] - if present, clears all documents of the collection before importing new ones")
    System.err.println("[--bulkWrite] - if present it will write many documents into MongoDb each iteration (requires more available RAM")
    System.exit(1)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) usage()

    val parameters: Map[String, String] = args.foldLeft[Map[String, String]](Map()) {
      case (map, par) =>
        val split = par.split(" *= *", 2)
        if (split.size == 1) map + ((split(0).substring(2), ""))
        else map + ((split(0).substring(1), split(1)))
    }

    if (!Set("isisMaster", "database", "collection").forall(parameters.contains)) usage()

    val isisMaster: String = parameters("isisMaster")
    val database: String = parameters("database")
    val collection: String = parameters("collection")

    val host: Option[String] = parameters.get("host")
    val port: Option[Int] = parameters.get("port").flatMap(_.toIntOption)
    val user: Option[String] = parameters.get("user")
    val password: Option[String] = parameters.get("password")
    val fieldTags: Option[Set[Int]] = parameters.get("fieldTags").map(_.trim).map(_.split(" *, *")).map(_.map(_.toInt)).map(_.toSet)
    val from: Option[Int] = parameters.get("from").flatMap(_.toIntOption)
    val to: Option[Int] = parameters.get("to").flatMap(_.toIntOption)
    val auxConvTags: Option[Array[Array[String]]] = parameters.get("convTags").map(_.trim).map(_.split(" *, *")).map(_.map(_.split(" *: *")))
    val auxConvTags2: Option[Array[(Integer, String)]] = auxConvTags.map(_.map(x => (x(0).toInt, x(1))))
    val convTags: Option[Map[Integer, String]] = auxConvTags2.map(arr => Map(arr:_*))
    val clear: Boolean = parameters.contains("clear")
    val bulkWrite: Boolean = parameters.contains("bulkWrite")

    val params: I2M_Parameters = I2M_Parameters(isisMaster, database, collection, host, port, user, password,
      fieldTags, from, to, convTags, clear, bulkWrite)

    (new Isis2Mongo).exportRecords(params) match {
      case Success(_) =>
        println("Export was successfull!")
        System.exit(0)
      case Failure(exception) =>
        println(s"Export error: ${exception.toString}")
        System.exit(1)
    }
  }
}