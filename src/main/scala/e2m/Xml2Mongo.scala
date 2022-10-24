package e2m

import org.json4s.JValue
import org.json4s.Xml.toJson
import org.json4s.native.JsonMethods.render
import org.json4s.native.Printer.compact

import java.io.{BufferedWriter, File, FileWriter}
import scala.annotation.tailrec
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}
import scala.xml.Elem
import scala.xml.XML.loadString

case class X2M_Parameters(xmlDir: String,
                          database: String,
                          collection: String,
                          host: Option[String],
                          port: Option[Int],
                          user: Option[String],
                          password: Option[String],
                          xmlFilter: Option[String],
                          xmlFileEncod: Option[String],
                          logFile: Option[String],
                          recursive: Boolean,
                          clear: Boolean,
                          bulkWrite: Boolean)

class Xml2Mongo {
  def exportFiles(parameters: X2M_Parameters): Try[Unit] = {
    Try {
      val mExport: MongoExport = new MongoExport(parameters.database, parameters.collection, parameters.clear,
        parameters.host, parameters.port, parameters.user, parameters.password)
      val xmls: Set[File] = getFiles(new File(parameters.xmlDir), parameters.xmlFilter, parameters.recursive)
      val xmlFileEncod: String = parameters.xmlFileEncod.getOrElse("utf-8")
      val expFile: Option[BufferedWriter] = parameters.logFile.map(name => new BufferedWriter(new FileWriter(name)))

      if parameters.bulkWrite then exportFiles(mExport, xmls, xmlFileEncod, expFile)
      else xmls.foreach {
        xml =>
          exportFile(mExport, xml, xmlFileEncod, expFile) match {
            case Success(_) => println(s"+++xml=$xml")
            case Failure(exception) => println(s"export files error: ${exception.getMessage}")
          }
      }

      expFile.foreach(_.close())
      mExport.close()
    }
  }

  @tailrec
  private def exportFiles(mExport: MongoExport,
                          xmls: Set[File],
                          xmlFileEncod: String,
                          logFile: Option[BufferedWriter]): Unit = {
    if (xmls.nonEmpty) {
      val bufferSize: Int = 500
      val (pref: Set[File], suff: Set[File]) = xmls.splitAt(bufferSize)
      val pref1: Set[(File, File)] = pref.map(x => (x, x))
      val pref2: Set[(String, Try[String])] = pref1.map(f => (f._1.getAbsolutePath, getFileContent(f._2, xmlFileEncod)))
      val pref3: Set[(String, Try[String])] = pref2.map(f => (f._1, f._2.flatMap(xml2json)))
      val (goods, bads) = pref3.span(_._2.isSuccess)

      bads.foreach(x => logFile.foreach(_.write(x._1)))

      println("+++")
      mExport.insertDocuments(goods.map(_._2.get).toSeq) match {
        case Success(_) => ()
        case Failure(exception) => println(s"export files error: ${exception.getMessage}")
      }

      exportFiles(mExport, suff, xmlFileEncod, logFile)
    }
  }

  private def exportFile(mExport: MongoExport,
                         xml: File,
                         xmlFileEncod: String,
                         logFile: Option[BufferedWriter]): Try[String] = {
    val result: Try[String] = for {
      content <- getFileContent(xml, xmlFileEncod)
      json <- xml2json(content)
      id <- mExport.insertDocument(json)
    } yield id

    result match {
      case Success(id) => Success(id)
      case Failure(exception) =>
        logFile.foreach(_.write(xml.getAbsolutePath))
        Failure(exception)
    }
  }

  private def getFileContent(xml: File,
                             xmlFileEncod: String): Try[String] = {
    Try {
      val source: BufferedSource = Source.fromFile(xml, xmlFileEncod)
      val content: String = source.getLines().mkString("\n")
      source.close()

      content
    }
  }

  private def getFiles(file: File,
                       filter: Option[String],
                       recursive: Boolean,
                       isRoot: Boolean = true): Set[File] = {
    file match {
      case f if f.isDirectory =>
        if (isRoot || recursive)
          file.listFiles().foldLeft(Set[File]()) {
            case (set, nfile) => set ++ getFiles(nfile, filter, recursive, isRoot = false)
          }
        else Set[File]()
      case f if f.exists() =>
        filter match {
          case Some(flt) => if file.getName.matches(flt) then Set(file) else Set[File]()
          case None => Set(f)
        }
      case _ => throw new IllegalArgumentException(file.getCanonicalPath)
    }
  }

  def xml2json(xml: String): Try[String] = {
    Try {
      val xmlElem: Elem = loadString(xml)
      val json: JValue = toJson(xmlElem)

      compact(render(json))
    }
  }
}

object Xml2Mongo {
  private def usage(): Unit = {
    System.err.println("Insert XML documentos into a MongoDB collection")
    System.err.println("usage: Xml2Mongo <options>")
    System.err.println("Options:")
    System.err.println("-xmlDir=<path>     - directory of the XML files")
    System.err.println("-database=<name>   - MongoDB database name")
    System.err.println("-collection=<name> - MongoDB database collection name")
    System.err.println("[-host=<name>]     - MongoDB server name. Default value is 'localhost'")
    System.err.println("[-port=<number>]   - MongoDB server port number. Default value is 27017")
    System.err.println("[-user=<name>])    - MongoDB user name")
    System.err.println("[-password=<pwd>]  - MongoDB user password")
    System.err.println("[-xmlFilter=<regex>]  - if present, uses the regular expression to filter the desired xml file names")
    System.err.println("[-xmlFileEncod=<enc>] - if present, indicate the xml file encoding. Default is utf-8")
    System.err.println("[-logFile=<path>]     - if present, indicate the name of a log file with the names XML files that were not imported because of bugs")
    System.err.println("[--recursive]         - if present, look for xml documents in subdirectories")
    System.err.println("[--clear]             - if present, clear all documents of the collection before importing new ones")
    System.err.println("[--bulkWrite]         - if present it will write many documents into MongoDb each iteration (requires more available RAM")
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

    if (!Set("xmlDir", "database", "collection").forall(parameters.contains)) usage()

    val xmlDir: String = parameters("xmlDir")
    val database: String = parameters("database")
    val collection: String = parameters("collection")

    val host: Option[String] = parameters.get("host")
    val port: Option[Int] = parameters.get("port").flatMap(_.toIntOption)
    val user: Option[String] = parameters.get("user")
    val password: Option[String] = parameters.get("password")
    val xmlFilter: Option[String] = parameters.get("xmlFilter")
    val xmlFileEncod: Option[String] = parameters.get("xmlFileEncod")
    val logFile: Option[String] = parameters.get("logFile")
    val recursive: Boolean = parameters.contains("recursive")
    val clear: Boolean = parameters.contains("clear")
    val bulkWrite: Boolean = parameters.contains("bulkWrite")

    val params: X2M_Parameters = X2M_Parameters(xmlDir, database, collection, host, port, user, password, xmlFilter,
      xmlFileEncod, logFile, recursive, clear, bulkWrite)

    (new Xml2Mongo).exportFiles(params) match {
      case Success(_) =>
        println("Export was successfull!")
        System.exit(0)
      case Failure(exception) =>
        println(s"Export error: ${exception.toString}")
        System.exit(1)
    }
  }
}
