package e2m

import org.json.*
import org.w3c.dom.{Document, Node, NodeList}

import java.io.{BufferedWriter, File, FileReader, FileWriter, Reader, StringWriter}
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.Calendar
import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}
import javax.xml.transform.{OutputKeys, TransformerFactory}
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult
import javax.xml.xpath.{XPath, XPathConstants, XPathFactory}
import scala.annotation.tailrec
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

case class X2M_Parameters(xmlDir: String,
                          database: String,
                          collection: String,
                          host: Option[String],
                          port: Option[Int],
                          user: Option[String],
                          password: Option[String],
                          xmlFilter: Option[String],
                          xmlFileEncod: Option[String],
                          xpath: Option[String],
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
      val xpath: Option[String] = parameters.xpath
      val expFile: Option[BufferedWriter] = parameters.logFile.map(name => new BufferedWriter(new FileWriter(name)))

      if parameters.bulkWrite then exportFiles(mExport, xmls, xmlFileEncod, xpath, expFile)
      else xmls.foreach(exportFile(mExport, _, xmlFileEncod, xpath, expFile))

      expFile.foreach(_.close())
      mExport.close()
    }
  }

  @tailrec
  private def exportFiles(mExport: MongoExport,
                          xmls: Set[File],
                          xmlFileEncod: String,
                          xpath: Option[String],
                          logFile: Option[BufferedWriter]): Unit = {
    if (xmls.nonEmpty) {
      val bufferSize: Int = 500
      val (pref: Set[File], suff: Set[File]) = xmls.splitAt(bufferSize)
      val pref1: Set[(String, Try[Seq[String]])] =
        pref.map(x => (x.getAbsolutePath, getFileContent(x, xmlFileEncod, xpath, logFile)))
      val (goods, bads) = pref1.span(_._2.isSuccess)
      val pref2: Set[(String, Seq[String])] = goods.map(f => (f._1, f._2.get))
      val pref3: Set[(String, String)] = pref2.foldLeft(Set[(String,String)]()) {
        case (set, (name, seq)) => set ++ seq.map((name,_))
      }
      val pref4: Set[(String, Try[String])] = pref3.map(f => (f._1, xml2json(f._2)))
      val (goods1, bads1) = pref4.span(_._2.isSuccess)
      val goods2: Set[(String, String)] = goods1.map(f => (f._1, f._2.get))
      (bads.map(_._1) ++ bads1.map(_._1)).foreach(x => logFile.foreach(_.write(x)))

      println("+++")
      mExport.insertDocuments(goods2.map(_._2).toSeq) match {
        case Success(_) => ()
        case Failure(exception) => println(s"export files error: ${exception.getMessage}")
      }

      exportFiles(mExport, suff, xmlFileEncod, xpath, logFile)
    }
  }

  private def exportFile(mExport: MongoExport,
                         xml: File,
                         xmlFileEncod: String,
                         xpath: Option[String],
                         logFile: Option[BufferedWriter]): Unit = {
    println(s"+++ ${xml.getName} (${Files.size(xml.toPath)})")

    getFileContent(xml, xmlFileEncod, xpath, logFile) match {
      case Success(contents) =>
        contents.foreach {
          content =>
            xml2json(content) match {
              case Success(json) =>
                mExport.insertDocument(json) match {
                  case Success(_) => ()
                  case Failure(exception) => logFile.foreach(_.write(exception.toString))
                }
              case Failure(exception) => logFile.foreach(_.write(exception.toString))
            }
        }
      case Failure(exception) =>
        logFile.foreach(_.write(exception.toString))
    }
  }

  private def getFileContent(xml: File,
                             xmlFileEncod: String,
                             xpath: Option[String],
                             logFile: Option[BufferedWriter]): Try[Seq[String]] = {
    Try {
      xpath match {
        case Some(xp) =>
          val dbf: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
          val db: DocumentBuilder = dbf.newDocumentBuilder()
          val xmlDocument: Document = db.parse(xml)
          val xPath: XPath = XPathFactory.newInstance().newXPath()
          val nodeList: NodeList = xPath.compile(xp).evaluate(xmlDocument, XPathConstants.NODESET).asInstanceOf[NodeList]
          val nodeSeq: Seq[Node] = NodeList2SeqNode(nodeList)
          val trySeq: Seq[Try[String]] = nodeSeq.map(node2String)
          val (goods, bads) = trySeq.span(_.isSuccess)

          bads.foreach(x => logFile.foreach(_.write(x.get)))
          goods.map(_.get)
        case None =>
          val source: BufferedSource = Source.fromFile(xml, xmlFileEncod)
          val content: String = source.getLines().mkString("\n")
          source.close()

          Seq(content)
      }
    }
  }

  private def NodeList2SeqNode(nodeList: NodeList): Seq[Node] = {
    (0 until nodeList.getLength).foldLeft(Seq[Node]()) {
      case (seq, idx) =>
        val node: Node = nodeList.item(idx)
        seq :+ node
    }
  }

  private def node2String(node: Node): Try[String] = {
    Try {
      val sw = new StringWriter()
      val t = TransformerFactory.newInstance().newTransformer()
      t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes")
      t.setOutputProperty(OutputKeys.INDENT, "yes")
      t.transform(new DOMSource(node), new StreamResult(sw))

      sw.toString
    }
  }

  private def getFiles(file: File,
                       filter: Option[String],
                       recursive: Boolean,
                       isRoot: Boolean = true): Set[File] = {
    file match
      case f if f.isDirectory =>
        if (isRoot || recursive)
          file.listFiles().foldLeft(Set[File]()) {
            case (set, nfile) => set ++ getFiles(nfile, filter, recursive, isRoot = false)
          }
        else Set[File]()
      case f if f.exists() =>
        filter match
          case Some(flt) => if file.getName.matches(flt) then Set(file) else Set[File]()
          case None => Set(f)
      case _ => throw new IllegalArgumentException(file.getCanonicalPath)
  }

  def xml2json(xml: String): Try[String] = {
    Try {
      val json: JSONObject = org.json.XML.toJSONObject(xml)
      val out = json.toString(1)
      out
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
    System.err.println("[-xpath=<spec>]       - if present, the xpath will be used to extract many xml documents from one xml file")
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
        else map + (split(0).substring(1) -> split(1))
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
    val xpath: Option[String] = parameters.get("xpath")
    val logFile: Option[String] = parameters.get("logFile")
    val recursive: Boolean = parameters.contains("recursive")
    val clear: Boolean = parameters.contains("clear")
    val bulkWrite: Boolean = parameters.contains("bulkWrite")

    val params: X2M_Parameters = X2M_Parameters(xmlDir, database, collection, host, port, user, password, xmlFilter,
      xmlFileEncod, xpath, logFile, recursive, clear, bulkWrite)
    val time1: Long = Calendar.getInstance().getTimeInMillis

    (new Xml2Mongo).exportFiles(params) match {
      case Success(_) =>
        println("Export was successfull!")
        val time2: Long = Calendar.getInstance().getTimeInMillis
        println(s"Diff time=${time2 - time1}ms")
        System.exit(0)
      case Failure(exception) =>
        println(s"Export error: ${exception.toString}")
        System.exit(1)
    }
  }
}