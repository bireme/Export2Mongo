package e2m

import org.json4s.*
import org.json4s.Xml.{toJson, toXml}
import org.json4s.native.JsonMethods.render
import org.json4s.native.Printer.compact

import scala.util.Try
import scala.xml.Elem
import scala.xml.XML.loadString

object Xml2Json extends App {

  def xml2json(xml: String): Try[String] = {
    Try {
      val xmlElem: Elem = loadString(xml)
      val json: JValue = toJson(xmlElem)

      compact(render(json))
    }
  }

  val fruits: String = """
    <fruits>
      <fruit>
        <name>apple</name>
        <taste>
          <sweet>true</sweet>
          <juicy>true</juicy>
        </taste>
      </fruit>
      <fruit>
        <name>banana</name>
        <taste>better</taste>
      </fruit>
    </fruits>"""

  println(xml2json(fruits))
}
