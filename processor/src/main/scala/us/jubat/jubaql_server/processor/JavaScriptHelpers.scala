// Jubatus: Online machine learning framework for distributed environment
// Copyright (C) 2014-2015 Preferred Networks and Nippon Telegraph and Telephone Corporation.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License version 2.1 as published by the Free Software Foundation.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
package us.jubat.jubaql_server.processor

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session, Transport}

import dispatch.Defaults._
import dispatch._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object JavaScriptHelpers {
  def test(): String = {
    "test"
  }

  val h = Http()

  /**
   * Make a simple HTTP GET request.
   *
   * @param where URL to query
   * @return response body
   */
  def httpGet(where: String): Try[String] = {
    val r = url(where)
    makeRequest(r)
  }

  /**
   * Make a simple HTTP GET request with URL parameters.
   *
   * @param where URL to query
   * @param params a key-value JavaScript object (will be sent as URL
   *               parameters)
   * @return response body
   */
  def httpGet(where: String, params: java.util.Map[_, _]): Try[String] = {
    val urlParams = stringifyMap(params)
    val r = url(where) <<? urlParams
    makeRequest(r)
  }

  /**
   * Make a simple HTTP POST request without a request body.
   *
   * @param where URL to query
   * @return response body
   */
  def httpPost(where: String): Try[String] = {
    val r = url(where).POST
    makeRequest(r)
  }

  /**
   * Make a simple HTTP POST request with a request body.
   *
   * @param where URL to query
   * @param body request body as a UTF-8-encoded string
   * @return response body
   */
  def httpPost(where: String, body: String): Try[String] = {
    val r = url(where).POST
      .setBody(body.getBytes("UTF-8")).setBodyEncoding("UTF-8")
    makeRequest(r)
  }

  /**
   * Make a simple HTTP POST request with a request body.
   *
   * @param where URL to query
   * @param params a key-value JavaScript object (will be sent as a
   *               x-www-form-urlencoded string in the request body)
   * @return response body
   */
  def httpPost(where: String, params: java.util.Map[_, _]): Try[String] = {
    val postParams = stringifyMap(params)
    val r = (url(where) << postParams)
      .setContentType("application/x-www-form-urlencoded", "utf-8")
    makeRequest(r)
  }

  // TODO add authentication and give this a nicer interface
  def sendMail(smtpHost: String, smtpPort: Number,
               from: String, to: String,
               subject: String, body: String): Try[String] = {
    val properties = System.getProperties
    properties.setProperty("mail.smtp.host", smtpHost)
    properties.setProperty("mail.smtp.port", smtpPort.intValue.toString)
    /*if (!user.isEmpty) {
      properties.setProperty("mail.smtp.user", user)
    }*/
    val session = Session.getDefaultInstance(properties)
    Try {
      val msg = new MimeMessage(session)
      msg.setFrom(new InternetAddress(from))
      msg.setRecipient(Message.RecipientType.TO,
        new InternetAddress(to))
      msg.setSubject(subject)
      msg.setText(body)
      Transport.send(msg)
    } match {
      case Success(_) =>
        Success("Mail sent")
      case Failure(err) =>
        println(err)
        Failure(err)
    }
  }

  /**
   * Perform a blocking request and return the result as a Try-wrapped string.
   */
  protected def makeRequest(req: Req): Try[String] = {
    h(req OK as.String).either.map(_ match {
      case Left(err) => Failure(err)
      case Right(s) => Success(s)
    }).apply()
  }

  protected def stringifyMap(obj: java.util.Map[_, _]): Map[String, String] = {
    (obj.toList.map { case (key, value) =>
      (key.toString, value match {
        case s: String =>
          s
        case d: java.lang.Double if d.toString.endsWith(".0") =>
          d.toInt.toString
        case other => other.toString
      })
    }).toMap
  }

  def javaScriptToScala(s: String) = s
  def javaScriptToScala(x: Double) = x
}
