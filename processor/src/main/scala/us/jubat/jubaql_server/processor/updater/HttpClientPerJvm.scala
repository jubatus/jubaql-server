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
package us.jubat.jubaql_server.processor.updater

import scala.concurrent.future
import scala.concurrent.ExecutionContext.Implicits.global
import dispatch._
import com.typesafe.scalalogging.slf4j.LazyLogging

object HttpClientPerJvm extends LazyLogging {
  // flag to store whether we are already running the status checker
  protected var running = false

  protected var _stopped = false

  def stopped: Boolean = _stopped

  def startChecking(statusUrl: String) = synchronized {
    // only start the check if none is running already
    if (!running) {
      // start in a separate thread
      future {
        val h = new Http()
        // unless we see a "shutdown" message or an error, keep running
        while (!_stopped) {
          h(url(statusUrl) OK as.String).option.apply() match {
            case Some("shutdown") | None =>
              // if we see shutdown state, stop running
              logger.debug("status switched to 'shutdown', stopping")
              synchronized {
                _stopped = true
              }
            case _ =>
          }
          Thread.sleep(1000)
        }
        h.shutdown()
      }
      // indicate that we are running now
      running = true
    }
  }
}
