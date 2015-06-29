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

  protected var _interrupted = false

  def stopped: Boolean = _stopped || _interrupted

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
            case Some("stop-and-poll") if !_interrupted =>
              // if we see stop-and-poll state, we must tell Jubatus
              // instances to stop running, but the polling should
              // continue until we see either "shutdown" or "running".
              // in both cases we should exit this loop, but in the
              // latter case we should reset `running` and `_stopped`
              // to their initial values.
              logger.debug("status switched to 'stop-and-poll', stopping")
              synchronized {
                _interrupted = true
              }
            case Some("running") if _interrupted =>
              // in this state, the server has switched from "stop-and-poll"
              // to "running", i.e. we should set this object back to its
              // initial state after breaking out of the loop
              logger.debug("status switched back to 'running', resetting")
              synchronized {
                _stopped = true
              }
            case _ =>
          }
          if (!_interrupted && !_stopped) {
            Thread.sleep(1000)
          } else if (_interrupted && !_stopped) {
            // poll more frequently when we are waiting to get back to
            // "running" state
            Thread.sleep(300)
          } else {
            // when we are stopped, exit immediately
          }
        }
        h.shutdown()
        if (_stopped && _interrupted) {
          _stopped = false
          _interrupted = false
          running = false
          logger.debug("HTTP poller reset to initial state")
        }
      }
      // indicate that we are running now
      running = true
    }
  }
}
