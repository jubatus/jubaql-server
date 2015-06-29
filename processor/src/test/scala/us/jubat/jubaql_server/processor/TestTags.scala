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

import org.scalatest.Tag

/*
 * These are tags for tests so that, for example, only tests that require no
 * network connection can be run.
 * When testing from sbt, use
 * > testOnly us.jubat.jubaql_server.processor.* -- -l jubaql.HDFSTest
 * to exclude HDFSTest-tagged tests from the run, and
 * > testOnly us.jubat.jubaql_server.processor.* -- -n jubaql.LocalTest
 * to include only LocalTest-tagged tests in the run.
 */

// used for tests that require no network connection or any external service
object LocalTest extends Tag("jubaql.LocalTest")

// used for tests that require Jubatus being installed locally
object JubatusTest extends Tag("jubaql.JubatusTest")

// used for tests that use a HDFS installation in some network
object HDFSTest extends Tag("jubaql.HDFSTest")

// used for tests that use a Kafka installation in some network
object KafkaTest extends Tag("jubaql.KafkaTest")

// can be used to run just one single test
object JustThisTest extends Tag("jubaql.JustThisTest")
