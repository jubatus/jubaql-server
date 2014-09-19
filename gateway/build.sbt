name := "JubaQL Gateway"

version := "1.2.0"

// use an older version than necessary to use the same set of dependencies
// across projects
scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  // logging
  "com.typesafe.scala-logging" %% "scala-logging-slf4j"  % "2.1.2",
  "org.slf4j"                  %  "slf4j-api"      % "1.7.7",
  "org.slf4j"                  %  "slf4j-log4j12"  % "1.7.7",
  // HTTP server interface
  "net.databinder"    %% "unfiltered-filter"       % "0.8.2",
  "net.databinder"    %% "unfiltered-netty-server" % "0.8.2",
  "net.databinder"    %% "unfiltered-json4s"       % "0.8.2",
  "org.json4s"                 %% "json4s-ext"     % "3.2.10",
  // making HTTP requests
  "net.databinder.dispatch"    %% "dispatch-core"  % "0.11.2",
  // parsing of program arguments
  "com.github.scopt"           %% "scopt"          % "3.2.0",
  // testing
  "org.scalatest"     %% "scalatest"               % "2.2.1"
)

// disable parallel test execution to avoid BindException when mocking
// HTTP servers
parallelExecution in Test := false
