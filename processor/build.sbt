import com.typesafe.sbt.SbtStartScript
import java.io.File

name := "JubaQL Processor"

version := "1.2.0"

// use 2.10 for now (Spark has no 2.11 support yet)
scalaVersion := "2.10.4"

// to prevent problems with encfs path length issues
scalacOptions ++= Seq( "-Xmax-classfile-name", "140" )

// Add Jubatus repository
resolvers += "Jubatus" at "http://download.jubat.us/maven"

// Add Cloudera repository
resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

// Add msgpack repository (sbt does not use the information provided in the Jubatus POM)
resolvers += "MessagePack" at "http://msgpack.org/maven2"

libraryDependencies ++= Seq(
  // logging
  "com.typesafe.scala-logging" %% "scala-logging-slf4j"    % "2.1.2",
  "org.slf4j"                  %  "slf4j-api"              % "1.7.7",
  "org.slf4j"                  %  "slf4j-log4j12"          % "1.7.7",
  // Jubatus
  "us.jubat"                   % "jubatus"                 % "0.6.0"
            exclude("org.jboss.netty", "netty"),
  // jubatusonyarn
  "us.jubat"                   %% "jubatus-on-yarn-client"    % "1.0"
            exclude("javax.servlet", "servlet-api")
            exclude("org.jboss.netty", "netty"),
  // HTTP server
  "com.twitter"                %% "finagle-http"           % "6.7.4",
  "org.json4s"                 %% "json4s-native"          % "3.2.10",
  "org.json4s"                 %% "json4s-ext"             % "3.2.10",
  // parsing of program arguments
  "com.github.scopt"           %% "scopt"                  % "3.2.0",
  // Spark
  "org.apache.spark"           %% "spark-core"             % "1.1.1" % "provided",
  // the following will prevent org.spark-project.akka:akka-remote_2.10:2.2.3-shaded-protobuf
  // from pulling in io.netty:netty:3.6.6.Final, but it will not prevent spark-core
  // itself to pull in io.netty:netty-all:4.0.23.Final (note that the former
  // includes the package "org.jboss.netty", while the latter includes "io.netty".)
  "org.spark-project.akka"     %% "akka-remote"            % "2.2.3-shaded-protobuf"
            exclude("io.netty", "netty"),
  "org.apache.spark"           %% "spark-streaming"        % "1.1.1" % "provided",
  "org.apache.spark"           %% "spark-streaming-kafka"  % "1.1.1"
            exclude("commons-beanutils", "commons-beanutils")
            exclude("commons-collections", "commons-collections")
            exclude("com.esotericsoftware.minlog", "minlog"),
  "org.apache.spark"           %% "spark-sql"              % "1.1.1",
  // registration with the gateway
  "net.databinder.dispatch"    %% "dispatch-core"          % "0.11.2",
  // HDFS
  "org.apache.hadoop"          % "hadoop-client"           % "2.5.0-cdh5.2.0" % "provided",
  // for testing
  "org.scalatest"              %% "scalatest"              % "2.2.1"   % "test",
  "net.databinder"             %% "unfiltered-filter"      % "0.8.2"   % "test",
  "net.databinder"             %% "unfiltered-json4s"      % "0.8.2"   % "test",
  "net.databinder"             %% "unfiltered-netty-server" % "0.8.2"  % "test"
)

// disable parallel test execution to avoid conflicting to launch jubatus when mocking
// Jubatus servers
parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

// add the "start-script" task as per
// <https://github.com/sbt/sbt-start-script#details>
seq(SbtStartScript.startScriptForClassesSettings: _*)

SbtStartScript.StartScriptKeys.startScriptName <<= baseDirectory / "start-script/run"

// add "provided" dependencies back to classpath when using "sbt start-script".
SbtStartScript.StartScriptKeys.relativeFullClasspathString in Compile <<=
  (SbtStartScript.StartScriptKeys.startScriptBaseDirectory, fullClasspath in Compile) map myRelativeClasspathStringTask

// the three functions below are 1:1 copies (with changed names) from
// SbtStartScript.scala, with the `private` modifier removed because there seems
// to be no other way to modify the classpath for sbt-start-script;
// cf. <https://github.com/sbt/sbt-start-script/issues/47>

def myRelativeClasspathStringTask(baseDirectory: File, cp: Classpath) = {
  SbtStartScript.RelativeClasspathString(cp.files map { f => myRelativizeFile(baseDirectory, f, "$PROJECT_DIR") } mkString ("", java.io.File.pathSeparator, ""))
}

def myRelativizeFile(baseDirectory: File, f: File, prefix: String = ".") = {
  if (java.io.File.separatorChar != '/') {
    f
  } else {
    val baseCanonical = baseDirectory.getCanonicalFile()
    val fCanonical = f.getCanonicalFile()
    if (myDirectoryEqualsOrContains(baseCanonical, fCanonical)) {
      val basePath = baseCanonical.getAbsolutePath()
      val fPath = fCanonical.getAbsolutePath()
      if (fPath.startsWith(basePath)) {
        new File(prefix + fPath.substring(basePath.length))
      } else {
        sys.error("Internal bug: %s contains %s but is not a prefix of it".format(basePath, fPath))
      }
    } else {
      // leave it as-is, don't even canonicalize
      f
    }
  }
}

def myDirectoryEqualsOrContains(d: File, f: File): Boolean = {
  if (d == f) {
    true
  } else {
    val p = f.getParentFile()
    if (p == null)
      false
    else
      myDirectoryEqualsOrContains(d, p)
  }
}
