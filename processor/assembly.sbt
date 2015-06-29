test in assembly := {}

jarName in assembly := "jubaql-processor-assembly-" + version.value + ".jar"

// Scala libraries will be provided by the runtime.
assemblyOption in assembly ~= {
  _.copy(includeScala = false)
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) {
  (old) => {
    //// The following conflicts only need to be fixed when Spark dependencies
    //// are not marked as "provided":
    //
    // javax.transaction-1.1.1.v201105210645.jar:META-INF/ECLIPSEF.RSA vs.
    //  javax.servlet-3.0.0.v201112011016.jar:META-INF/ECLIPSEF.RSA vs.
    //  javax.mail.glassfish-1.4.1.v201005082020.jar:META-INF/ECLIPSEF.RSA vs.
    //  javax.activation-1.1.0.v201105071233.jar:META-INF/ECLIPSEF.RSA
    case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.discard
    // javax.mail.glassfish-1.4.1.v201005082020.jar:META-INF/mailcap vs.
    //   javax.activation-1.1.0.v201105071233.jar:META-INF/mailcap
    case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
    // slf4j-api-1.7.7.jar:META-INF/maven/org.slf4j/slf4j-api/pom.properties vs.
    //   parquet-format-2.0.0.jar:META-INF/maven/org.slf4j/slf4j-api/pom.properties
    //   and others
    case x if x.startsWith("META-INF/maven/org.slf4j/") => MergeStrategy.last
    // kryo-2.21.jar:com/esotericsoftware/minlog/Log$Logger.class vs.
    //   minlog-1.2.jar:com/esotericsoftware/minlog/Log$Logger.class
    case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
    // commons-beanutils-1.7.0.jar:org/apache/commons/beanutils/BasicDynaBean.class vs.
    //   commons-beanutils-core-1.8.0.jar:org/apache/commons/beanutils/BasicDynaBean.class
    //   and others
    case PathList("org", "apache", xs @ _*) => MergeStrategy.last
    // scala-logging-slf4j_2.10-2.1.2.jar:com/typesafe/scalalogging/slf4j/Logger$.class vs.
    //   scalalogging-slf4j_2.10-1.1.0.jar:com/typesafe/scalalogging/slf4j/Logger$.class
    case PathList("com", "typesafe", "scalalogging", xs @ _*) => MergeStrategy.last
    // javax.transaction-1.1.1.v201105210645.jar:plugin.properties vs.
    //   javax.servlet-3.0.0.v201112011016.jar:plugin.properties vs.
    //   javax.mail.glassfish-1.4.1.v201005082020.jar:plugin.properties vs.
    //   javax.activation-1.1.0.v201105071233.jar:plugin.properties
    //   and others
    case x if x.startsWith("plugin.properties") => MergeStrategy.last
    // jubatus-on-yarn-client_2.10.jar:log4j.xml vs.
    //   our own log4j.xml
    case x if x.startsWith("log4j.xml") => MergeStrategy.first
    //
    case x => old(x)
  }
}

// add "provided" dependencies back to classpath when using "sbt run".
// this does not affect the "run" function in IDEA (i.e., it can't be used)
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
