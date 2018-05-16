name := "template-java-parallel-ecommercerecommendation"

scalaVersion := "2.11.11"

scalaVersion in ThisBuild := "2.11.11"

version := "0.1.0"

val mahoutVersion = "0.13.0"


//resolvers += Resolver.mavenLocal


libraryDependencies ++= Seq(
  "org.apache.predictionio" %% "apache-predictionio-core" % "0.12.1" % "provided",
  "org.apache.spark" %% "spark-core" % "2.1.1" % "provided",
  "org.apache.spark"        %% "spark-mllib" % "2.1.1"    % "provided",
  "org.jblas"                % "jblas" % "1.2.4",
  "org.xerial.snappy" % "snappy-java" % "1.1.1.7",

  //mahout's libs
  "org.apache.mahout" %% "mahout-math-scala" % mahoutVersion,
  "org.apache.mahout" %% "mahout-spark" % mahoutVersion
    exclude("org.apache.spark", "spark-core_2.11"),
  "org.apache.mahout" % "mahout-math" % mahoutVersion,
  "org.apache.mahout" % "mahout-hdfs" % mahoutVersion
    exclude("com.thoughtworks.xstream", "xstream")
    exclude("org.apache.hadoop", "hadoop-client"),




  "org.json4s" %% "json4s-native" % "3.2.10",

  "com.thoughtworks.xstream" % "xstream" % "1.4.4"
    exclude("xmlpull", "xmlpull"),
//  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.1.2"
//    exclude("org.apache.spark", "spark-catalyst_2.10")
//    exclude("org.apache.spark", "spark-sql_2.10"),
  // https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark
//  "org.elasticsearch" %% "elasticsearch-spark" % "2.4.5"
//    exclude("org.apache.spark", "spark-catalyst_2.10")
//    exclude("org.apache.spark", "spark-sql_2.10"),
  "org.elasticsearch" % "elasticsearch" % "5.5.2",
  "org.apache.predictionio" %% "apache-predictionio-data-elasticsearch" % "0.12.1"
    exclude("org.apache.spark", "spark-*"),

  "com.google.code.gson"    % "gson"             % "2.5",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "com.fatboyindustrial.gson-jodatime-serialisers" % "gson-jodatime-serialisers" % "1.6.0",
  "org.elasticsearch.client" % "transport" % "5.5.2"
//  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
//  "com.google.guava" % "guava" % "12.0"
).map(_.exclude("org.apache.lucene","lucene-core")).map(_.exclude("org.apache.lucene","lucene-analyzers-common"))

resolvers += "Temp Scala 2.11 build of Mahout" at "https://github.com/actionml/mahout_2.11/raw/mvn-repo/"

resolvers += Resolver.mavenLocal



assemblyMergeStrategy in assembly := {
  case "plugin.properties" => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "package-info.class" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "UnusedStubClass.class" =>
    MergeStrategy.first
  case x => MergeStrategy.first

  // case x =>
  //   val oldStrategy = (assemblyMergeStrategy in assembly).value
  //   oldStrategy(x)
}
