import AssemblyKeys._

name := "Twitter-Sentiment-Analysis"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-streaming" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.2.0",
  "com.google.code.gson" % "gson" % "2.2.2",
  "com.typesafe" % "config" % "1.2.1",
  "org.json4s" %% "json4s-native" % "3.3.0",
  "joda-time" % "joda-time" % "2.7",
  "org.joda" % "joda-convert" % "1.2",
   "org.apache.spark" %% "spark-mllib" % "1.6.0",
   "com.databricks" % "spark-csv_2.10" % "1.3.0",
    "org.apache.lucene" % "lucene-core" % "5.5.0",
    "org.apache.lucene" % "lucene-analyzers-common" % "5.5.0",
    "com.google.guava" % "guava" % "19.0",
    "com.gravity" % "goose" % "2.1.23",
    "org.scala-lang" % "jline" % "2.10.6",
        "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
    "edu.stanford.nlp" % "stanford-parser" % "3.6.0"
)

resolvers ++= Seq(
   "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
   "Spray Repository" at "http://repo.spray.cc/",
   "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
   "Akka Repository" at "http://repo.akka.io/releases/",
   "Twitter4J Repository" at "http://twitter4j.org/maven2/",
   "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
   "Twitter Maven Repo" at "http://maven.twttr.com/",
   "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/"
)


//assemblyJarName in assembly := "twitter-sentiment-assembly.jar"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
