name := "SparkRDF"

version := "0.1"

scalaVersion := "2.11.7"
assemblyJarName in assembly := "runner.jar"
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.0"%"provided",
  "org.apache.spark" % "spark-graphx_2.11" % "1.6.0"%"provided",
  "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1",
  "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.7.1" % "provided",
  "org.apache.hadoop" % "hadoop-streaming" % "2.7.1" % "provided",
  "org.apache.jena" % "jena-arq" % "2.13.0"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => old(x)
}
}