name := "HdfsCompression"

version := "1.0"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
"org.apache.hadoop" % "hadoop-common" % "2.8.1",
"org.apache.hadoop" % "hadoop-hdfs" % "2.8.1",
"org.apache.hadoop" % "hadoop-client" % "2.8.1",
"org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.1",

"org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.8.1"
)