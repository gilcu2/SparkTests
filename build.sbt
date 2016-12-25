name := "simpleTest"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.2" % "provided"

//libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.5.2" exclude("org.spark-project.spark", "unused")
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0" exclude("org.spark-project.spark", "unused")

mainClass in assembly := Some("SimpleSpark")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x =>
    val baseStrategy = (assemblyMergeStrategy in assembly).value
    baseStrategy(x)
}