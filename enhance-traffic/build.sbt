// Sources
resolvers ++= Seq[Resolver](
  Resolver.mavenLocal,
  s3resolver.value("Linqia maven release", s3("linqia/maven"))
)

// Internal depencies
libraryDependencies ++= Seq(
  "com.linqia" % "formats" % "1.32",
  "com.linqia" % "rules" % "1.46"
)

// Third party depencies
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.6.0" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided",
  "joda-time" % "joda-time" % "2.9.1",
  "org.joda" % "joda-convert" % "1.2" // this optional dependency of joda time is required by scala compiler
)

lazy val root = (project in file(".")).
  settings(
    name := "botlist",
    version := "1.0",
    scalaVersion := "2.10.5"
  )
