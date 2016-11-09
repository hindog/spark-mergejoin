scalaVersion := "2.10.5"

resolvers += "bintray-plugin-repo" at "https://dl.bintray.com/sbt/sbt-plugin-releases/"

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.1.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.4")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")