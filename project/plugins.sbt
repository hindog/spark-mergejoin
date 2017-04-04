scalaVersion := "2.10.5"

resolvers += "bintray-plugin-repo" at "https://dl.bintray.com/sbt/sbt-plugin-releases/"
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.1.0")
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.5.4")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")
addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")