import sbtsparkpackage.SparkPackagePlugin.autoImport.sparkVersion

ghpages.settings

git.remoteRepo := "git@github.com:hindog/spark-mergejoin.git"

enablePlugins(SiteScaladocPlugin)

lazy val root = Project(
	id = "root",
	base = file(".")
).settings(
	Seq(
		organization := "com.hindog.spark",
		moduleName := "spark-mergejoin",
		name := "spark-mergejoin",
		sparkVersion := "1.5.2",
		scalaVersion := "2.11.8",
		crossScalaVersions := Seq("2.10.6", "2.11.8"),
		spIncludeMaven := true,
		spIgnoreProvided := true, // override spark-packages dependency management
		spAppendScalaVersion := true,
		spName := "hindog/spark-mergejoin",
		credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials"),
		releaseCrossBuild := true,
		releaseVersionBump := sbtrelease.Version.Bump.Bugfix,
		releasePublishArtifactsAction := PgpKeys.publishSigned.value,
		javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.7"),
		scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.7", "-feature", "-language:_"),
		testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
		libraryDependencies ++= Seq(
			"org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
			"org.scalatest" %% "scalatest" % "2.2.6" % "test",
			"org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests"
		),
		autoAPIMappings := true,
		publishMavenStyle := true,
		pomIncludeRepository := { _ => false },
		parallelExecution in Test := false,
		publishArtifact in Test := false,
		publishTo := {
			val nexus = "https://oss.sonatype.org/"
			if (isSnapshot.value)
				Some("snapshots" at nexus + "content/repositories/snapshots")
			else
				Some("releases"  at nexus + "service/local/staging/deploy/maven2")
		},
		pomExtra := (
			<url>https://github.com/hindog/spark-mergejoin</url>
				<licenses>
					<license>
						<name>Apache 2</name>
						<url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
						<distribution>repo</distribution>
						<comments>A business-friendly OSS license</comments>
					</license>
				</licenses>
				<scm>
					<url>git@github.com:hindog/spark-mergejoin.git</url>
					<connection>scm:git:git@github.com:hindog/spark-mergejoin.git</connection>
				</scm>
				<developers>
					<developer>
						<id>hindog</id>
						<name>Aaron Hiniker</name>
						<url>https://github.com/hindog/</url>
					</developer>
				</developers>
			)
	)
)
