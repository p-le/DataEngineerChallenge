ThisBuild / scalaVersion := "2.12.13"
ThisBuild / organization := "phu.le.dev"

val  sparkVersion = "3.1.1"

lazy val root = (project in file("."))
	.settings(
    	name := "analyze-job",
		libraryDependencies ++= Seq(
			"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
			"org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
			"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
			"org.scalatest" %% "scalatest" % "3.2.7" % Test,
		)
	)
