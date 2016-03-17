name := "StormExamples"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	//kafka和storm的slf4j冲突问题,必须屏蔽kafka的log4j
	"org.apache.kafka" %% "kafka" % "0.9.0.1" exclude("org.slf4j", "slf4j-log4j12") exclude("log4j", "log4j"),
	"org.apache.storm" % "storm-core" % "0.10.0",
	"org.apache.storm" % "storm-kafka" % "0.10.0"
)

//避免出现以下包冲突
//package foo contains object and package with same name: supervisor
//one of them needs to be removed from classpath
scalacOptions += "-Yresolve-term-conflict:package"