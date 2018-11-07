// sbt.version=1.2.6
val ordermgtm = project in file(".")

organization in ThisBuild := "com.dwijnand"
     version in ThisBuild := "0.1.0-SNAPSHOT"
scalaVersion in ThisBuild := "2.12.7"

libraryDependencies += "com.typesafe.akka" %% "akka-actor"             % "2.5.17"
libraryDependencies += "com.typesafe.akka" %% "akka-persistence"       % "2.5.17"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed"       % "2.5.17"
libraryDependencies += "com.typesafe.akka" %% "akka-persistence-typed" % "2.5.17"

libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"

fork in run := true // for leveldb

cancelable in Global := true
