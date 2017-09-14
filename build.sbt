scalaVersion := "2.11.8" 

organization := "io.github.xnukernpoll"

name := "peer_services"

lazy val topologies = (project in file("topologies") )

lazy val store_util = (project in file("store_lib") )

lazy val peer_services = (project in file(".") )
  .dependsOn(topologies)
  .dependsOn(store_util)

