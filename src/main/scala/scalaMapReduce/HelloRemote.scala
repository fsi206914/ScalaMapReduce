package scalaMapReduce

import akka.actor._
import common._

object HelloRemote extends App  {
  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
  }

  val system = ActorSystem("HelloRemoteSystem", Config.JobTrackerConfig)
  val remoteActor = system.actorOf(Props[RemoteActor], name = "RemoteActor")
  printActor(remoteActor)
  remoteActor ! Message("The RemoteActor is alive")
}


class RemoteActor extends Actor {
  def receive = {
    case Message(msg) =>
        println(s"RemoteActor received message '$msg'")
        val s = sender
        printActor(s)
    case st: String =>
        println("RemoteActor received message " + st)
        val s = sender
        printActor(s)
    case _ => 
        println("RemoteActor got something unexpected.")
  }

  def printActor(s: ActorRef){
    println(s.path.toString)
    println(s.path.address.toString)
    println(s.path.address.host)
    println(s.path.address.port)
  }


}


