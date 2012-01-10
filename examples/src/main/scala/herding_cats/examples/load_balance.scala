/**
 * load_balance.scala
 *
 * @author <a href="mailto:jim@corruptmemory.com">Jim Powers</a>
 *
 * Copyright 2011 Jim Powers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.corruptmemory.herding_cats.examples

object SimpleLoadBalanceExample {
  import com.corruptmemory.herding_cats._
  import com.corruptmemory.herding_cats.recipes._
  import scalaz._
  import Scalaz._
  import concurrent._
  import scala.actors.Future
  import scala.actors.Futures._
  import org.apache.zookeeper.{CreateMode,ZooDefs}
  import ZooDefs.Ids

  sealed trait LBMessages
  case object Balance extends LBMessages

  type LoadBalancerActor = Actor[LBMessages]

  def genActor(resourcesPath:String,clientsPath:String)(factory:ZKConnFactory[Unit]):LoadBalancerActor = actor[LBMessages] {
    case Balance => {
      val lb = SimpleLoadBalance(resourcesPath,clientsPath)_
      withZK[Unit]("/test/simple_load_balancer",factory,())(lb(_))
    }
  }

  def registerResourceRoot[S](path:String)(conn:ZK):ZKState[S,String] =
    conn.withWriter[S,String](_.path(path).createIfNotExists("",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.PERSISTENT))

  def registerResource[S](root:String)(conn:ZK):ZKState[S,String] =
    conn.withWriter[S,String](_.path(root+"/resource").create("",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.EPHEMERAL_SEQUENTIAL))

  def registerClientRoot[S](path:String)(conn:ZK):ZKState[S,String] =
    conn.withWriter[S,String](_.path(path).createIfNotExists("",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.PERSISTENT))

  def registerClient(root:String)(conn:ZK):ZKState[Option[String],String] = for {
    path <- initT
    path <- path.fold(some = s => promiseResult[Option[String],String](s),
                      none = conn.withWriter[Option[String],String](_.path(root+"/client").create("",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.EPHEMERAL_SEQUENTIAL)) >>=
                               {p => mkZKState[Option[String],String](some(p),p)})
  } yield path

  def election(i:Int, factory:ZKConnFactory[Participant], lba:LoadBalancerActor):Unit = withZK[Participant]("/test/election%d".format(i),factory,Participant(none,none,false)) {
    (zk:ZK) =>
      Participant("/elect_test")(zk){
        println("won election %d".format(i))
        lba ! Balance
      }
  }

  def initPaths(conn:ZK,resourcesPath:String,clientsPath:String):ZKState[Unit,Unit] = for {
    _ <- registerResourceRoot[Unit](resourcesPath)(conn)
    _ <- registerClientRoot[Unit](clientsPath)(conn)
    _ <- errorState[Unit,Unit](shutdown)
  } yield ()

  def client(conn:ZK,clientsPath:String):ZKState[Option[String],Unit] = for {
    path <- registerClient(clientsPath)(conn)
    val reader = conn.reader[Option[String]].path(path)
    resource <- reader.data[String]()
  } yield if (resource.isEmpty) println("No resource assigned to %s".format(path))
          else println("%s assigned to %s".format(path,resource))

  def resource(conn:ZK,resourcesPath:String):ZKState[Unit,Unit] = for {
    path <- registerResource(resourcesPath)(conn)
  } yield ()

  def createResource(id:Int,resourcesPath:String,factory:ZKConnFactory[Unit]):Future[Unit] =
    future(withZK[Unit]("/test/resource_%d".format(id),factory,())(c => resource(c,resourcesPath)))

  def createClient(id:Int,clientsPath:String,factory:ZKConnFactory[Option[String]]):Future[Unit] =
    future(withZK[Option[String]]("/test/client_%d".format(id),factory,None)(c => client(c,clientsPath)))

  def factory[S]():ZKConnFactory[S] = ZK[S]("127.0.0.1:2181",5000)_

  def createParticipant(id:Int,resourcesPath:String,clientsPath:String):Future[Unit] =
    future(election(1,factory[Participant](),genActor(resourcesPath,clientsPath)(factory[Unit]())))

  def main(args:Array[String]) {
    val syncObject = new Object
    val resources = "/resources"
    val clients = "/clients"
    withZK[Unit]("/init_paths",factory[Unit](),())(c => initPaths(c,resources,clients))
    createResource(1,resources,factory[Unit]())
    createResource(2,resources,factory[Unit]())
    createResource(3,resources,factory[Unit]())
    createClient(1,clients,factory[Option[String]]())
    createClient(2,clients,factory[Option[String]]())
    createClient(3,clients,factory[Option[String]]())
    createClient(4,clients,factory[Option[String]]())
    createClient(5,clients,factory[Option[String]]())
    createClient(6,clients,factory[Option[String]]())
    createParticipant(1,resources,clients)
    createParticipant(2,resources,clients)
    createParticipant(3,resources,clients)
    createParticipant(4,resources,clients)
    syncObject.synchronized {
      syncObject.wait()
    }
  }
}