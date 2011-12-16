/**
 * zookeeper.scala
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

package com.corruptmemory.herding_cats
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{ZooKeeper,Watcher,WatchedEvent,CreateMode,ZooDefs}
import ZooDefs.Ids
import scalaz._
import Scalaz._
import concurrent._

trait Zookeepers {
  import Zookeepers._
  type ZKState[S,A] = StateT[PromisedResult,S,A]

  def withZK[S](controlPath:String,factory:(String,S,(WatchedEvent,S) => PromisedResult[S]) => ZK,initial:S)(body:ZK => ZKState[S,Unit]) {
    import Watcher.Event.{KeeperState,EventType}
    val syncObject = new Object
    var zk:ZK = null
    def shutdown() {
      syncObject.synchronized {
        syncObject.notifyAll()
      }
    }
    def watcher(event:WatchedEvent,state:S):PromisedResult[S] = {
      event.getState match {
        case KeeperState.SyncConnected =>
          (watchControlNode[S](zk,controlPath)(body) ~> state) map {
            _.fold(failure = { f =>
                     if (f match { case Shutdown => true; case _ => false }) shutdown()
                     f.fail
                   },
                   success = _.success)
          }
        case KeeperState.Expired | KeeperState.AuthFailed => {shutdown(); promise(Shutdown.fail)}
        case x@_ => promise(message(x.toString).fail) // Some other condition that we can ignore.  Need logging!
      }
    }
    zk = factory(controlPath,initial,watcher _)
    syncObject.synchronized {
      syncObject.wait()
    }
    zk.withWrapped(_.close())
  }
}

final class ZK(val controlPath:String, wrapped:ZooKeeper) {
  type ZKWriterOp[S,B] = ZKWriter[S] => ZKState[S,B]
  final class ZKReader[S](conn:ZK) {
    def path(p:String):ZKPathReader[S] = new ZKPathReader[S](p,conn)
  }

  final class ZKWriter[S](conn:ZK) {
    def path(p:String):ZKPathWriter[S] = new ZKPathWriter[S](p,conn)
  }

  private[herding_cats] def withWrapped[T](f:ZooKeeper => T):T = f(wrapped)

  def id:Long = withWrapped(_.getSessionId)
  def password:Array[Byte] = withWrapped(_.getSessionPasswd)
  def timeout:Int = withWrapped(_.getSessionTimeout)

  def reader[S]:ZKReader[S] = new ZKReader[S](this)
  def withWriter[S,B](body:ZKWriterOp[S,B]):ZKState[S,B] = body(new ZKWriter[S](this))

  def getWatchesState[S]:ZKState[S,String] =
    reader.path(controlPath).data[String]()

  def setWatchesState[S](state:String):ZKState[S,Stat] =
    withWriter[S,Stat](_.path(controlPath).update(state,anyVersion))

  def enableWatches[S]:ZKState[S,Stat] = setWatchesState[S]("1")

  def disableWatches[S]:ZKState[S,Stat] = setWatchesState[S]("0")

  def createControlNode[S]:ZKState[S,String] =
    withWriter[S,String](_.path(controlPath).createIfNotExists("1",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.EPHEMERAL))
}

object Zookeepers {
  def watchControlNode[S](zk:ZK,controlPath:String)(body:ZK => ZKState[S,Unit]):ZKState[S,Unit] = {
    val path = zk.reader[S].path(controlPath)
    for {
      _ <- zk.createControlNode
      _ <- path.data[String]() >>= (d => if (d == "1") body(zk) else promiseUnit[S])
    } yield ()
  }
}

object ZK {
  def apply[S](connectString:String,sessionTimeout:Int)(controlPath:String,initial:S,watcher:(WatchedEvent,S) => PromisedResult[S]):ZK =
    new ZK(controlPath,new ZooKeeper(connectString,sessionTimeout,ZKWatcher(initial,watcher)))
}
