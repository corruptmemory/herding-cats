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
import org.apache.zookeeper.{ZooKeeper,Watcher,WatchedEvent,CreateMode,ZooDefs}
import ZooDefs.Ids
import scalaz._
import Scalaz._
import concurrent._


final class Shutdowner(wrapped:Object) {
  def shutdown() {
    wrapped.synchronized {
      wrapped.notifyAll()
    }
  }
}

trait Zookeepers {
  import Zookeepers._

  def withZK(controlPath:String,factory:(String,WatchedEvent => Unit) => ZKReader)(body:(Shutdowner,ZKReader) => Unit) {
    import Watcher.Event.{KeeperState,EventType}
    val syncObject = new Object
    val shutdowner = new Shutdowner(syncObject)
    var zk:ZKReader = null
    def watcher(event:WatchedEvent) {
      event.getState match {
        case KeeperState.SyncConnected => watchControlNode(zk,controlPath)(body(shutdowner,zk))
        case KeeperState.Expired | KeeperState.AuthFailed => shutdowner.shutdown()
        case x@_ => () // Some other condition that we can ignore.  Need logging!
      }
    }
    zk = factory(controlPath,watcher _)
    syncObject.synchronized {
      syncObject.wait()
    }
    zk.withWrapped(_.close)
  }
}

sealed abstract class ZK(wrapped:ZooKeeper) {
  def controlPath:String
  def id:Long = wrapped.getSessionId
  def password:Array[Byte] = wrapped.getSessionPasswd
  def timeout:Int = wrapped.getSessionTimeout
  def withWrapped[T](f:ZooKeeper => T):T = f(wrapped)
}

final class ZKReader(val controlPath:String,wrapped:ZooKeeper) extends ZK(wrapped) {
  def path(p:String):ZKPathReader = new ZKPathReader(p,this)
  def withWriter[T](body:ZKWriter => T):T = body(new ZKWriter(controlPath,wrapped))
}

final class ZKWriter(val controlPath:String,wrapped:ZooKeeper) extends ZK(wrapped) {
  def path(p:String):ZKPathWriter = new ZKPathWriter(p,this)
}

object Zookeepers {
  def watchControlNode(zk:ZKReader,controlPath:String)(cont: => Unit):Unit = {
    val path = zk.path(controlPath)
    path.exists >>= { exists =>
      if (!exists) zk.withWriter(_.path(controlPath).create(Array[Byte]('1'.toByte),toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.EPHEMERAL))
      path.data map (data => data.map(new String(_)).foreach(e => if (e == "1") cont))
    }
  }

  def enableWatches(zk:ZKReader,controlPath:String):Promise[Result[Unit]] =
    zk.withWriter(_.path(controlPath).update(Array[Byte]('1'.toByte),anyVersion))

  def disableWatches(zk:ZKReader,controlPath:String):Promise[Result[Unit]] =
    zk.withWriter(_.path(controlPath).update(Array[Byte]('0'.toByte),anyVersion))
}

object ZK {
  def apply(connectString:String,sessionTimeout:Int)(controlPath:String,watcher:WatchedEvent => Unit):ZKReader = new ZKReader(controlPath,new ZooKeeper(connectString,sessionTimeout,ZKWatcher(watcher)))
}
