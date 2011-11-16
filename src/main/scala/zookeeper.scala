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
import scala.util.continuations._
import org.apache.zookeeper.{ZooKeeper,Watcher,WatchedEvent}
import scalaz._
import Scalaz._

trait Zookeepers {
  def withZK[T <: AnyRef](factory:(WatchedEvent => Unit) => ZK)(body:ZK => T):T = {
    var result:T = null
    var zk:ZK = null
    def watcher(event:WatchedEvent) {
      result = body(zk)
      result.notifyAll()
    }
    zk = factory(watcher _)
    result.synchronized {
      result.wait()
    }
    zk.withWrapped(_.close)
    result
  }
  def loopWithZK(factory:(WatchedEvent => Unit) => ZK)(body:ZK => Unit):Unit = {
    var zk:ZK = null
    def watcher(event:WatchedEvent) {
      body(zk)
    }
    zk = factory(watcher _)
    Thread.sleep(1000*10)
    zk.withWrapped(_.close)
  }
}

class ZK(wrapped:ZooKeeper) {
  def id:Long = wrapped.getSessionId
  def password:Array[Byte] = wrapped.getSessionPasswd
  def timeout:Int = wrapped.getSessionTimeout
  def withWrapped[T](f:ZooKeeper => T):T = f(wrapped)
  def path(p:String):ZKPath = new ZKPath(p,this)
}

object ZK {
  def apply(connectString:String,sessionTimeout:Int)(watcher:WatchedEvent => Unit):ZK = new ZK(new ZooKeeper(connectString,sessionTimeout,ZKWatcher(watcher)))
}
