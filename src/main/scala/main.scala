/**
 * main.scala
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
import org.apache.zookeeper.{ZooKeeper, Watcher, AsyncCallback,CreateMode}
import org.apache.zookeeper.data.{Id,Stat,ACL}
import AsyncCallback.{ACLCallback, Children2Callback, ChildrenCallback, DataCallback, StatCallback, StringCallback, VoidCallback}

/** Just a silly test used to explore how the continuations plugin works
 */
object Main {

  def foo:Stat = withZK(ZK("",0,null)) {
    zk => reset {
      val path = zk.path("foo")
      val stat = path.stat
      path.delete()
      stat
    }
  }

  def main(args:Array[String]) {
    println(foo)
  }
}