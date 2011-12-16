/**
 * elect.scala
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

package com.corruptmemory.herding_cats.recipes
import com.corruptmemory.herding_cats._
import com.corruptmemory.herding_cats._
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{ZooKeeper,Watcher,WatchedEvent,CreateMode,ZooDefs}
import ZooDefs.Ids
import scalaz._
import Scalaz._
import concurrent._

class Elect(val path:String) {
  var myNode:Option[String] = none
  var prevNode:Option[String] = none

  def paticipate(conn:ZK):ZKState[Unit, String] =
    conn.withWriter[Unit,String](_.path(path+"/node").create("1",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.EPHEMERAL_SEQUENTIAL))

  def apply(conn:ZK):ZKState[Unit, Unit] = {
      val pathr = conn.reader[Unit].path(path)
      for {
        _ <- myNode.fold(some = _ => passT[PromisedResult,Unit],
                         none = paticipate(conn) map (str => myNode = some(str)))
      } yield ()
  }
}
