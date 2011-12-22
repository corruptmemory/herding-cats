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
import org.apache.zookeeper.{CreateMode,ZooDefs}
import ZooDefs.Ids
import scalaz._
import Scalaz._
import concurrent._

case class Elect(node:Option[String], prevNode:Option[String], elected:Boolean)

object Elect {
  def participate(path:String,conn:ZK):ZKState[Elect, Unit] =
    conn.withWriter[Elect,Unit] { writer =>
      for {
        es <- initT
        myNode <- writer.path(path+"/node").create("1",toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE),CreateMode.EPHEMERAL_SEQUENTIAL)
        _ <- writer.path(myNode).watch
        children <- writer.path(path).children(false)
        _ <- putT[PromisedResult,Elect] {
          val prev = for {
            z <- children.map(c => path+"/"+c).toSeq.sorted.toList.toZipper
            z <- z.findNext(_ == myNode)
            z <- z.previous
          } yield z.focus
          es.copy(node = some(myNode), prevNode = prev, elected = prev.fold(some = _ => false, none = true))
        }
      } yield ()
    }

  def apply(path:String)(conn:ZK)(onElected: => Unit):ZKState[Elect, Unit] = {
      val reader = conn.reader[Elect]
      for {
        es <- initT
        _ <- es.node.fold(some = s => reader.path(s).exists() (passT[PromisedResult,Elect]) (participate(path,conn)),
                          none = participate(path,conn))
        es <- initT
        _ <- es.prevNode.fold(none = passT[PromisedResult,Elect], // not quite
                              some = s => reader.path(s).exists() (passT[PromisedResult,Elect]) (putT[PromisedResult,Elect](es.copy(prevNode=none,elected=true))))
        es <- initT
      } yield if (es.elected) onElected
              else ()
  }
}
