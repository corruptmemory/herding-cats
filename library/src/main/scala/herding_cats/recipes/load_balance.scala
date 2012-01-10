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

package com.corruptmemory.herding_cats.recipes
import com.corruptmemory.herding_cats._
import scala.collection.mutable.Builder

/** Simple load balancer */
object SimpleLoadBalance {
  import scalaz._
  import scalaz.concurrent._
  import Scalaz._
  def partitionN[T](n:Int,items:Traversable[T]):Seq[Traversable[T]] = {
    def pN(result:Builder[Traversable[T], Vector[Traversable[T]]],remainder:Traversable[T]):Seq[Traversable[T]] = {
      if (remainder.isEmpty) result.result()
      else pN(result += remainder.take(n),remainder.drop(n))
    }
    pN(Vector.newBuilder[Traversable[T]],items)
  }

  /** Load balance a set of resources to a set of clients
   *
   *  Balancing algorithm is extremely simple and naive: simply distribute
   *  the available clients evenly across the resources
   *
   *  @param resourcesPath The path of the parent node where resource nodes will be listed
   *  @param clientsPath The path of the parent node where client nodes will be listed
   *  @param conn The connection to the `ZooKeeper` cluster
   *  @return `ZKState[Unit, Unit]`
   */
  def apply(resourcesPath:String,clientsPath:String)(conn:ZK):ZKState[Unit, Unit] = {
    val reader = conn.reader[Unit]
    val resourcesNode = reader.path(resourcesPath)
    val clientsNode = reader.path(clientsPath)
    for {
      resources <- resourcesNode.children()
      clients <- clientsNode.children()
      _ <- conn.withWriter[Unit,Unit] {
        writer => {
          val rc = resources.size
          val cc = clients.size
          if ((rc > 0) && (cc > 0)) {
            val r = cc/rc
            resources.toSeq.zip(partitionN(if (r == 0) 1 else r,clients)).foreach {
              case (resource,assignedClients) => assignedClients.foreach(p => writer.path(clientsPath+"/"+p).update[String](resourcesPath+"/"+resource))
            }
            promiseUnit[Unit]
          } else if (rc == 0) errorState[Unit,Unit](message("No resources at: %s".format(resourcesPath)))
          else errorState[Unit,Unit](message("No clients at: %s".format(clientsPath)))
        }
      }
    } yield ()
  }
}
