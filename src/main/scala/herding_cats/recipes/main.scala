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

package com.corruptmemory.herding_cats.recipes
import com.corruptmemory.herding_cats._
import org.apache.zookeeper.{CreateMode,ZooDefs}
import ZooDefs.Ids
import scalaz._
import Scalaz._
import concurrent._
import scala.actors.Futures._

object ElectMain {
  import scalaz._
  import Scalaz._

  def election(i:Int):Unit = withZK[Participant]("/test/election%d".format(i),ZK("127.0.0.1:2181",5000),Participant(none,none,false)) {
    (zk:ZK) =>
      Participant("/elect_test")(zk){println("won election %d".format(i))}
  }

  def main(args:Array[String]) {
    val syncObject = new Object
    future(election(1))
    future(election(2))
    future(election(3))
    future(election(4))
    syncObject.synchronized {
      syncObject.wait()
    }
  }
}
