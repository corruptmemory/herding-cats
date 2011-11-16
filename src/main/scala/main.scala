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

/** Just a silly test used to explore how the continuations plugin works
 */
object Main {

  def foo:Unit = loopWithZK(ZK("127.0.0.1:2181",5000)) {
    zk => reset {
      val path = zk.path("/foo")
      val data = path.data[Unit]
      println("Data: %s".format(data))
      // println("Stat: %s".format(new String(data)))
    }
  }

  def main(args:Array[String]) {
    foo
  }
}