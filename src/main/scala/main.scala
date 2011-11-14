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

  def bar[X](a:String) = shift { k: (Int => X) => k(a.length)}

  def foo(a:String):Unit = reset {
    val b:Int = bar[Unit](a)
    println("result: %d".format(b))
  }

  def main(args:Array[String]) {
    foo("test")
  }
}