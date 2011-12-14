/**
 * serialization.scala
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
import scalaz._
import Scalaz._

trait ZKSerialize[T] {
  def write(x:T):Array[Byte]
  def read(x:Array[Byte]):ValidationNEL[Error,T]
}

object ZKSerialize {
  def write[T](x:T)(implicit s:ZKSerialize[T]):Array[Byte] = s.write(x)
  def read[T](x:Array[Byte])(implicit s:ZKSerialize[T]):ValidationNEL[Error,T] = s.read(x)
}

trait ZKSerializers {
  implicit def byteArraySerializer:ZKSerialize[Array[Byte]] = new ZKSerialize[Array[Byte]] {
    def write(x:Array[Byte]):Array[Byte] = x
    def read(x:Array[Byte]):ValidationNEL[Error,Array[Byte]] = x.successNel
  }
}
