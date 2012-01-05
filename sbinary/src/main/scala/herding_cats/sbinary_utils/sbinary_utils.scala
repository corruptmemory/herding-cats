/**
 * sbinary_utils.scala
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

package com.corruptmemory.herding_cats.sbinary_utils

object SBinaryUtils {
  import com.corruptmemory.herding_cats._
  import sbinary._
  import Operations._
  import scalaz._
  import Scalaz._

  implicit def sbinarySerializer[T : Writes : Reads]:ZKSerialize[T] = new ZKSerialize[T] {
    def write(x:T):Array[Byte] = toByteArray[T](x)
    def read(x:Array[Byte]):Validation[Error,T] = try {
      fromByteArray(x).success
    } catch {
      case t:Throwable => uncaught(t).fail
    }
  }
}
