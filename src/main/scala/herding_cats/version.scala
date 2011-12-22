/**
 * version.scala
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

sealed abstract class ZKVersion {
  def value:Int
}
case object AnyVersion extends ZKVersion {
  val value:Int = -1
}
case class Version(value:Int) extends ZKVersion

trait ZKVersions {
  import ZKVersion._
  def anyVersion:ZKVersion = AnyVersion
  def version(i:Int):ZKVersion = toVersion(i)
}

object ZKVersion {
  implicit def toVersion(i:Int):ZKVersion =
   if (i < 0) AnyVersion
   else Version(i)
}