/**
 * error.scala
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

trait Error
case class Message(message:String) extends Error
case class Caught(message:String,throwable:Throwable) extends Error
case class Uncaught(throwable:Throwable) extends Error
case object Disconnected extends Error
case class NoNode(path:String) extends Error
case class NodeExists(path:String) extends Error
case object VersionMismatch extends Error
case object Shutdown extends Error

trait Errors {
  def message(m:String):Error = Message(m)
  def caught(m:String,t:Throwable):Error = Caught(m,t)
  def uncaught(t:Throwable):Error = Uncaught(t)
  def disconnected:Error = Disconnected
  def noNode(path:String):Error = NoNode(path)
  def nodeExists(path:String):Error = NodeExists(path)
  def versionMismatch:Error = VersionMismatch
  def shutdown:Error = Shutdown
}
