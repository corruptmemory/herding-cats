/**
 * watcher.scala
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

import org.apache.zookeeper.{ZooKeeper, Watcher, AsyncCallback, WatchedEvent}
import AsyncCallback.{ACLCallback, Children2Callback, ChildrenCallback, DataCallback, StatCallback, StringCallback, VoidCallback}
import scalaz._
import Scalaz._

object ZKWatcher {
  class WatcherCallbackW[T](responder:WatchedEvent => T) extends Watcher {
    var result:T = _
    def process(event:WatchedEvent):Unit = {
      result = responder(event)
    }
  }

  def apply(block:WatchedEvent => Unit):Watcher = new Watcher {
    def process(event:WatchedEvent):Unit = block(event)
  }

  def watcherCallback[T](responder:WatchedEvent => T):WatcherCallbackW[T] = new WatcherCallbackW[T](responder)
}
