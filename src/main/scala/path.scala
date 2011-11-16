/**
 * path.scala
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
import org.apache.zookeeper.{ZooKeeper, Watcher, AsyncCallback,CreateMode}
import org.apache.zookeeper.data.{Id,Stat,ACL}
import AsyncCallback.{ACLCallback, Children2Callback, ChildrenCallback, DataCallback, StatCallback, StringCallback, VoidCallback}
import scalaz._
import Scalaz._
import java.util.{List=>JList}
import scala.collection.JavaConversions._

class ZKPath(val path:String,val connection:ZK) {
  import ZKCallbacks._
  def stat[T] = shift { k:(Stat => T) =>
    val cb = statCallback((_:Int,_:String,_:Object,stat:Stat) => stat)
    connection.withWrapped(_.exists(path,true,cb,this))
    k(cb.result)
  }
  def exists = stat != null

  def statAndACL[T] = shift { k:(Tuple2[Stat,Seq[ZKAccessControlEntry]] => T) =>
    val cb = aclCallback[Tuple2[Stat,Seq[ZKAccessControlEntry]]]((_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,jacl.map(a => ZKAccessControlEntry(a.getId,a.getPerms))))
    val statIn = new Stat()
    connection.withWrapped(_.getACL(path,statIn,cb,this))
    k(cb.result)
  }
  def children[T] = shift { k:(Seq[String] => T) =>
    val cb = children2Callback[Seq[String]]((_:Int,_:String,_:Object,children:JList[String],_:Stat) => children)
    connection.withWrapped(_.getChildren(path,true,cb,this))
    k(cb.result)
  }
  def data[T] = shift { k:(Array[Byte] => T) =>
    val cb = dataCallback[Array[Byte]]((_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => data)
    connection.withWrapped(_.getData(path,true,cb,this))
    k(cb.result)
  }
  def create(data:Array[Byte],acl:Seq[ZKAccessControlEntry], createMode:CreateMode):String = connection.withWrapped(_.create(path,data,acl,createMode))
  def delete(version:ZKVersion = anyVersion):Unit = connection.withWrapped(_.delete(path,version.value))
  def update(data:Array[Byte],version:ZKVersion):Unit = connection.withWrapped(_.setData(path,data,version.value))
  def updateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion):Unit = connection.withWrapped(_.setACL(path,acl,version.value))
}
