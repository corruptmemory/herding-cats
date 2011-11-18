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
  def stat[T] = shift { k:(Result[Stat] => T) =>
    val cb = statCallback[Stat,T](connection,k,(_:Int,_:String,_:Object,stat:Stat) => stat.successNel)
    connection.withWrapped(_.exists(path,true,cb,this))
  }
  def exists[T] = {
    val s = stat[T]
    s.fold(failure = _ => false,
           success = s => s != null)
  }

  def statAndACL[T] = shift { k:(Result[Tuple2[Stat,Seq[ZKAccessControlEntry]]] => T) =>
    val cb = aclCallback[Tuple2[Stat,Seq[ZKAccessControlEntry]],T](connection,k,(_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,toSeqZKAccessControlEntry(jacl)).successNel)
    val statIn = new Stat()
    connection.withWrapped(_.getACL(path,statIn,cb,this))
  }

  def children[T] = shift { k:(Result[Seq[String]] => T) =>
    val cb = children2Callback[Seq[String],T](connection,k,(_:Int,_:String,_:Object,children:JList[String],_:Stat) => children.toSeq.successNel)
    connection.withWrapped(_.getChildren(path,true,cb,this))
  }

  def data[T] = shift { k:(Result[Array[Byte]] => T) =>
    val cb = dataCallback[Array[Byte],T](connection,k,(_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => data.successNel)
    connection.withWrapped(_.getData(path,true,cb,this))
  }

  // Use Async call to avoid double invocation of watcher
  def create[T](data:Array[Byte],acl:Seq[ZKAccessControlEntry], createMode:CreateMode) = shift { k:(Result[String] => T) =>
    val cb = createCallback[String,T](connection,data,acl,createMode,k,(_:Int,_:String,_:Object,name:String) => name.successNel)
    connection.withWrapped(_.create(path,data,acl,createMode,cb,this))
  }
  def delete(version:ZKVersion = anyVersion):Unit = connection.withWrapped(_.delete(path,version.value))
  def update(data:Array[Byte],version:ZKVersion):Unit = connection.withWrapped(_.setData(path,data,version.value))
  def updateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion):Unit = connection.withWrapped(_.setACL(path,acl,version.value))
}
