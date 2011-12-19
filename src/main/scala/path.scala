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
import org.apache.zookeeper.{ZooKeeper, Watcher, AsyncCallback,CreateMode}
import org.apache.zookeeper.data.{Id,Stat,ACL}
import AsyncCallback.{ACLCallback, Children2Callback, ChildrenCallback, DataCallback, StatCallback, StringCallback, VoidCallback}
import java.util.{List=>JList}
import scala.collection.JavaConversions._
import scalaz._
import scalaz.concurrent._
import Scalaz._

sealed trait ZKPathBase[S] {
  type ZKState1[A] = ZKState[S,A]

  import ZKCallbacks._
  def path:String
  def connection:ZK

  def makePromise[A](f:PromisedResult[A] => Unit):ZKState1[A] = {
    val p = emptyPromise[Result[A]](Strategy.Sequential)
    f(p)
    stateT[PromisedResult,S,A](s => p map (r => r map (a => (s,a))))
  }

  def stat(watch:Boolean = true):ZKState1[Stat] =
    makePromise[Stat] { p =>
      val cb = statCallback(connection,watch,p,(_:Int,_:String,_:Object,stat:Stat) => stat.success)
      connection.withWrapped(_.exists(path,watch,cb,this))
    }

  def exists(watch:Boolean = true)(a: => ZKState1[Unit])(b: => ZKState1[Unit]):ZKState1[Unit] = {
    val stat = connection.withWrapped(_.exists(path,watch))
    if (stat != null) a
    else b
  }

  def watch:ZKState1[Unit] = exists() (passT[PromisedResult,S]) (passT[PromisedResult,S])

  def statAndACL:ZKState1[Tuple2[Stat,Seq[ZKAccessControlEntry]]] =
    makePromise[Tuple2[Stat,Seq[ZKAccessControlEntry]]] { p =>
      val cb = aclCallback(connection,p,(_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,toSeqZKAccessControlEntry(jacl)).success)
      val statIn = new Stat()
      connection.withWrapped(_.getACL(path,statIn,cb,this))
    }

  def children(watch:Boolean = true):ZKState1[Traversable[String]] =
    makePromise[Traversable[String]] { p =>
      val cb = children2Callback(connection,watch,p,(_:Int,_:String,_:Object,children:JList[String],_:Stat) => children.toSeq.success)
      connection.withWrapped(_.getChildren(path,watch,cb,this))
    }

  def data[T : ZKSerialize](watch:Boolean = true):ZKState1[T] =
    makePromise[T] { p =>
      import ZKSerialize._
      val cb = dataCallback[T](connection,watch,p,(_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => read[T](data))
      connection.withWrapped(_.getData(path,watch,cb,this))
    }
}

class ZKPathReader[S](val path:String,val connection:ZK) extends ZKPathBase[S]

class ZKPathWriter[S](val path:String,val connection:ZK) extends ZKPathBase[S] {
  import ZKCallbacks._
  def create[T : ZKSerialize](data:T,acl:Seq[ZKAccessControlEntry], createMode:CreateMode):ZKState1[String] =
    makePromise[String] { p =>
      import ZKSerialize._
      val arrayData = write[T](data)
      val cb = createCallback(connection,arrayData,acl,createMode,p,(_:Int,_:String,_:Object,name:String) => name.success)
      connection.withWrapped(_.create(path,arrayData,acl,createMode,cb,this))
    }

  def createIfNotExists[T : ZKSerialize](data:T,acl:Seq[ZKAccessControlEntry], createMode:CreateMode):ZKState1[String] =
    stateT[PromisedResult,S,String](s => (create(data,acl,createMode) apply s) map (_.fold(failure = _ match {
                                                                                                       case NodeExists => (s,path).success
                                                                                                       case f@_ => f.fail
                                                                                                     },
                                                                                           success = s1 => s1.success)))

  def delete(version:ZKVersion = anyVersion):ZKState1[Unit] =
    makePromise[Unit] { p =>
      val cb = deleteCallback(connection,version,p,(_:Int,_:String,_:Object) => ().success)
      connection.withWrapped(_.delete(path,version.value,cb,this))
    }

  def update[T : ZKSerialize](data:T,version:ZKVersion):ZKState1[Stat] =
    makePromise[Stat] { p =>
      import ZKSerialize._
      val arrayData = write[T](data)
      val cb = setDataCallback(connection,arrayData,version,p,(_:Int,_:String,_:Object,stat:Stat) => stat.success)
      connection.withWrapped(_.setData(path,arrayData,version.value,cb,this))
    }

  def updateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion):ZKState1[Stat] =
    makePromise[Stat] { p =>
      val cb = setAclCallback(connection,acl,version,p,(_:Int,_:String,_:Object,stat:Stat) => stat.success)
      connection.withWrapped(_.setACL(path,acl,version.value,cb,this))
    }
}
