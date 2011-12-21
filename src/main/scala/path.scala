/** path.scala
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

/** Base class for path-based operations
  *
  * @tparam S The type of ''state'' to operate over
  */
sealed trait ZKPathBase[S] {

  /** Type synonym, duh.
    */
  type ZKState1[A] = ZKState[S,A]

  import ZKCallbacks._
  /** The current path
    */
  def path:String

  /** The current connection
    */
  def connection:ZK

  /** Helper function to wrap the creation of a completable Promise
    *
    * @return `ZKState[S,A]`
    */
  def makePromise[A](f:PromisedResult[A] => Unit):ZKState1[A] = {
    val p = emptyPromise[Result[A]](Strategy.Sequential)
    f(p)
    stateT[PromisedResult,S,A](s => p map (r => r map (a => (s,a))))
  }

  /** Get the stat of a node.
    *
    * @param watch used to indicate if the node on this patch should be watched
    * @return `ZKState[S,Stat]`
    */
  def stat(watch:Boolean = true):ZKState1[Stat] = {
    val stat = connection.withWrapped(_.exists(path,watch))
    stateT[PromisedResult,S,Stat](s => promise(if (stat != null) (s,stat).success
                                               else noNode(path).fail)(Strategy.Sequential))
  }

  /** A combinator over node existence
    *
    * @param watch used to indicate if the node on this patch should be watched.  You know, like a hawk.
    * @param a the "true" case
    * @param b the "false" case
    * @return `ZKState[S,Unit]`
    */
  def exists(watch:Boolean = true)(a: => ZKState1[Unit])(b: => ZKState1[Unit]):ZKState1[Unit] = {
    val stat = connection.withWrapped(_.exists(path,watch))
    if (stat != null) a
    else b
  }

  /** Watch the current node
    *
    * @return `ZKState[S,Unit]`
    */
  def watch:ZKState1[Unit] = exists() (passT[PromisedResult,S]) (stateT[PromisedResult,S,Unit](s => promise(noNode(path).fail)))

  def statAndACL:ZKState1[Tuple2[Stat,Seq[ZKAccessControlEntry]]] =
    makePromise[Tuple2[Stat,Seq[ZKAccessControlEntry]]] { p =>
      val cb = aclCallback(connection,p,(_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,toSeqZKAccessControlEntry(jacl)).success)
      val statIn = new Stat()
      connection.withWrapped(_.getACL(path,statIn,cb,this))
    }

  /** A Get the children of the current node
    *
    * @param watch used to indicate if the node on this patch should be watched.  You know, like a hawk.
    * @return `ZKState[S,Traversable[String]]`
    */
  def children(watch:Boolean = true):ZKState1[Traversable[String]] =
    makePromise[Traversable[String]] { p =>
      val cb = children2Callback(connection,watch,p,(_:Int,_:String,_:Object,children:JList[String],_:Stat) => children.toSeq.success)
      connection.withWrapped(_.getChildren(path,watch,cb,this))
    }

  /** A Get the data for the current node
    *
    * @tparam T The type of data to retrieve.  There is a context bounds of `ZKSerialize` on this type
    *   so an instance of `ZKSerialize` for the desired type has to be visible in implicit scope.
    * @param watch used to indicate if the node on this patch should be watched.  You know, like a hawk.
    * @return `ZKState[S,T]`
    */
  def data[T : ZKSerialize](watch:Boolean = true):ZKState1[T] =
    makePromise[T] { p =>
      import ZKSerialize._
      val cb = dataCallback[T](connection,watch,p,(_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => read[T](data))
      connection.withWrapped(_.getData(path,watch,cb,this))
    }
}

/** An actual reader thingy.
  *
  * @tparam S The type of ''state'' to operate over
  */
class ZKPathReader[S](val path:String,val connection:ZK) extends ZKPathBase[S]


/** A writer thingy.
  *
  * @tparam S The type of ''state'' to operate over
  */
class ZKPathWriter[S](val path:String,val connection:ZK) extends ZKPathBase[S] {
  import ZKCallbacks._
  /** Create a new node at the current path
    *
    * @tparam T The type of data to retrieve.  There is a context bounds of `ZKSerialize` on this type
    *   so an instance of `ZKSerialize` for the desired type has to be visible in implicit scope.
    * @param data The data to be written (serialized) into the node.  Of type T
    * @param acl A `Seq` of `ZKAccessControlEntry` values.  Defines the access control l
    * @return `ZKState[S,String]`
    */
  def create[T : ZKSerialize](data:T,acl:Seq[ZKAccessControlEntry], createMode:CreateMode):ZKState1[String] =
    makePromise[String] { p =>
      import ZKSerialize._
      val arrayData = write[T](data)
      val cb = createCallback(connection,arrayData,acl,createMode,p,(_:Int,_:String,_:Object,name:String) => name.success)
      connection.withWrapped(_.create(path,arrayData,acl,createMode,cb,this))
    }

  def createIfNotExists[T : ZKSerialize](data:T,acl:Seq[ZKAccessControlEntry], createMode:CreateMode):ZKState1[String] =
    stateT[PromisedResult,S,String](s => (create(data,acl,createMode) apply s) map (_.fold(failure = _ match {
                                                                                                       case NodeExists(_) => (s,path).success
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
