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

sealed trait ZKPathBase[T <: ZK] {
  import ZKCallbacks._
  def path:String
  def connection:T
  def stat:Result[Stat] = {
    println("stat")
    val p = emptyPromise[Result[Stat]](Strategy.Sequential)
    println("p")
    val cb = statCallback[Stat](connection,p,(_:Int,_:String,_:Object,stat:Stat) => stat.successNel)
    println("cb")
    connection.withWrapped(_.exists(path,true,cb,this))
    println("connection.withWrapped(_.exists(path,true,cb,this))")
    Thread.sleep(3000)
    println("Slept")
    p.get
  }

  def exists:Boolean = {
    println("exists")
    stat.fold(failure = _ => false,
              success = s => s != null)
  }

  def statAndACL:Result[Tuple2[Stat,Seq[ZKAccessControlEntry]]] = {
    val p = emptyPromise[Result[Tuple2[Stat,Seq[ZKAccessControlEntry]]]](Strategy.Sequential)
    val cb = aclCallback[Tuple2[Stat,Seq[ZKAccessControlEntry]]](connection,p,(_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,toSeqZKAccessControlEntry(jacl)).successNel)
    val statIn = new Stat()
    connection.withWrapped(_.getACL(path,statIn,cb,this))
    p.get
  }

  def children:Result[Seq[String]] = {
    val p = emptyPromise[Result[Seq[String]]](Strategy.Sequential)
    val cb = children2Callback[Seq[String]](connection,p,(_:Int,_:String,_:Object,children:JList[String],_:Stat) => children.toSeq.successNel)
    connection.withWrapped(_.getChildren(path,true,cb,this))
    p.get
  }

  def data:Result[Array[Byte]] = {
    val p = emptyPromise[Result[Array[Byte]]](Strategy.Sequential)
    val cb = dataCallback[Array[Byte]](connection,p,(_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => data.successNel)
    connection.withWrapped(_.getData(path,true,cb,this))
    p.get
  }
}

class ZKPathReader(val path:String,val connection:ZKReader) extends ZKPathBase[ZKReader]

class ZKPathWriter(val path:String,val connection:ZKWriter) extends ZKPathBase[ZKWriter] {
  import ZKCallbacks._
  def create(data:Array[Byte],acl:Seq[ZKAccessControlEntry], createMode:CreateMode):Result[String] = {
    val p = emptyPromise[Result[String]](Strategy.Sequential)
    val cb = createCallback[String](connection,data,acl,createMode,p,(_:Int,_:String,_:Object,name:String) => name.successNel)
    connection.withWrapped(_.create(path,data,acl,createMode,cb,this))
    p.get
  }

  def delete(version:ZKVersion = anyVersion):Result[Unit] = {
    val p = emptyPromise[Result[Unit]](Strategy.Sequential)
    val cb = deleteCallback[Unit](connection,version,p,(_:Int,_:String,_:Object) => ().successNel)
    connection.withWrapped(_.delete(path,version.value,cb,this))
    p.get
  }

  def update(data:Array[Byte],version:ZKVersion):Result[Unit] = {
    val p = emptyPromise[Result[Unit]](Strategy.Sequential)
    val cb = setDataCallback[Unit](connection,data,version,p,(_:Int,_:String,_:Object,_:Stat) => ().successNel)
    connection.withWrapped(_.setData(path,data,version.value,cb,this))
    p.get
  }

  def updateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion):Result[Unit] = {
    val p = emptyPromise[Result[Unit]](Strategy.Sequential)
    val cb = setAclCallback[Unit](connection,acl,version,p,(_:Int,_:String,_:Object,_:Stat) => ().successNel)
    connection.withWrapped(_.setACL(path,acl,version.value,cb,this))
    p.get
  }
}
