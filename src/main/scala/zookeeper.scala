/**
 * zookeeper.scala
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

case class ZKAccessControlEntry(id:Id,perms:Int)

class ZK(val wrapped:ZooKeeper) {
  def sessionId:Long = sys.error("undefined")
  def sessionPasswd:Array[Byte] = sys.error("undefined")
  def sessionTimeout:Int = sys.error("undefined")
}

abstract class ZKAction {
  def apply(path:ZKPath):Unit
}

object ZKAction {
  case class Create(data:Array[Byte],acl:Seq[ZKAccessControlEntry], createMode:CreateMode) extends ZKAction {
    def apply(path:ZKPath):Unit = sys.error("undefined")
  }
  case class Delete(version:ZKVersion) extends ZKAction {
    def apply(path:ZKPath):Unit = sys.error("undefined")
  }
  case class Update(data:Array[Byte],version:ZKVersion) extends ZKAction {
    def apply(path:ZKPath):Unit = sys.error("undefined")
  }
  case class UpdateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion) extends ZKAction {
    def apply(path:ZKPath):Unit = sys.error("undefined")
  }

  def create(data:Array[Byte],acl:Seq[ZKAccessControlEntry], createMode:CreateMode):ZKAction = Create(data,acl,createMode)
  def delete(version:ZKVersion):ZKAction = Delete(version)
  def update(data:Array[Byte],version:ZKVersion):ZKAction = Update(data,version)
  def updateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion):ZKAction = UpdateACL(acl,version)
}

class ZKPath(val path:String,val connection:ZK) {
  import ZKCallbacks._
  def stat = shift { k:(Stat => Stat) =>
    val cb = statCallback((_:Int,_:String,_:Object,stat:Stat) => stat)
    connection.wrapped.exists(path,true,cb,this)
    k(cb.result)
  }
  def exists = stat != null

  def statAndACL = shift { k:(Tuple2[Stat,Seq[ZKAccessControlEntry]] => Tuple2[Stat,Seq[ZKAccessControlEntry]]) =>
    val cb = aclCallback[Tuple2[Stat,Seq[ZKAccessControlEntry]]]((_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,jacl.map(a => ZKAccessControlEntry(a.getId,a.getPerms))))
    val statIn = new Stat()
    connection.wrapped.getACL(path,statIn,cb,this)
    k(cb.result)
  }
  def children = shift { k:(Seq[String] => Seq[String]) =>
    val cb = children2Callback[Seq[String]]((_:Int,_:String,_:Object,children:JList[String],_:Stat) => children)
    connection.wrapped.getChildren(path,true,cb,this)
    k(cb.result)
  }
  def data = shift { k:(Array[Byte] => Array[Byte]) =>
    val cb = dataCallback[Array[Byte]]((_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => data)
    connection.wrapped.getData(path,true,cb,this)
    k(cb.result)
  }
}

object ZKPath {
  class ZKPathW(path:ZKPath) {
    def <<(action:ZKAction):ZKPath = {action(path); path}
  }

  implicit def ZKPathW(path:ZKPath):ZKPathW = new ZKPathW(path)
}

object ZKCallbacks {
  class StatCallbackW[T](responder:(Int,String,Object,Stat)=>T) extends StatCallback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,stat:Stat):Unit = {
      result = responder(rc,path,ctx,stat)
    }

  }
  class ACLCallbackW[T](responder:((Int,String,Object,JList[ACL],Stat)=>T)) extends ACLCallback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,acl:JList[ACL],stat:Stat):Unit = {
      result = responder(rc,path,ctx,acl,stat)
    }
  }
  class Children2CallbackW[T](responder:((Int,String,Object,JList[String],Stat)=>T)) extends Children2Callback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,children:JList[String],stat:Stat)  = {
      result = responder(rc,path,ctx,children,stat)
    }
  }
  class DataCallbackW[T](responder:((Int,String,Object,Array[Byte],Stat)=>T)) extends DataCallback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,data:Array[Byte],stat:Stat):Unit = {
      result = responder(rc,path,ctx,data,stat)
    }
  }

  def statCallback[T](responder:(Int,String,Object,Stat)=>T):StatCallbackW[T] = new StatCallbackW[T](responder)
  def aclCallback[T](responder:(Int,String,Object,JList[ACL],Stat)=>T):ACLCallbackW[T] = new ACLCallbackW[T](responder)
  def children2Callback[T](responder:(Int,String,Object,JList[String],Stat)=>T):Children2CallbackW[T] = new Children2CallbackW[T](responder)
  def dataCallback[T](responder:(Int,String,Object,Array[Byte],Stat)=>T):DataCallbackW[T] = new DataCallbackW[T](responder)
}

object ZK {
  def apply(connectString:String,sessionTimeout:Int,watcher:Watcher):ZK = new ZK(new ZooKeeper(connectString,sessionTimeout,watcher))
}

object Test {
  def foo:Stat = reset {
    import ZKAction._
    val zk = ZK("",0,null)
    val path = new ZKPath("foo",zk)
    val stat = path.stat
    val stat1 = path.stat
    val stat2 = path.stat
    val stat3 = path.stat
    path << delete(anyVersion)
    stat
  }
}