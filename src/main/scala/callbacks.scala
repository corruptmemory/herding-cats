/**
 * callbacks.scala
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
import scalaz._
import Scalaz._
import concurrent._

object ZKCallbacks {
  import org.apache.zookeeper.KeeperException.Code
  def rcWrap[T](path:String,rc:Int)(result: => Result[T]):Result[T] = Code.get(rc) match {
    case Code.OK => result
    case Code.SESSIONEXPIRED | Code.NOAUTH => disconnected.failNel
    case Code.BADVERSION => versionmismatch.failNel
    case Code.NONODE => nonode.failNel
    case x@_ => {
      println("'%s' not OK: %s".format(path,x.toString))
      message("'%s' not OK: %s".format(path,x.toString)).failNel
    }
  }
  def done[T](result:Result[T]):Boolean =
    result.fold(success = _ => true,
                failure = f => f.list.head match {
                  case Message(_) => false
                  case _ => true
                })

  class StatCallbackW(zk:ZK,watch:Boolean,promise:PromisedResult[Stat],responder:(Int,String,Object,Stat)=>Result[Stat]) extends StatCallback {
    def processResult(rc:Int,path:String,ctx:Object,stat:Stat):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,stat))
      if (!done(result)) zk.withWrapped(_.exists(path,watch,this,ctx))
      else promise.fulfill(result)
    }
  }

  class SetDataCallbackW(zk:ZK,data:Array[Byte],version:ZKVersion,promise:PromisedResult[Stat],responder:(Int,String,Object,Stat)=>Result[Stat]) extends StatCallback {
    def processResult(rc:Int,path:String,ctx:Object,stat:Stat):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,stat))
      if (!done(result)) zk.withWrapped(_.setData(path,data,version.value,this,ctx))
      else promise.fulfill(result)
    }
  }

  class SetAclCallbackW(zk:ZK,acl:JList[ACL],version:ZKVersion,promise:PromisedResult[Stat],responder:(Int,String,Object,Stat)=>Result[Stat]) extends StatCallback {
    def processResult(rc:Int,path:String,ctx:Object,stat:Stat):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,stat))
      if (!done(result)) zk.withWrapped(_.setACL(path,acl,version.value,this,ctx))
      else promise.fulfill(result)
    }
  }

  class ACLCallbackW(zk:ZK,promise:PromisedResult[Tuple2[Stat,Seq[ZKAccessControlEntry]]],responder:((Int,String,Object,JList[ACL],Stat)=>Result[Tuple2[Stat,Seq[ZKAccessControlEntry]]])) extends ACLCallback {
    def processResult(rc:Int,path:String,ctx:Object,acl:JList[ACL],stat:Stat):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,acl,stat))
      val statIn = new Stat()
      if (!done(result)) zk.withWrapped(_.getACL(path,statIn,this,ctx))
      else promise.fulfill(result)
    }
  }

  class Children2CallbackW(zk:ZK,watch:Boolean,promise:PromisedResult[Traversable[String]],responder:((Int,String,Object,JList[String],Stat)=>Result[Traversable[String]])) extends Children2Callback {
    def processResult(rc:Int,path:String,ctx:Object,children:JList[String],stat:Stat)  = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,children,stat))
      if (!done(result)) zk.withWrapped(_.getChildren(path,watch,this,ctx))
      else promise.fulfill(result)
    }
  }

  class DataCallbackW[T](zk:ZK,watch:Boolean,promise:PromisedResult[T],responder:((Int,String,Object,Array[Byte],Stat)=>Result[T])) extends DataCallback {
    def processResult(rc:Int,path:String,ctx:Object,data:Array[Byte],stat:Stat):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,data,stat))
      if (!done(result)) zk.withWrapped(_.getData(path,watch,this,ctx))
      else promise.fulfill(result)
    }
  }

  class CreateCallbackW(zk:ZK,data:Array[Byte],acl:JList[ACL],createMode:CreateMode,promise:PromisedResult[String],responder:((Int,String,Object,String)=>Result[String])) extends StringCallback {
    def processResult(rc:Int,path:String, ctx:Object, name:String):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx,name))
      if (!done(result)) zk.withWrapped(_.create(path,data,acl,createMode,this,ctx))
      else {
        println("created: %s".format(path))
        promise.fulfill(result)
      }
    }
  }

  class DeleteCallbackW(zk:ZK,version:ZKVersion,promise:PromisedResult[Unit],responder:((Int,String,Object)=>Result[Unit])) extends VoidCallback {
    def processResult(rc:Int,path:String,ctx:Object):Unit = {
      val result = rcWrap(path,rc)(responder(rc,path,ctx))
      if (!done(result)) zk.withWrapped(_.delete(path,version.value,this,ctx))
      else promise.fulfill(result)
    }
  }

  def statCallback(zk:ZK,watch:Boolean,promise:PromisedResult[Stat],responder:(Int,String,Object,Stat)=>Result[Stat]):StatCallbackW =
    new StatCallbackW(zk,watch,promise,responder)
  def setDataCallback(zk:ZK,data:Array[Byte],version:ZKVersion,promise:PromisedResult[Stat],responder:(Int,String,Object,Stat)=>Result[Stat]):SetDataCallbackW =
    new SetDataCallbackW(zk,data,version,promise,responder)
  def setAclCallback(zk:ZK,acl:JList[ACL],version:ZKVersion,promise:PromisedResult[Stat],responder:(Int,String,Object,Stat)=>Result[Stat]):SetAclCallbackW =
    new SetAclCallbackW(zk,acl,version,promise,responder)
  def aclCallback(zk:ZK,promise:PromisedResult[Tuple2[Stat,Seq[ZKAccessControlEntry]]],responder:(Int,String,Object,JList[ACL],Stat)=>Result[Tuple2[Stat,Seq[ZKAccessControlEntry]]]):ACLCallbackW =
    new ACLCallbackW(zk,promise,responder)
  def children2Callback(zk:ZK,watch:Boolean,promise:PromisedResult[Traversable[String]],responder:(Int,String,Object,JList[String],Stat)=>Result[Traversable[String]]):Children2CallbackW =
    new Children2CallbackW(zk,watch,promise,responder)
  def dataCallback[T](zk:ZK,watch:Boolean,promise:PromisedResult[T],responder:(Int,String,Object,Array[Byte],Stat)=>Result[T]):DataCallbackW[T] =
    new DataCallbackW[T](zk,watch,promise,responder)
  def createCallback(zk:ZK,data:Array[Byte],acl:JList[ACL],createMode:CreateMode,promise:PromisedResult[String],responder:(Int,String,Object,String)=>Result[String]):CreateCallbackW =
    new CreateCallbackW(zk,data,acl,createMode,promise,responder)
  def deleteCallback(zk:ZK,version:ZKVersion,promise:PromisedResult[Unit],responder:(Int,String,Object)=>Result[Unit]):DeleteCallbackW =
    new DeleteCallbackW(zk,version,promise,responder)
}
