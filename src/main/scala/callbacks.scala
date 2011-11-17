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

object ZKCallbacks {
  import org.apache.zookeeper.KeeperException.Code
  def rcDumper(rc:Int):Unit = println(Code.get(rc).toString)
  def rcWrap[T](rc:Int)(result: => Result[T]):Result[T] = Code.get(rc) match {
    case Code.OK => result
    case Code.SESSIONEXPIRED | Code.NOAUTH => disconnected.failNel
    case x@_ => message("Not OK: %s".format(x.toString)).failNel
  }
  def done[T](result:Result[T]):Boolean =
    result.fold(success = _ => true,
                failure = f => f.list.head match {
                  case Message(_) => false
                  case _ => true
                })

  class StatCallbackW[T,K](zk:ZK,cont:Result[T] => K,responder:(Int,String,Object,Stat)=>Result[T]) extends StatCallback {
    def processResult(rc:Int,path:String,ctx:Object,stat:Stat):Unit = {
      rcDumper(rc)
      val result = rcWrap(rc)(responder(rc,path,ctx,stat))
      if (!done(result)) zk.withWrapped(_.exists(path,true,this,null))
      else cont(result)
    }
  }

  class ACLCallbackW[T,K](zk:ZK,cont:Result[T] => K,responder:((Int,String,Object,JList[ACL],Stat)=>Result[T])) extends ACLCallback {
    def processResult(rc:Int,path:String,ctx:Object,acl:JList[ACL],stat:Stat):Unit = {
      rcDumper(rc)
      val result = rcWrap(rc)(responder(rc,path,ctx,acl,stat))
      val statIn = new Stat()
      if (!done(result)) zk.withWrapped(_.getACL(path,statIn,this,null))
      else cont(result)
    }
  }

  class Children2CallbackW[T,K](zk:ZK,cont:Result[T] => K,responder:((Int,String,Object,JList[String],Stat)=>Result[T])) extends Children2Callback {
    def processResult(rc:Int,path:String,ctx:Object,children:JList[String],stat:Stat)  = {
      rcDumper(rc)
      val result = rcWrap(rc)(responder(rc,path,ctx,children,stat))
      if (!done(result)) zk.withWrapped(_.getChildren(path,true,this,null))
      else cont(result)
    }
  }

  class DataCallbackW[T,K](zk:ZK,cont:Result[T] => K,responder:((Int,String,Object,Array[Byte],Stat)=>Result[T])) extends DataCallback {
    def processResult(rc:Int,path:String,ctx:Object,data:Array[Byte],stat:Stat):Unit = {
      rcDumper(rc)
      val result = rcWrap(rc)(responder(rc,path,ctx,data,stat))
      if (!done(result)) zk.withWrapped(_.getData(path,true,this,null))
      else cont(result)
    }
  }

  def statCallback[T,K](zk:ZK,cont:Result[T] => K,responder:(Int,String,Object,Stat)=>Result[T]):StatCallbackW[T,K] = new StatCallbackW[T,K](zk,cont,responder)
  def aclCallback[T,K](zk:ZK,cont:Result[T] => K,responder:(Int,String,Object,JList[ACL],Stat)=>Result[T]):ACLCallbackW[T,K] = new ACLCallbackW[T,K](zk,cont,responder)
  def children2Callback[T,K](zk:ZK,cont:Result[T] => K,responder:(Int,String,Object,JList[String],Stat)=>Result[T]):Children2CallbackW[T,K] = new Children2CallbackW[T,K](zk,cont,responder)
  def dataCallback[T,K](zk:ZK,cont:Result[T] => K,responder:(Int,String,Object,Array[Byte],Stat)=>Result[T]):DataCallbackW[T,K] = new DataCallbackW[T,K](zk,cont,responder)
}
