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

object ZKCallbacks {
  import org.apache.zookeeper.KeeperException.Code
  def rcDumper(rc:Int):Unit = println(Code.get(rc).toString)

  class StatCallbackW[T](responder:(Int,String,Object,Stat)=>T) extends StatCallback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,stat:Stat):Unit = {
      rcDumper(rc)
      result = responder(rc,path,ctx,stat)
    }
  }

  class ACLCallbackW[T](responder:((Int,String,Object,JList[ACL],Stat)=>T)) extends ACLCallback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,acl:JList[ACL],stat:Stat):Unit = {
      rcDumper(rc)
      result = responder(rc,path,ctx,acl,stat)
    }
  }

  class Children2CallbackW[T](responder:((Int,String,Object,JList[String],Stat)=>T)) extends Children2Callback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,children:JList[String],stat:Stat)  = {
      rcDumper(rc)
      result = responder(rc,path,ctx,children,stat)
    }
  }

  class DataCallbackW[T](responder:((Int,String,Object,Array[Byte],Stat)=>T)) extends DataCallback {
    var result:T = _
    def processResult(rc:Int,path:String,ctx:Object,data:Array[Byte],stat:Stat):Unit = {
      rcDumper(rc)
      result = responder(rc,path,ctx,data,stat)
    }
  }

  def statCallback[T](responder:(Int,String,Object,Stat)=>T):StatCallbackW[T] = new StatCallbackW[T](responder)
  def aclCallback[T](responder:(Int,String,Object,JList[ACL],Stat)=>T):ACLCallbackW[T] = new ACLCallbackW[T](responder)
  def children2Callback[T](responder:(Int,String,Object,JList[String],Stat)=>T):Children2CallbackW[T] = new Children2CallbackW[T](responder)
  def dataCallback[T](responder:(Int,String,Object,Array[Byte],Stat)=>T):DataCallbackW[T] = new DataCallbackW[T](responder)
}
