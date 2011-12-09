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

  // Questionable
  final class ZKState1W[A](zks:ZKState1[A]) {
    def withFilter(p:A => Boolean):ZKState1[A] = {
      zks.flatMap { a =>
        if (p(a)) zks
        else {
          stateT[PromisedResult,S,A] { s =>
            val np = emptyPromise[Result[(S,A)]](Strategy.Sequential)
            np fulfill (s,a).successNel[Error]
            np
          }
        }
      }
    }
  }

  implicit def toZKState1W[A](zks:ZKState1[A]):ZKState1W[A] =
    new ZKState1W[A](zks)

  import ZKCallbacks._
  def path:String
  def connection:ZK

  def makePromise[A](f:PromisedResult[A] => Unit):ZKState1[A] = {
    val p = emptyPromise[Result[A]](Strategy.Sequential)
    f(p)
    stateT[PromisedResult,S,A](s => p map (r => r map (a => (s,a))))
  }


  def stat:ZKState1[Stat] =
    makePromise[Stat] { p =>
      val cb = statCallback(connection,p,(_:Int,_:String,_:Object,stat:Stat) => stat.successNel)
      connection.withWrapped(_.exists(path,true,cb,this))
    }

  // def exists(f:ZKState1[Unit]):ZKState1[Unit] =
  //   stat.flatMap{s =>
  //     if (s != null) f
  //     else stateT[PromisedResult,S,Unit] { s =>
  //       val p = emptyPromise[Result[(S,Unit)]](Strategy.Sequential)
  //       p fulfill (s,()).successNel[Error]
  //       p
  //     }
  //   }

  // def notExists(f:ZKState1[Unit]):ZKState1[Unit] =
  //   stat.flatMap{s =>
  //     if (s == null) f
  //     else stateT[PromisedResult,S,Unit] { s =>
  //       val p = emptyPromise[Result[(S,Unit)]](Strategy.Sequential)
  //       p fulfill (s,()).successNel[Error]
  //       p
  //     }
  //   }

  def exists(f:ZKState1[Unit]):ZKState1[Unit] =
    for {
      s <- stat if s != null
      _ <- f
    } yield ()

  def notExists(f:ZKState1[Unit]):ZKState1[Unit] =
    for {
      s <- stat if s == null
      _ <- f
    } yield ()

  def statAndACL:ZKState1[Tuple2[Stat,Seq[ZKAccessControlEntry]]] =
    makePromise[Tuple2[Stat,Seq[ZKAccessControlEntry]]] { p =>
      val cb = aclCallback(connection,p,(_:Int,_:String,_:Object,jacl:JList[ACL],stat:Stat) => (stat,toSeqZKAccessControlEntry(jacl)).successNel)
      val statIn = new Stat()
      connection.withWrapped(_.getACL(path,statIn,cb,this))
    }

  def children:ZKState1[Traversable[String]] =
    makePromise[Traversable[String]] { p =>
      val cb = children2Callback(connection,p,(_:Int,_:String,_:Object,children:JList[String],_:Stat) => children.toSeq.successNel)
      connection.withWrapped(_.getChildren(path,true,cb,this))
    }

  def data:ZKState1[Array[Byte]] =
    makePromise[Array[Byte]] { p =>
      val cb = dataCallback(connection,p,(_:Int,_:String,_:Object,data:Array[Byte],_:Stat) => data.successNel)
      connection.withWrapped(_.getData(path,true,cb,this))
    }
}

class ZKPathReader[S](val path:String,val connection:ZK) extends ZKPathBase[S]

class ZKPathWriter[S](val path:String,val connection:ZK) extends ZKPathBase[S] {
  import ZKCallbacks._
  def create(data:Array[Byte],acl:Seq[ZKAccessControlEntry], createMode:CreateMode):ZKState1[String] =
    makePromise[String] { p =>
      val cb = createCallback(connection,data,acl,createMode,p,(_:Int,_:String,_:Object,name:String) => name.successNel)
      connection.withWrapped(_.create(path,data,acl,createMode,cb,this))
    }

  def delete(version:ZKVersion = anyVersion):ZKState1[Unit] =
    makePromise[Unit] { p =>
      val cb = deleteCallback(connection,version,p,(_:Int,_:String,_:Object) => ().successNel)
      connection.withWrapped(_.delete(path,version.value,cb,this))
    }

  def update(data:Array[Byte],version:ZKVersion):ZKState1[Stat] =
    makePromise[Stat] { p =>
      val cb = setDataCallback(connection,data,version,p,(_:Int,_:String,_:Object,stat:Stat) => stat.successNel)
      connection.withWrapped(_.setData(path,data,version.value,cb,this))
    }

  def updateACL(acl:Seq[ZKAccessControlEntry],version:ZKVersion):ZKState1[Stat] =
    makePromise[Stat] { p =>
      val cb = setAclCallback(connection,acl,version,p,(_:Int,_:String,_:Object,stat:Stat) => stat.successNel)
      connection.withWrapped(_.setACL(path,acl,version.value,cb,this))
    }
}
