/** zookeeper.scala
 *
 *  @author <a href="mailto:jim@corruptmemory.com">Jim Powers</a>
 *
 *  Copyright 2011 Jim Powers
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.corruptmemory.herding_cats
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent, CreateMode, ZooDefs }
import ZooDefs.Ids
import scalaz._
import Scalaz._
import concurrent._

/** Package trait used to expose `ZK` functions */
trait Zookeepers {
  /** `ZKState` is a ''monadic'' value abstracting over a state value `S` 
   *    returning a value `A` all wrapped in a [[com.corruptmemory.herding_cats.PromisedResult]]
   *    
   *  @tparam S The state value type
   *  @tparam A The result type
   */
  type ZKState[S, A] = StateT[PromisedResult, S, A]

  /** Apply a `ZK` to a function
   *
   *  This function only returns under the following circumstances:
   *
   *   1. The `body` returns a `Shutdown` failure value
   *   2. The connection to the `ZooKeeper` cluster suffers an unrecoverable failure
   * 
   *  @param controlPath a Zookeeper path string
   *  @param factory A function that takes in a `controlPath`, a state of type `S`, and a 
   *    function with type: `(WatchedEvent, S) => PromisedResult[S]` and yields a ZK value.  This
   *    sets up the ''watcher'' event handler on the ZK connection.
   *  @param initial The initial value of the state.
   *  @param body The function that is invoked each time a watched node changes
   *  @tparam S The type of the state value
   */
  def withZK[S](controlPath: String, factory: (String, S, (WatchedEvent, S) => PromisedResult[S]) => ZK, initial: S)(body: ZK => ZKState[S, Unit]) {
    import Zookeepers._
    import Watcher.Event.{ KeeperState, EventType }
    val syncObject = new Object
    var zk: ZK = null
    def shutdown() {
      syncObject.synchronized {
        syncObject.notifyAll()
      }
    }
    def watcher(event: WatchedEvent, state: S): PromisedResult[S] = {
      event.getState match {
        case KeeperState.SyncConnected =>
          (watchControlNode[S](zk, controlPath)(body) ~> state) map {
            _.fold(failure = { f =>
              if (f match { case Shutdown => true; case _ => false }) shutdown()
              f.fail
            },
              success = _.success)
          }
        case KeeperState.Expired | KeeperState.AuthFailed => { shutdown(); promise(Shutdown.fail) }
        case x @ _ => promise(message(x.toString).fail) // Some other condition that we can ignore.  Need logging!
      }
    }
    zk = factory(controlPath, initial, watcher _)
    syncObject.synchronized {
      syncObject.wait()
    }
    zk.withWrapped(_.close())
  }
}

/** An abstraction over ZooKeeper connections */
final class ZK(val controlPath: String, wrapped: ZooKeeper) {
  
  /** Simple type synonym for operating on a `ZKWriter` */
  type ZKWriterOp[S, B] = ZKWriter[S] => ZKState[S, B]
  
  /** An abstraction over reading values from a ZooKeeper node
   *  
   *  @tparam S The type of the state value
   */
  final class ZKReader[S](conn: ZK) {
    
    /** Get a `ZKPathReader` */
    def path(p: String): ZKPathReader[S] = new ZKPathReader[S](p, conn)
  }

  /** An abstraction over writing values to a ZooKeeper node
   *  
   *  @tparam S The type of the state value
   */
  final class ZKWriter[S](conn: ZK) {

    /** Get a `ZKPathWriter` */
    def path(p: String): ZKPathWriter[S] = new ZKPathWriter[S](p, conn)
  }

  /** Private helper method that exposes the ''wrapped' native ZooKeeper connection */
  private[herding_cats] def withWrapped[T](f: ZooKeeper => T): T = f(wrapped)

  /** The ZooKeeper session ID */
  def id: Long = withWrapped(_.getSessionId)

  /** The ZooKeeper password */
  def password: Array[Byte] = withWrapped(_.getSessionPasswd)

  /** The ZooKeeper session timeout */
  def timeout: Int = withWrapped(_.getSessionTimeout)

  /** Returns a `ZKReader` instance
   * 
   *  @tparam S The type of the state value
   *  @return `ZKReader[S]`
   */
  def reader[S]: ZKReader[S] = new ZKReader[S](this)

  /** Creates a writer scope to the ZooKeeper cluster
   * 
   *  @tparam S The type of the state value
   *  @tparam B The type of the return value
   *  @return `ZKState[S, B]`
   */
  def withWriter[S, B](body: ZKWriterOp[S, B]): ZKState[S, B] = body(new ZKWriter[S](this))

  /** Gets the current ''watches'' state.
   * 
   *  @tparam S The type of the state value
   *  @return `ZKState[S, String]` A value of ''1'' means watches are enabled, ''0'' otherwise.
   */
  def getWatchesState[S]: ZKState[S, String] =
    reader.path(controlPath).data[String]()

  /** Sets the current ''watches'' state.
   * 
   *  @tparam S The type of the state value
   *  @param state A value of ''1'' means watches are enabled, ''0'' otherwise.
   *  @return `ZKState[S, Stat]`
   */
  def setWatchesState[S](state: String): ZKState[S, Stat] =
    withWriter[S, Stat](_.path(controlPath).update(state, anyVersion))

  /** Enables watches
   * 
   *  @tparam S The type of the state value
   *  @return `ZKState[S, Stat]`
   */
  def enableWatches[S]: ZKState[S, Stat] = setWatchesState[S]("1")

  /** Disables watches
   * 
   *  @tparam S The type of the state value
   *  @return `ZKState[S, Stat]`
   */
  def disableWatches[S]: ZKState[S, Stat] = setWatchesState[S]("0")

  /** Creates a ''control node''
   * 
   *  ''Control nodes'' are ephemeral and can be watched to see if a particular client shuts down.
   *
   *  @tparam S The type of the state value
   *  @return `ZKState[S, String]`
   */
  def createControlNode[S]: ZKState[S, String] =
    withWriter[S, String](_.path(controlPath).createIfNotExists("1", toSeqZKAccessControlEntry(Ids.OPEN_ACL_UNSAFE), CreateMode.EPHEMERAL))
}

/** Companion object to the `Zookeepers` trait. */
object Zookeepers {
  /** Set up a watch on the ''control node''.  The ''control node'' is used in several ways:
   *
   *  - If the control node contains a `1` then watches are enabled, otherwise disabled.
   *  - If the control node is deleted it will automatically be re-created with watches enabled.  Also the watch body
   *    will be triggered.  This functionality is particularly useful to ''reset'' an application if one of its
   *    watched nodes get deleted.
   *  - If the control node is modified it will trigger the watch.  If the value of the control node is
   *    `1` then the watch body will be triggered.
   *
   *  ''Control nodes'' are ephemeral and can be watched to see if a particular client shuts down.
   *
   *  @param zk The `ZooKeeper` wrapper
   *  @param controlPath A ZooKeeper path for the ''control node''
   *  @param body The function that will be executed anytime a watched node changes AND watches are enabled.
   *  @tparam S The type of the state value
   *  @return `ZKState[S, Unit]`
   */
  def watchControlNode[S](zk: ZK, controlPath: String)(body: ZK => ZKState[S, Unit]): ZKState[S, Unit] = {
    val path = zk.reader[S].path(controlPath)
    for {
      _ <- zk.createControlNode
      _ <- path.data[String]() >>= (d => if (d == "1") body(zk) else promiseUnit[S])
    } yield ()
  }
}

/** Factory object to create wrappers around a `ZooKeeper` connection */
object ZK {
  /** Creates a wrapper around a `ZooKeeper` connection
   *
   *  Typically users do not create `ZK` objects themselves, instead using the `withZK` function.
   *
   *  @param connectString A valid `ZooKeeper` connection string
   *  @param sessionTimeout A valid `ZooKeeper` session timeout
   *  @param controlPath The path to the connections ''control node''
   *  @param initial The initial value for the state
   *  @param watcher The function that receives the ZooKeeper watch events plus the current state
   *  @tparam S The type of the state value
   *  @return `ZK`
   */
  def apply[S](connectString: String, sessionTimeout: Int)(controlPath: String, initial: S, watcher: (WatchedEvent, S) => PromisedResult[S]): ZK =
    new ZK(controlPath, new ZooKeeper(connectString, sessionTimeout, ZKWatcher(initial, watcher)))
}
