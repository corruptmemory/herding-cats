/**
 * package.scala
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

package com.corruptmemory

package object herding_cats extends ZKVersions
                            with ZKACL
                            with Zookeepers
                            with Errors
                            with Results
                            with StateTs
                            with ZKSerializers {
  import scalaz._
  import scalaz.concurrent._
  import Scalaz._
  def emptyPromise[A](implicit s: Strategy):Promise[A] = new Promise[A]()(s)

  def promiseUnit[S]:ZKState[S, Unit] =
    stateT[PromisedResult,S,Unit](s => promise((s,()).success[Error]))

  def promiseResult[S,A](a: => A):ZKState[S, A] =
    stateT[PromisedResult,S,A](s => promise((s,a).success[Error]))

  def errorState[S,A](e:Error):ZKState[S, A] =
    stateT[PromisedResult,S,A](_ => promise(e.fail[(S,A)]))

  def mkZKState[S, A](s: => S,a: => A):ZKState[S,A] =
    stateT[PromisedResult, S, A](_ => promise((s,a).success[Error])(Strategy.Sequential))

  def shutdownUnit[S]:ZKState[S, Unit] =
    stateT[PromisedResult,S,Unit](s => promise(shutdown.fail[(S,Unit)]))
}
