/**
 * stateT.scala
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
import scalaz._
import Scalaz._

trait StateTs {
  def initT[F[_],S](implicit p:Pointed[F]): StateT[F, S, S] =
    stateT[F, S, S](s => p.pure((s, s)))

  def modifyT[F[_],S](f: S => S)(implicit p:Pointed[F], b:Bind[F]):StateT[F, S, Unit] =
    initT[F,S] flatMap (s => stateT[F,S,Unit](_ => p.pure((f(s), ()))))

  def putT[F[_],S](s: S)(implicit p:Pointed[F]):StateT[F, S, Unit] =
    stateT[F, S, Unit](_ => p.pure((s, ())))

  def getsT[F[_],S,A](f: S => A)(implicit p:Pointed[F]): StateT[F, S, A] =
    for (s <- initT[F,S]) yield f(s)

  def passT[F[_],S](implicit p:Pointed[F], b:Bind[F]): StateT[F, S, Unit] =
    modifyT((x:S) => x)
}
