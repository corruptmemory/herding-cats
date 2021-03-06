/** result.scala
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
import scalaz._
import scalaz.concurrent._
import Scalaz._

/** Trait for the package that defines the results of Zookeeper operations */
trait Results {
  /** A Result is just a `Validation` over `Error` and `T` */
  type Result[T] = Validation[Error, T]
  
  /** Just a `Promise` of a `Result` */
  type PromisedResult[T] = Promise[Result[T]]

  /** Implicit evidence of a `Bind[PromisedResult]` */
  implicit def PromisedResultBind: Bind[PromisedResult] = new Bind[PromisedResult] {
    def bind[A, B](r: PromisedResult[A], f: A => PromisedResult[B]) = r flatMap { v =>
      v.fold(failure = { a =>
        val p = emptyPromise[Result[B]](Strategy.Sequential)
        p fulfill a.fail[B]
        p
      },
        success = s => f(s))
    }
  }

  /** Implicit evidence of a `Functor[PromisedResult]` */
  implicit def PromisedResultFunctor: Functor[PromisedResult] = new Functor[PromisedResult] {
    def fmap[A, B](r: PromisedResult[A], f: A => B): PromisedResult[B] = r.map(_ map f)
  }

  /** Implicit evidence of a `Pure[PromisedResult]` */
  implicit def PromisedResultPure: Pure[PromisedResult] = new Pure[PromisedResult] {
    def pure[A](a: => A): PromisedResult[A] = promise(a.success)
  }

  /** Implicit evidence of a `Pointed[PromisedResult]` */
  implicit def PromisedResultPointed: Pointed[PromisedResult] =
    Pointed.pointed[PromisedResult](PromisedResultFunctor, PromisedResultPure)
}