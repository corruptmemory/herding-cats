# About

`herding-cats` is a Scala library for working with [Apache Zookeeper](http://zookeeper.apache.org/).  It uses
[Scalaz extensively](http://code.google.com/p/scalaz/), in particular [Scalaz Promises](https://github.com/scalaz/scalaz/blob/master/core/src/main/scala/scalaz/concurrent/Promise.scala) to eliminate structuring your code as a
series of callbacks (i.e.: inversion of control).  The result is a much cleaner way to work with ZooKeeper.

## Status

Just finished promised-based version.  Much easier to understand relative to the continuations-based version.

## License

Licensed under Apache 2.0, see `LICENSE.txt` for details.