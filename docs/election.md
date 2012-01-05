# Leader Election

Leader election using `herding-cats` is very straight-forward: you
register a participant and you receive a callback if you're elected
the leader.  Let's look at some code:

```scala
def participate(i:Int):Unit = withZK[Participant]("/test/election%d".format(i),ZK("127.0.0.1:2181",5000),Participant(none,none,false)) {
  (zk:ZK) =>
    Participant("/elect_test")(zk){println("won election %d".format(i))}
}
```

Breaking this down a bit:

```scala
withZK[Participant]("/test/election%d".format(i),ZK("127.0.0.1:2181",5000),Participant(none,none,false))
```

This call is going to create a `ZooKeeper` client with a _control
node_ `"/test/election%d".format(i)`.  The call to the connection
factory `ZK` connects us to the cluster.  The value
`Participant(none,none,false)` is the initial value for the election
state.  The body of the `withZK` call:

```scala
(zk:ZK) =>
  Participant("/elect_test")(zk){println("won election %d".format(i))}
```

Actually creates an election participant.  The election occurs under
the node `"/elect_test"`, the body:

```scala
{println("won election %d".format(i))}
```

Is called *only if* the current participant has won the election.

## What to do when you win an election

Well, it's great to win an election and all, but exactly what should
one do? A general rule of `herding-cats` is that callbacks must
always return.  So, as in all the other cases the "proper" thing to do
is to construct something of interest and then _ship it off_ to
another thread for consumption.  A nice way to do this is to use
_actors_ or `Scalaz` _promises_ or some such.  You can also directly
manipulate data in a synchronized "out-of-band" fashion as well.  It's
really up to you.
