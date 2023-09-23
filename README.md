
# bknr.cluster

A Highly Available replicated cluster of bknr.datastores. Currently
only supported on Lispworks, but it's probably not too hard to add
support for other implementations.

## Context

bknr.datastore is a really fantastic datastore to prototype and build
new applications.

But bknr.datastore is a single isolated process running on single
machine. That said, with modern tools you can achieve a reasonable
availability quite easily: e.g., on EC2 if you use EBS io2, or EFS, if
your machine goes down EC2 will just recreate it.

So you'll lose a few minutes. That's perfectly fine for a small startup.

However, eventually you get a customer that requires five 9s of
reliability and then that's just not going to cut it.

Also, relying on EC2's restarts for availability is *hard to
test*. Each time you test your failover you're going to have a few
minutes of downtime, which in practice means you'll almost never test
your failover.

bknr.cluster attempts to solve this problem by replicating your
transaction logs across multiple machines in real time.

## Raft, braft

This library is built on the shoulder of giants. For one, bknr.datastore.

But also, the [Raft Consensus Protocol](https://en.wikipedia.org/wiki/Raft_(algorithm))

I attempted building Raft from scratch in Lisp. But while it's not
that impossible, it seemed like more effort than I wanted to. Plus,
there's a big issue with GC pauses: A CL GC pause might make it look
like a leader is unavailable, which might make a switch happen, and we
want to avoid leader switches as much as possible.

So we settled on the absolutely fantastic
[braft](https://github.com/baidu/braft) library from Baidu. This is a
C++ library. It's not lightweight, but it does live up to its promise
of being Industrial Grade from what I've seen so far.

## Other niceness

Braft is better than plain bknr.datastore at maintain data
integrity. For instance bknr.datastore doesn't checksum transactions
(but it's easy to add). If bknr.datastore crashes during a snapshot,
you have to manually recover things (bugs that need fixing).

But also, Braft supports asynchronous snapshots. Obviously, this needs
some additional changes to bknr-datastore (and to any subsystem you're
using), but that's something we plan to add.

## Installation

bknr.cluster absolutely needs a fork of bknr.datastore: https://github.com/tdrhq/bknr-datastore.

In particular, the fork ensures that transactions are only committed
after being applied to a majority of replicas.

bknr.cluster also needs braft, which is a pretty heavy
dependency. (For this reason, it's unlikely that you'll find
bknr.cluster in Quicklisp any time soon.)

First install brpc: https://github.com/apache/brpc
Then install braft: https://github.com/baidu/braft

And then the hope is you'll be able to load bknr.cluster with quick-patch:

```
    (ql:quickload :quick-patch)
    (quick-patch:register "https://github.com/tdrhq/bknr.cluster" "main")
    (quick-patch:checkout-all "build/")
```

But at this point, I suspect things will be buggy.

You can create a cluster of stores like so:

```lisp
(make-instance 'my-test-store
 :election-timeout-ms 100
 :directory dir
 :group "MyGroup"
 :config "127.0.0.1:<port1>:0,127.0.0.1:<port2>:0,127.0.0.1:<port3>:0",
 :port <portN>)
```

You can use the `braft_cli` tool to change the configuration of
servers without downtime.

## Migration

We provide a mixin `backward-compatibility-mixin`. Add this as a
superclass to your store class and we'll *try* to keep the store
backward compatible.

What this means is the following:
* When you load this store, if there's not Raft data, it'll load the
  snapshot from the old location.
* As you write data, it'll keep appending logs to the old transaction
  log.

Keep in mind that snapshots will **not** be propagated to the old
format. It will always have the old snapshot with a long appended log
file. So you probably want to be reasonably sure that you want to make
the switch before you do.

## Administration

TODO

## The usual disclaimers

Screenshotbot.io has been successfully using bknr.cluster in our
production services, including for our Enterprise installs which gets
a lot of use.

So far, it has been super reliable.

## Failover/failback strategies

At Screenshotbot.io, this is how we set up failover:

* We have three machines replicating with bknr.cluster.
* Only the leader responds to queries. (In the future, if we need to
  optimize CPU usage, we could make replicas respond to read queries.)
* To achieve this, we expose an endpoint /raft-state, which returns
  200 if it's a leader, and use a AWS Load Balancer to mark only the
  leader as healthy.
* The above set up is *good enough*. But it might have a minute or so
  downtime if the leader goes down. To do even better:

* we have an nginx server running on each of the servers, and these
  servers are set up to use the other servers in the cluster as a
  backup if it receives a 502.
* If one of the CL services gets a request, and it finds out it's not
  a leader, then it responds with a 502, so that the nginx server will
  just try the next server.

With this complete setup, our downtime goes does to less than 0.5s,
but we have some tricks up our sleeve to remove even that 0.5s
downtime.
