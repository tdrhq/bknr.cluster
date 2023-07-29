
# bknr.cluster

A Highly Available replicated cluster of bknr.datastores

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

I attempted building Raft from scratch in Lisp. But which it's not
that impossible, it seemed like more effort than I wanted to. Plus,
there's a big issue with GC pauses: A CL GC pause might make it look
like a leader is unavailable, which might make a switch happen, and we
want to avoid leader switches as much as possible.

So we settled on the absolutely fantastic
[braft](https://github.com/baidu/braft) library from Baidu. This is a
C++ library. It's not lightweight, but it does live up to its promise
of being Industrial Grade from what I've seen so far.

## Installation

bknr.cluster absolutely needs a fork of bknr.datastore: https://github.com/tdrhq/bknr-datastore.

In particular, the fork ensures that transactions are only committed
after being applied to all replicas.

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


## Administration

TODO

## The usual disclaimers

I haven't yet started using this in prod, but I expect to migrate our
servers within the next few weeks (from 7/29/23).

## Failover/failback strategies

TODO.
