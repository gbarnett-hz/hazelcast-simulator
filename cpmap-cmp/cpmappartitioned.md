# A Partitioned `CPMap`, `CPMapPartitioned`

One of the fundamental drawbacks of `CPMap` is that it is hosted within a single `CPGroup`. Therefore, we are limited in terms of the size of data that the `CPMap` can host (e.g. snapshot pressure) and the throughput upon a particular `CPMap` (all operations are totally ordered with respect to that `CPGroup`.

A partitioned `CPMap` is relatively trivial in its most basic form: it's simply a wrapper around one or more `CPMap`s, with each partition (distinct `CPMap`) being hosted in a distinct `CPGroup`. 

```
                ┌─────────────────────────┐                                                               
                │    CPMapPartitioned     │                                                               
┌───────────────┴─────────────────────────┴───────────────┐                                               
│ ┌────────────┐  ┌────────────┐           ┌────────────┐ │                                               
│ │            │  │            │           │            │ │                                               
│ │            │  │            │           │            │ │                                               
│ │            │  │            │  ┌─────┐  │            │ │                                               
│ │  CPGroup   │  │  CPGroup   │  │ ... │  │  CPGroup   │ │                                               
│ │ ┌────────┐ │  │ ┌────────┐ │  └─────┘  │ ┌────────┐ │ │                  ┌───────────────────────────┐
│┌┼▶│ CPMap  │ │  │ │ CPMap  │ │     ▲     │ │ CPMap  │◀┼─┼──────────────────│        partitions         │
│││ └────────┘ │  │ └────────┘ │     │     │ └────────┘ │ │                  └───────────────────────────┘
││└────────────┘  └──────▲─────┘     │     └────────────┘ │                                │              
└┼───────────────────────┼───────────┼────────────────────┘                                │              
 └───────────────────────┴───────────┴─────────────────────────────────────────────────────┘
```

Avantages, 

- Scales beyond per-`CPMap` data limit (~2GB)
- Increases concurrency:
  - Operations are hashed to one of many distinct `CPGroup`s, with each `CPGroup` rendering a total ordering over a portion of the global key space
  - Snapshotting occurs across smaller (finer grained) `CPGroup`s
- `CPMapPartitioned` abstracts constituent `CPMap` construction
- Partitions come with no additional backup/partition table protocol: CP already provides this via Raft

Disadvantages,

- Increases surface area of CP across operation threads
  - More partitions corresponds to an increased number of `CPGroup`s, each mapped-to an operation thread, icreasing the probability that CP operations will impact AP operations executions. (This data structure is a genuine use case for isolated CP operation threads.)
- Partitions are limited to ~2GB as they are each a `CPMap`
- Partitions are statically defined and constructed, similar to AP. (I omit dynamic partition addition/removal as that's more complex.)
- Partitions need to be defined and calculated to strike a balance between the following:
  - `CPMap` (partition) size: compromise between partition residen data size and snapshot cost
  - `CPGroup` mapping to a `CPMap`. Here, there is a one-to-one mapping, and it's testing in a way where for a 36 vCPU machine each `CPGroup` should be mapped-to a distinct operation thread. The best case. 
- Sustained load requires modifying the Raft advance log for snapshotting
  - without this the deefault results in frequent sawtoothing GC can be observed that manifests into full GC which results in loss of servicing access requests (this is not specific to `CPMap` or `CPMapPartitioned`)
  - this configuration impacts `RaftLog`'s size
  - the log advance configuration requires performance and memory analysis to find its approximate value
  - configuration is a global Raft property, not specific to a `CPGroup`
- Loss of transactionality
  - batch commands, should they ever be introduced, would only be transactional across a single partition. For example, `map.setAll(...)`, would not be transactional in its entirety, only each partition's key space


Topology,

- Every member of a `CPGroup` hosts the full data residency of that `CPGroup`
  - Generally, we need more CP members than the `CPGroup` size, so that we can reduce the resident memory per-JVM
  - Factors include,
    - snapshotting
    - `RaftLog`
    - internal backing data structures, e.g. the map itself, auxilary data structures

Auxilary bits,

- From my small testing, you need to aggressively add CP members to reduce the memory pressure issues
