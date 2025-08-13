Chardonnay MVP
This repository provides a minimal implementation of the core ideas from the OSDI ’23 paper “Chardonnay: Fast and General Datacenter Transactions for On‑Disk Databases”. It is a proof‑of‑concept of a partitioned, replicated key‑value store with multi‑version concurrency control, per‑shard consensus, two‑phase commit and epoch‑based snapshot reads.


Notes
This project is intended for educational and experimentation purposes. It omits many optimisations and features of a production system (e.g. persistent storage, asynchronous networking and failover). See the paper for the full design details and performance evaluations.