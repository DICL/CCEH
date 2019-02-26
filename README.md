# Cacheline-Concious Extendible Hashing (CCEH)

Low latency storage media such as byte-addressable per- sistent memory (PM) 
requires rethinking of various data structures in terms of optimization. One of
the main chal- lenges in implementing hash-based indexing structures on PM is
how to achieve efficiency by making effective use of cachelines while 
guaranteeing failure-atomicity for dy- namic hash expansion and shrinkage. In
this paper, we present Cacheline-Conscious Extendible Hashing (CCEH) that
reduces the overhead of dynamic memory block man- agement while guaranteeing
constant hash table lookup time. CCEH guarantees failure-atomicity without
making use of explicit logging. Our experiments show that CCEH effec- tively
adapts its size as the demand increases under the fine- grained
failure-atomicity constraint and its maximum query latency is an order of
magnitude lower compared to the state- of-the-art hashing techniques.

For more details about CCEH, please refer to USENIX FAST 2019 paper - 
"Write-Optimized Dynamic Hashing for Persistent Memory"

```
make ALL_CCEH
```
