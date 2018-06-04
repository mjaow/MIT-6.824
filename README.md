# MIT-6.824
mit 6.824分布式系统设计的lab练习

- lab1: mapreduce，阅读论文mapreduce，实现mr的核心功能，并基于mr完成wordcount和倒排索引（done）
- lab2A: leader election and heartbeat，阅读论文raft，实现leader election和heartbeat发送（done）
- lab2B: keep a consistent, replicated log of operations，阅读论文raft，实现日志发送，并保证一致性（done）
- lab2C: keep persistent state that survives a reboot，阅读论文raft，实现日志序列化和反序列化，以及完成论文第7页底部的nextindex优化（done）
- lab3A: Key/Value Service without log compaction，基于lab2的raft协议实现一个可容错的kv存储系统，可以不支持日志压缩（doing）
- lab3B: Key/Value Service with log compaction，基于lab2的raft协议实现一个可容错的kv存储系统，需要支持日志压缩（todo）
- lab4A: The Shard Master，基于lab3实现支持分片的kv存储系统（todo）
- lab4B: Sharded Key/Value Server ，基于lab3实现支持分片的kv存储系统（todo）
