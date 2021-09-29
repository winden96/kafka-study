# JVM参数设置

修改 bin/kafka-start-server.sh 中的 jvm 设置

```sh
export KAFKA_HEAP_OPTS="‐Xmx16G ‐Xms16G ‐Xmn12G ‐XX:MetaspaceSize=256M ‐XX:+UseG1GC ‐XX:MaxGCPauseMillis=50"
```

这种大内存的情况一般都要用G1垃圾收集器，因为年轻代内存比较大，用G1可以设置GC最大停顿时间，不至于一次minor gc就花费太长时间。

