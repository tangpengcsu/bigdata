package org.apache.ignite

import scala.reflect.ClassTag


package object spark {
  implicit def IgniteFunction[K:ClassTag, V:ClassTag](igniteRDD: IgniteRDD[K, V]): IgniteFunctions[K, V] = {
    new IgniteFunctions(igniteRDD.ic, igniteRDD.cacheName, igniteRDD.cacheCfg, igniteRDD.keepBinary, igniteRDD.partitions.size)
  }
}

