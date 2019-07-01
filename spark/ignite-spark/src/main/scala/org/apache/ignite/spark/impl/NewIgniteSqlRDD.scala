package org.apache.ignite.spark.impl

import org.apache.ignite.cache.query.Query
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.{TaskContext, Partition}
import scala.reflect.ClassTag

case class NewIgnitePartition[T](qry: Query[T],idx: Int) extends Partition {
  override def index: Int = idx
}
class NewIgniteSqlRDD[R: ClassTag, T, K, V](
                                          ic: IgniteContext,
                                          cacheName: String,
                                          cacheCfg: CacheConfiguration[K, V],
                                          conv: (T) ⇒ R,
                                          keepBinary: Boolean,
                                          partitions: Array[Partition]
                                        ) extends IgniteAbstractRDD[R, K, V](ic, cacheName, cacheCfg, keepBinary) {
  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val cur = ensureCache().query(split.asInstanceOf[NewIgnitePartition[T]].qry)

    TaskContext.get().addTaskCompletionListener((_) ⇒ cur.close())

    new IgniteQueryIterator[T, R](cur.iterator(), conv)
  }

  override protected def getPartitions: Array[Partition] = partitions
}

object NewIgniteSqlRDD {
  def apply[R: ClassTag, T, K, V](ic: IgniteContext, cacheName: String, cacheCfg: CacheConfiguration[K, V],
                                conv: (T) ⇒ R, keepBinary: Boolean,
                                  partitions: Array[Partition] ): NewIgniteSqlRDD[R, T, K, V] =
    new NewIgniteSqlRDD(ic, cacheName, cacheCfg,  conv, keepBinary, partitions)
}

