package org.apache.ignite.spark

import java.util.Map.Entry

import com.szkingdom.fspt.spark.ignite.test.{Person, PersonKey}
import org.apache.ignite.cache.query.{SqlFieldsQuery, SqlQuery}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteSystemProperties, Ignition}
import org.scalatest.{BeforeAndAfter, FunSuite}

class IgniteTest extends  FunSuite with BeforeAndAfter{

  var ignite:Ignite = _
  before{
/*    System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false")*/

    ignite  = Ignition.start("stk_order.xml")

  }
  after{
    ignite.close()
  }
  test("public"){
    //SQL_PUBLIC_PERSON2
    val cache= ignite.getOrCreateCache[Any, Any]("stk_order12")
    val ccfg = cache.getConfiguration(classOf[CacheConfiguration[Any, Any]])
 print(ccfg.getCacheMode)
 print(ccfg.getAffinity)
/*    val sql = " id = '10'";
    import javax.cache.Cache
  val matched:  java.util.List[Cache.Entry[PersonKey,Person]] = cache.query(new SqlQuery[PersonKey,Person](classOf[Person], sql)).getAll();
    if(null != matched){
      import scala.collection.JavaConversions._
      matched.toSeq.foreach(i=>{
        println(s"key:${i.getKey},value:${i.getValue}")
      })
    }*/



  }
  test("destory cache"){
    ignite.destroyCache("objectRDD")
  }

  test("ignite node parts"){
    import org.apache.ignite.cluster.ClusterNode

    import scala.collection.JavaConversions._
    import scala.collection.mutable.ArrayBuffer
    val aff = ignite.affinity("objectRDD")
    val parts = aff.partitions()

    val nodesToParts = (0 until parts).foldLeft(Map[ClusterNode, ArrayBuffer[Int]]()) {
      case (nodeToParts, ignitePartIdx) =>
        val primary = aff.mapPartitionToPrimaryAndBackups(ignitePartIdx).head

        if (nodeToParts.contains(primary)) {
          nodeToParts(primary) += ignitePartIdx

          nodeToParts
        }
        else
          nodeToParts + (primary -> ArrayBuffer[Int](ignitePartIdx))
    }
    println(nodesToParts)
  }
}
