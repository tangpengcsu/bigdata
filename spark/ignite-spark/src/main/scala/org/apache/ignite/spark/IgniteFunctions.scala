package org.apache.ignite.spark

import java.util.concurrent.TimeUnit

import javax.cache.Cache
import org.apache.ignite.cache.query.{SqlFieldsQuery, SqlQuery}
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.internal.processors.cache.query.QueryCursorEx
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata
import org.apache.ignite.spark.impl.{NewIgnitePartition, NewIgniteSqlRDD}
import org.apache.spark.Partition
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class IgniteFunctions[K: ClassTag, V: ClassTag](override val ic: IgniteContext,
                                                override val cacheName: String,
                                                override val cacheCfg: CacheConfiguration[K, V],
                                                override val keepBinary: Boolean,
                                                partitionNum: Int)
  extends IgniteRDD[K, V](ic, cacheName, cacheCfg, keepBinary) {


  /**
    * Builds spark schema from query metadata.
    *
    * @param fieldsMeta Fields metadata.
    * @return Spark schema.
    */
  private def buildSchema(fieldsMeta: java.util.List[GridQueryFieldMetadata]): StructType = {
    new StructType(fieldsMeta.map(i ⇒
      new StructField(i.fieldName(), IgniteRDD.dataType(i.fieldTypeName(), i.fieldName()), nullable = true))
      .toArray)
  }



  def sqlModTemplate(sql:String):DataFrame={
    sqlModTemplate(sql,0)
  }
  def sqlModTemplate(sql: String, newPatitionNum: Int): DataFrame = {
    sqlModTemplate(sql, true, 1024, newPatitionNum)
  }
  def sqlModTemplate(sql:String, isLazy: Boolean, pageSize: Int, newPatitionNum:Int):DataFrame = {
    val defaultFieldQuery = new SqlFieldsQuery("")
    defaultFieldQuery.setLazy(isLazy)
    defaultFieldQuery.setPageSize(pageSize)
    sqlModTemplate(sql, defaultFieldQuery, newPatitionNum)
  }
  //select * from table where field/? = ?

  def sqlModTemplate(sql: String, tmpQuery: SqlFieldsQuery, newPartitionNum: Int): DataFrame = {
    val partitions = IgniteFunctions.buildModePartition(sql, partitionNum, newPartitionNum, tmpQuery)
    template(partitions)
  }

  def sqlTemplate(sql:String,args:Any*):DataFrame={
    sqlTemplate(sql,true,1024,args:_*)
  }

  def sqlTemplate(sql:String,isLazy: Boolean, pageSize: Int,args:Any*):DataFrame={
    val tmpQuery = new SqlFieldsQuery("")
    tmpQuery.setLazy(isLazy)
    tmpQuery.setPageSize(pageSize)
    sqlTemplate(sql,tmpQuery,args:_*)
  }
  //分区查找数据
  def sqlTemplate(sql:String ,tmpQuery:SqlFieldsQuery,args:Any*): DataFrame ={
    val partitions = IgniteFunctions.buildPartition(sql, partitionNum, tmpQuery,args:_*)
    template(partitions)
  }

  def template(partitions: Array[Partition], args: Any*): DataFrame = {
    val schema = buildSchema(ensureCache().query(partitions.head.asInstanceOf[NewIgnitePartition[List[_]]].qry).asInstanceOf[QueryCursorEx[java.util.List[_]]].fieldsMeta())
    val rowRdd = new NewIgniteSqlRDD[Row, java.util.List[_], K, V](
      ic, cacheName, cacheCfg, list ⇒ Row.fromSeq(list), keepBinary, partitions)
    ic.sqlContext.createDataFrame(rowRdd, schema)
  }


}

object IgniteFunctions {
  /**
    * 取模方式构建分区信息
    * @param sql select * from table where field/? = ?
    * @param oldPartitionNum 原始rdd的分区
    * @param tmpQuery 模板
    * @return
    */
  def buildModePartition(sql: String, oldPartitionNum: Int, newPartitionNum: Int, tmpQuery: SqlFieldsQuery): Array[Partition] = {
    val partitionNum = if (newPartitionNum == 0) oldPartitionNum else newPartitionNum
    val partitions = 0 until (partitionNum) map (idx => {
      val qry = tmpQuery.copy() //coyp template query object
      qry.setSql(sql)
      qry.setPartitions(0 until oldPartitionNum: _*) //全分区扫描
      qry.setArgs(partitionNum.asInstanceOf[Object], idx.asInstanceOf[Object])
      NewIgnitePartition(qry, idx).asInstanceOf[Partition]
    })
    partitions.toArray
  }



  /**
    * 原生分区构建分区信息
    * @param sql
    * @param partitionNum 调整的分区大小
    * @param tmpQuery
    * @param args
    * @return
    */
  def buildPartition(sql: String, partitionNum: Int, tmpQuery: SqlFieldsQuery, args: Any*): Array[Partition] = {
    val partitions = 0 until (partitionNum) map (idx => {
      val qry = tmpQuery.copy() //coyp template query object
      qry.setSql(sql)
      qry.setArgs(args.map(_.asInstanceOf[Object]):_*)
      qry.setPartitions(idx)//TODO tang.peng 多表连接时，ignite分区是如何确定的？
      NewIgnitePartition(qry, idx).asInstanceOf[Partition]
    })
    partitions.toArray
  }


  /**
    *
    * @param typeName
    * @param sql
    * @param partitionNum
    * @param tmpQuery
    * @param isPartitioned  是否只查询当前指定分区数据
    * @param args
    * @return
    */
  def buildPartitions[K, V](typeName: String, sql: String, partitionNum: Int, tmpQuery: SqlQuery[K, V], isPartitioned: Boolean, args: Any*): Array[Partition] = {
    val partitions = 0 until partitionNum map (idx => {
      val qry = copyTmp(new SqlQuery[K, V](typeName, sql), tmpQuery)
      qry.setSql(sql)
      if (isPartitioned) {
        qry.setPartitions(idx) //查询指定当前分区查询
      } else {
        qry.setPartitions(0 until (partitionNum): _*) //所有分区查询
      }
      qry.setArgs(args.map(_.asInstanceOf[Object]):_*)
      NewIgnitePartition(qry, idx).asInstanceOf[Partition]
    })
    partitions.toArray
  }
    def copyTmp[K,V](qry:SqlQuery[K,V],tmpQuery:SqlQuery[K,V]): SqlQuery[K,V] ={
      //copy template query object
      qry.setAlias(tmpQuery.getAlias)
      qry.setDistributedJoins(tmpQuery.isDistributedJoins)
      qry.setLocal(tmpQuery.isLocal)
      qry.setPageSize(tmpQuery.getPageSize)
      qry.setReplicatedOnly(tmpQuery.isReplicatedOnly)
      qry.setTimeout(tmpQuery.getTimeout, TimeUnit.MILLISECONDS)
      qry.setType(tmpQuery.getType)
      qry
    }

}

