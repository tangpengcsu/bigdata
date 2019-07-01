package com.szkingdom.fspt.dao.file.dbf

import java.nio.charset.Charset

import com.linuxense.javadbf.DBFOffsetReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.sql.Row

class DBFFunctions(@transient val sc: SparkContext) extends Serializable {

  def loadAsRowRDD(path :String, chasrset: Charset,
                   partitionNum:Int,
                   showDeletedRows:Boolean=false,
                   userName:String="hadoop",
                   connectionTimeout:Int=3000, maxRetries:Int=1): RDD[Row] ={

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    new DBFReaderRDD[Row](sc,path,rowConvert,chasrset.name(),showDeletedRows,userName,connectionTimeout,maxRetries,partitions)
  }
  private[dbf] def rowConvert(reader: DBFOffsetReader, data: Array[AnyRef]): Row = {
   val row= Row.fromSeq(data)
   // println(row)
    row
  }
}
