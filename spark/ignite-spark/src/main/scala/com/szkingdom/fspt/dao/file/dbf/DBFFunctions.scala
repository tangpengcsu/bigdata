package com.szkingdom.fspt.dao.file.dbf

import java.nio.charset.Charset

import com.linuxense.javadbf.{DBFField}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.sql.Row

import scala.collection.mutable.ArrayBuffer

class DBFFunctions(@transient val sc: SparkContext) extends Serializable {

  def loadAsRowRDD(path :String, chasrset: Charset,
                   partitionNum:Int,
                   param:List[DBFOptParam]=Nil,
                   showDeletedRows:Boolean=false,
                   userName:String="hadoop",
                   connectionTimeout:Int=3000, maxRetries:Int=1): RDD[Row] ={

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    new DBFReaderRDD[Row,DBFOptParam](sc,path,rowConvert,chasrset.name(),showDeletedRows,userName,connectionTimeout,maxRetries,partitions,param,defaultAdjustLength)
  }

  def adjustJGMX(fields :Array[DBFField]): Unit ={
    val opt =fields.find(_.getName=="BY3")
    if(opt.isDefined){
      val f = opt.get
      val idx = fields.indexOf(f)
      f.setLength(50)
      fields(idx)= f
    }
  }

  def defaultAdjustLength(fields:Array[DBFField])={

  }
  private[dbf] def rowConvert(offset:Int,fields: Array[DBFField], data: Array[AnyRef],dBFOptParam:List[DBFOptParam]): Row = {

    val arrayBuffer = ArrayBuffer[AnyRef](offset.asInstanceOf[AnyRef])

    dBFOptParam.sortBy(_.orderSn).foreach(i=>{
      arrayBuffer += i.data
    })
    arrayBuffer++=data
   val row= Row.fromSeq(arrayBuffer)
   // println(row)
    row
  }
}

