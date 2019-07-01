package com.szkingdom.fspt.dao.file.dbf

import java.net.URI
import java.nio.charset.Charset

import com.linuxense.javadbf.DBFOffsetReader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._

case class DBFPartition(idx: Int

                       ) extends Partition {
  override def index: Int = idx

}

class DBFReaderRDD[T: ClassTag](sparkContext: SparkContext,
                                path: String,
                                conv: (DBFOffsetReader, Array[AnyRef]) => T,
                                charSet: String,
                                showDeletedRows:Boolean,
                                userName: String,
                                connectTimeout: Int,
                                maxRetries: Int,
                                partitions: Array[Partition]) extends RDD[T](sparkContext, deps = Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = split.asInstanceOf[DBFPartition]


    val conf = SparkHadoopUtil.get.conf
    conf.set("ipc.client.connect.timeout", connectTimeout.toString) //超时时间3S - 3000
    conf.set("ipc.client.connect.max.retries.on.timeouts", maxRetries.toString) // 重试次数1
    val fs = FileSystem.get(URI.create(path), conf, userName)
    val inputStream = fs.open(new Path(path))
    val reader = new DBFOffsetReader(inputStream,Charset.forName(charSet))
    val recoderCount = reader.getRecordCount

    val result: mutable.ListBuffer[T] = ListBuffer()
    val (startOffset, endOffset) = DBFReaderRDD.calcOffset(recoderCount, partition.idx, partitions.size)
   // println(s"Idx:${partition.idx}=${startOffset}-${endOffset}")
    if (recoderCount != 0 || startOffset == endOffset) {
      reader.setStartOffset(startOffset)
      reader.partitionIdx = partition.idx

      reader.setEndOffset(endOffset)
      if(startOffset==12&& endOffset==12){
        println("12")
      }

      var rowObjects: Array[AnyRef] = null

      breakable {
        while (true) {
          rowObjects = reader.nextRecord()
          if (rowObjects == null) {
            break()
          }
          breakable {

            if (rowObjects.length == 0) {
              break
            }
            else {
              val cd = conv(reader, rowObjects)
              cd
              result += (cd)
            }
          }
        }
      }

    }

    IOUtils.closeQuietly(reader)
    IOUtils.closeQuietly(inputStream)
    IOUtils.closeQuietly(fs)
    result.iterator

  }


  override protected def getPartitions: Array[Partition] = partitions
}


object DBFReaderRDD {
  def load(): Unit = {

  }



  def calcOffset(recordCount: Int,
                 idx: Int,
                 partitionNum: Int): (Int, Int) = {
    val ptnRecordCount = recordCount / partitionNum
    val mode = recordCount % partitionNum
    val (offset, stepSize) = if (ptnRecordCount == 0) {
      if (idx < mode) {
        (idx, 1)
      } else {
        (mode, 0)
      }
    } else {
      if (idx < mode) {
        (idx * (ptnRecordCount + 1), (ptnRecordCount + 1))
      } else {
        (mode * (ptnRecordCount + 1) + (idx - mode) * ptnRecordCount, ptnRecordCount)
      }

    }
    (offset, offset + stepSize)
  }


}