package com.linuxense.javadbf.spark

import java.net.URI
import java.nio.charset.Charset

import scala.reflect.runtime.{universe => ru}
import com.linuxense.javadbf.{DBFField, DBFOffsetReader, DBFRow, DBFSkipRow}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._

case class DBFPartition(idx: Int) extends Partition {
  override def index: Int = idx

}

class DBFReaderRDD[T: ClassTag, V <: DBFParam](sparkContext: SparkContext,
                                               path: String,
                                               conv: (Int, Array[DBFField], DBFRow, List[V], ru.RuntimeMirror, ru.ClassMirror, ru.MethodMirror, Iterable[ru.TermSymbol]) => T,
                                               charSet: String,
                                               showDeletedRows: Boolean,
                                               userName: String,
                                               connectTimeout: Int,
                                               maxRetries: Int,
                                               partitions: Array[Partition],
                                               param: List[V] = Nil,
                                               clazz: Class[_],
                                               adjustFields: (Array[DBFField]) => Unit) extends RDD[T](sparkContext, deps = Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = split.asInstanceOf[DBFPartition]

    var fs: FileSystem = null
    var inputStream: FSDataInputStream = null
    var reader: DBFOffsetReader = null
    val conf = SparkHadoopUtil.get.conf
    conf.set("ipc.client.connect.timeout", connectTimeout.toString) //超时时间3S - 3000
    conf.set("ipc.client.connect.max.retries.on.timeouts", maxRetries.toString) // 重试次数1
    try {
      fs = FileSystem.get(URI.create(path), conf, userName)

      inputStream = fs.open(new Path(path))
      reader = new DBFOffsetReader(inputStream, Charset.forName(charSet), showDeletedRows)

      adjustFields(reader.getFields())
      val recoderCount = reader.getRecordCount

      val result: mutable.ListBuffer[T] = ListBuffer()
      val (startOffset, endOffset) = DBFReaderRDD.calcOffset(recoderCount, partition.idx, partitions.size)
      if (recoderCount != 0 && startOffset != endOffset) {
        reader.setStartOffset(startOffset)
        reader.partitionIdx = partition.idx

        reader.setEndOffset(endOffset)
        val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像
        val classMirror = runtimeMirror.reflectClass(runtimeMirror.classSymbol(clazz))
        val typeSignature = classMirror.symbol.typeSignature


        val constructorSymbol = typeSignature.decl(ru.termNames.CONSTRUCTOR)
          .filter(i => i.asMethod.paramLists.flatMap(_.iterator).isEmpty)
          .asMethod
        val constructorMethod = classMirror.reflectConstructor(constructorSymbol)
        val reflectFields = typeSignature
          .decls.
          filter(i => i.isTerm && i.asTerm.isVar)
          .map(i => i.asTerm)

        var dbfRow: DBFRow = null

        breakable {
          while (true) {
            dbfRow = reader.nextRow()
            if (dbfRow == null) {
              break()
            }
            breakable {
              if (dbfRow.isInstanceOf[DBFSkipRow]) {
                break
              }
              else {
                val data = conv(reader.getCurrentOffset, reader.getFields, dbfRow, param, runtimeMirror, classMirror, constructorMethod, reflectFields)
                result += (data)
              }
            }
          }
        }

      }

      result.iterator
    } finally {
      IOUtils.closeQuietly(reader)
      IOUtils.closeQuietly(inputStream)
      IOUtils.closeQuietly(fs)
    }


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