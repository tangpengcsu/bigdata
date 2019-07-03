package com.linuxense.javadbf.spark

import java.nio.charset.Charset

import com.linuxense.javadbf.{DBFField, DBFFieldNotFoundException, DBFRow}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext}

import scala.reflect.runtime.{universe => ru}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import com.linuxense.javadbf.spark.Utils._

class DBFFunctions(@transient val sc: SparkContext) extends Serializable {

  def loadAsRowRDD(path: String, chasrset: Charset,
                   partitionNum: Int,
                   param: List[DBFOptParam] = Nil,
                   showDeletedRows: Boolean = false,
                   userName: String = "hadoop",
                   connectionTimeout: Int = 3000, maxRetries: Int = 1): RDD[Row] = {

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    new DBFReaderRDD[Row, DBFOptParam](sc, path, rowConvert, chasrset.name(), showDeletedRows, userName, connectionTimeout, maxRetries, partitions, param, Row.getClass, defaultAdjustLength)
  }

  def loadAsBeanRDD[T: ClassTag](path: String,
                                 chasrset: Charset,
                                 partitionNum: Int,
                                 param: List[DBFOptDFParam] = Nil,
                                 showDeletedRows: Boolean = false,
                                 userName: String = "hadoop",
                                 connectionTimeout: Int = 3000, maxRetries: Int = 1): RDD[T] = {

    val partitions = 0 until (partitionNum) map {
      DBFPartition(_).asInstanceOf[Partition]
    } toArray

    val clazz = getClazz[T]()

    new DBFReaderRDD[Any, DBFOptDFParam](sc, path, newBeanConvert, chasrset.name(), showDeletedRows, userName, connectionTimeout, maxRetries, partitions, param, clazz, defaultAdjustLength).asInstanceOf[RDD[T]]
  }


  def adjustJGMX(fields: Array[DBFField]): Unit = {
    val opt = fields.find(_.getName == "BY3")
    if (opt.isDefined) {
      val f = opt.get
      val idx = fields.indexOf(f)
      f.setLength(50)
      fields(idx) = f
    }
  }

  def defaultAdjustLength(fields: Array[DBFField]) = {

  }

  private[spark] def rowConvert(offset: Int, fields: Array[DBFField], data: DBFRow, dBFOptParam: List[DBFOptParam],
                                runTimeMirror: ru.RuntimeMirror,
                                classMirror: ru.ClassMirror,
                                constructor: ru.MethodMirror,
                                classFields: Iterable[ru.TermSymbol]
                               ): Row = {

    val arrayBuffer = ArrayBuffer[Any](offset)

    dBFOptParam.sortBy(_.orderSn).foreach(i => {
      arrayBuffer += i.data
    })
    for (idx <- 0 until (fields.length)) {
      arrayBuffer += data.getObject(idx)
    }

    val row = Row.fromSeq(arrayBuffer)
    row
  }

  def newBeanConvert(offset: Int,
                     fields: Array[DBFField],
                     data: DBFRow,
                     dBFOptParam: List[DBFOptDFParam],
                     runTimeMirror: ru.RuntimeMirror,
                     classMirror: ru.ClassMirror,
                     constructor: ru.MethodMirror,
                     classFields: Iterable[ru.TermSymbol]
                    ): Any = {


    val instance = constructor()
    val ref = runTimeMirror.reflect(instance)
    classFields.foreach(i => {
      try {
        val d = data.getString(i.name.toString.trim)

        // i.accessed
        val fm = ref.reflectField(i)
        fm.set(d)
        // println(fm.get)
      } catch {
        case e: DBFFieldNotFoundException =>
          println(s"误匹配字段：${i.name.toString.trim}")
      }
    })
    instance

  }

  private[spark] def beanConvert(offset: Int, fields: Array[DBFField], data: DBFRow, dBFOptParam: List[DBFOptDFParam], className: Class[_]): Any = {

    val classMirror = ru.runtimeMirror(getClass.getClassLoader) //获取运行时类镜像
    val classSymbol = classMirror.classSymbol(className)
    val reflectClass = classMirror.reflectClass(classSymbol)
    val typeSignature = reflectClass.symbol.typeSignature


    // val ctorC = typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
    val cons = typeSignature.decl(ru.termNames.CONSTRUCTOR)
      .filter(i => i.asMethod.paramLists.flatMap(_.iterator).isEmpty)
      .asMethod
    val ctorm = reflectClass.reflectConstructor(cons)
    ctorm
    val instance = ctorm()

    val vVal = typeSignature
      .decls.
      filter(i => i.isTerm && i.asTerm.isVar)
      .map(i => i.asTerm)
    val ref = classMirror.reflect(instance)

    vVal.foreach(i => {
      try {
        val d = data.getString(i.name.toString.trim)

        // i.accessed
        val fm = ref.reflectField(i)
        fm.set(d)
        // println(fm.get)
      } catch {
        case e: DBFFieldNotFoundException =>
          println(s"误匹配字段：${i.name.toString.trim}")
      }
    })
    instance
  }
}

