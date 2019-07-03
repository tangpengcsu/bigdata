package com.linuxense.javadbf.spark

import com.linuxense.javadbf.{DBFField, DBFFieldNotFoundException, DBFRow}
import org.apache.spark.sql.Row
import com.linuxense.javadbf.spark.Utils._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}
sealed trait DBFDataHandler[V <: DBFParam] {
    def process[T](offset: Int,
                   fields: Array[DBFField],
                   data: DBFRow,
                   dBFOptParam: List[V],
                   runTimeMirror: ru.RuntimeMirror,
                   classMirror: ru.ClassMirror,
                   constructor: ru.MethodMirror,
                   classFields: Iterable[ru.TermSymbol]
               ):T
}

object DBFRowHandler extends DBFDataHandler[DBFOptParam]{
  override def process[T](offset: Int, fields: Array[DBFField], data: DBFRow, dBFOptParam: List[DBFOptParam], runTimeMirror: ru.RuntimeMirror, classMirror: ru.ClassMirror, constructor: ru.MethodMirror, classFields: Iterable[ru.TermSymbol]): T = {
    val arrayBuffer = ArrayBuffer[Any](offset)

    dBFOptParam.sortBy(_.orderSn).foreach(i => {
      arrayBuffer += i.data
    })
    for (idx <- 0 until (fields.length)) {
      arrayBuffer += data.getObject(idx)
    }

    val row = Row.fromSeq(arrayBuffer)
    row.asInstanceOf[T]
  }
}

object DBFBeanHandler extends DBFDataHandler[DBFOptDFParam]{
  override def process[T](offset: Int,
                          fields: Array[DBFField],
                          data: DBFRow,
                          dBFOptParam: List[DBFOptDFParam],
                          runTimeMirror: ru.RuntimeMirror,
                          classMirror: ru.ClassMirror,
                          constructor: ru.MethodMirror,
                          classFields: Iterable[ru.TermSymbol]): T = {
    val instance = constructor()
    val ref = runTimeMirror.reflect(instance)
    classFields.foreach(i => {
      try {
        val fm = ref.reflectField(i)
        //先取注解ming，后取字段名
        val annOpt = fm.symbol.annotations.find(_.tree.tpe=:=ru.typeOf[DBFFieldProp])
        val fieldName = if (annOpt.isDefined) {
            getAnnotationData(annOpt.get.tree).name
        } else {
          fm.symbol.name.decodedName.toString.trim
         }
        val d = transData(fieldName, fm.symbol.typeSignature.typeSymbol.name.decodedName.toString, data)

        fm.set(d)
      } catch {
        case e: DBFFieldNotFoundException =>
          println(s"误匹配字段：${i.name.toString.trim}")
      }
    })
    instance.asInstanceOf[T]
  }

}



