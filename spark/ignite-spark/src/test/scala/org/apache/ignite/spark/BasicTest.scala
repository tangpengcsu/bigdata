package org.apache.ignite.spark

import java.io.FileInputStream

import com.linuxense.javadbf.{DBFField, DBFReader, DBFRow, DBFUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BasicTest extends FunSuite {
  test("1111") {
    println(classOf[Int].getSimpleName)
    println(classOf[Long].getSimpleName)
    println(classOf[Short].getSimpleName)
    println(classOf[Char].getSimpleName)
    println(classOf[Float].getSimpleName)
    println(classOf[String].getSimpleName)
    println(classOf[Double].getSimpleName)
    println(classOf[BigDecimal].getSimpleName)
    println(classOf[java.math.BigDecimal].getSimpleName)
    println(classOf[java.lang.Float].getSimpleName)
    println(classOf[java.lang.Double].getSimpleName)
    println(classOf[java.lang.Short].getSimpleName)
    println(classOf[java.lang.Character].getSimpleName)
    println(classOf[java.lang.String].getSimpleName)
    println(classOf[java.lang.Integer].getSimpleName)
    println(classOf[java.lang.Long].getSimpleName)
  }


  var path = ""
  test("jkfsaf;") {
    val filePath = "D://"
    path = filePath+"0904机构费用明细.dbf"
/*    path = filePath+"0904交易一级清算表.DBF"
    path = filePath+"SJSJG.DBF"*/
    path = filePath+"0904保证金日结表.DBF"
//    path = filePath+"0904机构费用明细导出.dbf"
    val reader: DBFReader = new DBFReader(new FileInputStream(path),true)



    println(s"count:${reader.getRecordCount}")
    // get the field count if you want for some reasons like the following

    val numberOfFields = reader.getFieldCount()
    for (elem <- 0 until (numberOfFields)) {
      val f = reader.getField(elem)
      println(s"${f.getName}:${f.getType}:${f.getLength}")
    }


    // Now, lets us start reading the rows

    var row: DBFRow = null
    var i =0
    breakable {
      while (true) {
        row = reader.nextRow()

        if (row == null) {
          break()
        }
        i=i+1
        println(row)
      }
    }
    println(s"read count:${i}")

    DBFUtils.close(reader)


  }
}
