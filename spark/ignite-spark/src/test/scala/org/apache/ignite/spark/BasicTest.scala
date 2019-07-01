package org.apache.ignite.spark

import java.io.FileInputStream

import com.linuxense.javadbf.{DBFField, DBFReader, DBFRow, DBFUtils}
import org.scalatest.FunSuite
import scala.util.control.Breaks._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class BasicTest extends FunSuite {
  test("") {
    val result: mutable.ListBuffer[String] = ListBuffer()
    result += ("fjksa")
    result += ("2")
    result.foreach(println(_))
  }
  var path = ""
  test("jkfsaf;") {
    val filePath = "D://"
    path = filePath+"0904机构费用明细.dbf"
    path = filePath+"0904交易一级清算表.DBF"
    path = filePath+"SJSJG.DBF"
 /*   path = filePath+"0904保证金日结表.DBF"
    path = filePath+"0904机构费用明细导出.dbf"*/
    val reader: DBFReader = new DBFReader(new FileInputStream(path))



    // get the field count if you want for some reasons like the following

    val numberOfFields = reader.getFieldCount()
    for (elem <- 0 until (numberOfFields)) {
      val f = reader.getField(elem)
      println(s"${f.getName}:${f.getType}:${f.getLength}")
    }


    // Now, lets us start reading the rows

    var row: DBFRow = null
    breakable {
      while (true) {
        row = reader.nextRow()

        if (row == null) {
          break()
        }
        println(row)
      }
    }

    DBFUtils.close(reader)


  }
}
