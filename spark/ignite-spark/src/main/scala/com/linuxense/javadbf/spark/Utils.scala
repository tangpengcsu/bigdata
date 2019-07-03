package com.linuxense.javadbf.spark

import com.linuxense.javadbf.DBFRow

import scala.reflect.ClassTag

object Utils {

  def getClazz[T]()(implicit m: ClassTag[T]): Class[T] = {
    m.runtimeClass.asInstanceOf[Class[T]]
  }

  def transData(fieldName:String,dataType:String,data: DBFRow): Any ={

    dataType match {
      case "String"=> data.getString(fieldName)
      case "BigDecimal"=> BigDecimal(data.getBigDecimal(fieldName))//只能反射 scala.math.BigDecimal，不能反射 java.math.BigDecimal
      case "Int"|"Integer"=>data.getInt(fieldName)
      case "Short"=>data.getInt(fieldName).toShort
      case "Long"=>data.getLong(fieldName)
      case "Float"=>data.getFloat(fieldName)
      case "Double"=>data.getDouble(fieldName)
      case "Char"|"Character"=> data.getString(fieldName).charAt(0)
      case _=> throw new IllegalArgumentException(s"不支持改反射数据类型：${fieldName},${dataType}")

    }
  }


}
