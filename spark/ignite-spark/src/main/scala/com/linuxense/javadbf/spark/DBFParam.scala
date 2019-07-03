package com.linuxense.javadbf.spark

import org.apache.spark.sql.types.DataType

sealed trait DBFParam {

}

case class DBFOptParam(orderSn: Int, value: AnyRef) extends DBFParam

case class DBFOptDFParam(name: String, value: Any, isCounter: Boolean = false) extends DBFParam
