package com.linuxense.javadbf.spark

sealed trait DBFParam {

}
case class DBFOptParam(orderSn:Int, data:AnyRef) extends DBFParam {

}
