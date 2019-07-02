package com.szkingdom.fspt.dao.file.dbf

sealed trait DBFParam {

}
case class DBFOptParam(orderSn:Int, data:AnyRef) extends DBFParam {

}
