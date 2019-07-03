package com.linuxense.javadbf.spark

import scala.reflect.ClassTag

object Utils {

  def getClazz[T]()(implicit m: ClassTag[T]): Class[T] = {
    m.runtimeClass.asInstanceOf[Class[T]]
  }

}
