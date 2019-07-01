package com.szkingdom.fspt.dao.file

import com.szkingdom.fspt.dao.file.dbf.{DBFFunctions}
import org.apache.spark.SparkContext


package object spark {
  implicit def toSparkContextFunctions(sc: SparkContext):DBFFunctions  = {

    new DBFFunctions(sc)
  }
}


