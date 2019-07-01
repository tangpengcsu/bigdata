package com.szkingdom.fspt.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object ignite {
    def getOrcreateSparkContxt(appName:String="test"): SparkSession ={
      val conf = new SparkConf()
        .setAppName(appName)
    /*    .setMaster("local[2]")
        .set("spark.sql.shuffle.partitions","35")
        .set("spark.executor.instances", "2")*/

      val sparkSession:SparkSession= SparkSession.builder().config(conf).getOrCreate()
      sparkSession
    }
}
