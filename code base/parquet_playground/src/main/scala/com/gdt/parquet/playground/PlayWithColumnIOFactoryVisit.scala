package com.gdt.parquet.playground

import com.gdt.parquet.playground.util.TempFileUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Copyright 2015 Tencent Inc.
 * @author lwlin <lwlin@tencent.com>
 *
 *         在 SparkSQL 1.5.1 里，同时开启 mergeSchema 和 filterPushdown 时，可能导致报错
 *         本类复现了这个错误
 *
 *         在 SparkSQL 1.5.2 及以后，当开启 mergeSchema 时，则强制关闭 filterPushdown
 *
 *         相关 JIRA
 *         https://issues.apache.org/jira/browse/PARQUET-389
 *         https://issues.apache.org/jira/browse/SPARK-11103
 *         https://issues.apache.org/jira/browse/SPARK-11434
 */
object PlayWithColumnIOFactoryVisit extends TempFileUtil {

  def main(args: Array[String]) {
    val tempPath = tempDir(this.getClass.getSimpleName)

    val conf = new SparkConf()
    conf.setAppName(this.getClass.getSimpleName)
    conf.setMaster("local[2]")

    /* Filter pushdown */
    conf.set("spark.sql.parquet.filterPushdown", true.toString)

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val pathOne = tempPath.toString + "/table1"
    val pathTwo = tempPath.toString + "/table2"
    sc.parallelize(1 to 3, 1).map(i => (i, i.toString)).toDF("a", "b")
      .write.parquet(pathOne)
    sc.parallelize(1 to 3, 1).map(i => (i, i.toString)).toDF("c", "b")
      .write.parquet(pathTwo)

    sqlContext.read.option("mergeSchema", "true").parquet(pathOne)//, pathTwo)
      .filter("a = 1")
      .selectExpr("b") // 注释掉此行将会复现错误
      // .selectExpr("c", "b", "a") // 注释掉此行将会复现错误
      .show()
  }
}