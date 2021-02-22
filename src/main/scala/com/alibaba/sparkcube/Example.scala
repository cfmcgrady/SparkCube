// scalastyle:off
//package com.alibaba.sparkcube
//
//import java.io.File
//
//import cloud.fchen.spark.utils.IdeaUtil
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.spark_project.jetty.server.handler.ContextHandler
//
//import scala.reflect.io.Directory
//
///**
// * @time 2020/12/1 7:12 下午
// * @author fchen <cloud.chenfu@gmail.com>
// */
//object Example {
//
//  def main(args: Array[String]): Unit = {
//
////    classOf[ContextHandler].getClass.getMethods
////      .filter(m => m.getName == "setContextPath")
////      .foreach(println)
//    run1
//  }
//  def run() {
//    val uuid = this.getClass.getSimpleName.replaceAll("\\$", "")
//    val classpathTempDir = s"/tmp/aaa/$uuid"
//    val util = new IdeaUtil(
//      None,
//      Option(classpathTempDir),
//      dependenciesInHDFSPath = s"libs/$uuid",
//      principal = Option("chenfu@CDH.HOST.DXY"),
//      keytab = Option("/Users/fchen/tmp/hadoop/chenfu.keytab")
//    )
//    util.setup()
//    val conf = new SparkConf()
//      .setMaster("yarn-client")
//      .set("spark.yarn.archive", "hdfs:///user/chenfu/libs/spark-2.4.3-bin-hadoop2.7.jar.zip")
//      .set("spark.repl.class.outputDir", classpathTempDir)
//      .set("spark.yarn.keytab", "/Users/fchen/tmp/hadoop/chenfu.keytab")
//      .set("spark.yarn.principal", "chenfu@CDH.HOST.DXY")
//      .set("spark.sql.cache.useDatabase", "default")
//      .set("spark.sql.extensions", "com.alibaba.sparkcube.SparkCube")
//      .set("spark.sql.cache.tab.display", "true")
////      .set("spark.driver.extraClassPath", "/Users/fchen/Project/bigdata/SparkCube/target/sparkcube-0.4.0-SNAPSHOT.jar")
////      .set("spark.extraListeners", "org.apache.spark.plugins.ExecutorPluginsListener")
//    conf.getAll.foreach(println)
//
//    val spark = SparkSession
//      .builder()
//      .appName("Spark Plugins Example")
//      .config(conf)
//      .enableHiveSupport()
//      .getOrCreate()
//    spark.sql("show tables").show(truncate = false)
//    spark.sql("select * from odl_mail_system_edm_task").show
//
//
//    Thread.sleep(Int.MaxValue)
//  }
//
//  def run1(): Unit = {
//    delete()
//    val spark = SparkSession
//      .builder()
//      .appName("Spark Plugins Example")
//      .master("local[*]")
//      .config("spark.sql.cache.useDatabase", "default")
//      .config("spark.sql.extensions", "com.alibaba.sparkcube.SparkCube")
//      .config("spark.sql.cache.tab.display", "true")
////      .config("spark.sql.codegen.wholeStage", "false")
//      .getOrCreate()
//
//    spark.udf.register("mock_col", (i: Int) => {
////      if (i < 10) {
////        "a"
////      } else if (i < 20) {
////        "b"
////      } else {
////        "c"
////      }
//      if (i % 3 == 0) {
//        "a"
//      } else {
//        "b"
//      }
//    })
//
//    spark.range(1 , 100)
//      .selectExpr("id", "id as value", "mock_col(id) as col_1", "id % 3 as pid")
//      .write
//      .partitionBy("pid")
//      .saveAsTable("test_tb")
//
//    val t = spark.table("test_tb")
//    t.show()
//    Thread.sleep(Int.MaxValue)
//  }
//
//  def delete(): Unit = {
//    val directory = new Directory(new File("spark-warehouse"))
//    directory.deleteRecursively()
//  }
//
//}
