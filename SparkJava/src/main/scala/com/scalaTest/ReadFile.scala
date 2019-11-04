package com.scalaTest

import scala.io.Source

/**
  * @author medal
  * @create 2019-07-29 20:46
  **/
object ReadFile {
  def main(args: Array[String]): Unit = {
    lazy val file = Source.fromFile("E:\\WorkSpaces\\Workspace--idea\\BigData\\SparkJava\\src\\main\\spark.log")
    for(f <- file.getLines()){
      println(f)
    }
  }
}
