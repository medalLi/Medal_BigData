package com.tuning

/**
  * @author medal
  * @create 2019-07-20 22:57
  **/
/*
*  针对两张表的join操作
*       一张表正常，一张表个别key异常
*
* */
object _02SplitKeyExtendOps {
  def main(args: Array[String]): Unit = {
    //1.采样找到异常的key
    //2.根据异常的key将左右表都拆分正常的数据和异常的数据
    //3.对左表异常数据添加N以内的随机前缀
    //4.对右表异常数据进行N倍的扩容
    //5.分别对异常数据和正常数据进行join操作
    //6.全局的union操作，前提是去掉5中的异常数据的前缀
  }
}
