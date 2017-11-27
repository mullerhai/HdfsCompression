package com.linkedme.hdfs

import java.io.File

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil}

/**
  * Created by linkedmemuller on 26/11/2017.
  */
class HdfsFileUntar {

}
object HdfsFileUntar{

  def main(args: Array[String]): Unit = {

    if(args(0).isEmpty||args(1).isEmpty){

      return
    }
    val infile:File=new File(args(0))
    val outDir:File=new File(args(1))

    println("要解压文件 "+args(0)+"解压后的目录"+args(1))
    val conf=new Configuration()
    val fs =FileSystem.get(conf)
    try{
      println("开始 解压缩")
      FileUtil.unTar(infile,outDir)
      println("解压缩完成")
    }catch {
      case  e:Exception =>e.printStackTrace()
    }




  }
}
