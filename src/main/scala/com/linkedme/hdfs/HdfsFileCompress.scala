package com.linkedme.hdfs

/**
  * Created by linkedmemuller on 26/11/2017.
  */
import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{BytesWritable, IOUtils, SequenceFile, Text}
import org.apache.hadoop.util.ReflectionUtils

import scala.util.control.Breaks

/**
  * Created by linkedmemuller on 25/11/2017.
  */
class HdfsFileCompress {

}

object HdfsFileCompress{


  val GIZP_SUFFIX=".gizp"
  val FS="hdfs://192.168.255.161:9000"
  val HDFS_SCHEMA="hdfs://"
 // val FS=""
 // val HDFS_SCHEMA=""
  val HELP_MEG=""
  val   GZIP="org.apache.hadoop.io.compress.GzipCodec"
  val loop=new Breaks;


  def main(args: Array[String]): Unit = {
    println("helo world")
    if (args!=null &&args.length<2){
      return

    }
    var in="./input"
    var out="./output"
    val conf=new Configuration()
    if(!StringUtils.startsWithIgnoreCase(args(0),HDFS_SCHEMA)){
      in=FS+args(0)
    }
    if(!StringUtils.startsWithIgnoreCase(args(1),HDFS_SCHEMA)){
      out=FS+args(1)
    }
    println("begin compress")
    compress(in,out,conf)
  }
  def compress(inputPath:String,outputPath:String,conf:Configuration):Unit={
    if(StringUtils.isBlank(inputPath)||StringUtils.isBlank(outputPath)){
     println("输入路径和输出路径为空"+inputPath+outputPath)
      return
    }
    conf.set("fs.defaultFS",FS)
    println("join compress mothod")
    try{
      val fs:FileSystem=FileSystem.get(conf)
      val input:Path=new Path(inputPath)
      if (!fs.exists(input)){
        println("fs.existsinput")
        return
      }
      if(!StringUtils.endsWith(outputPath,"/")){
        println("endswith outputPath,\"/\"")
        return

      }
      val stat:FileStatus=fs.getFileStatus(input)
      if(stat.isFile){
        println("开始写压缩文件")
        write(fs,conf,stat,outputPath)
      }else if(stat.isDirectory){
        println("shi 文件夹 向下找文件")
        val subinputFile:Array[FileStatus]=fs.listStatus(input)
        for (fileStatus <-subinputFile.toList){
          if(fileStatus.isFile){
            println("从文件夹里 找到文件 开始 写压缩文件")
            write(fs,conf,fileStatus,outputPath)
          }else{
            println("break 退出")
            loop.break()
          }
        }
      }
    }catch {
      case e:Exception =>e.printStackTrace()
    }
  }



  def  write(fs: FileSystem,conf:Configuration,fileStatus:FileStatus,outpath:String):Unit={

    val in =fileStatus.getPath.toString
    val out=outpath+fileStatus.getPath.getName+GIZP_SUFFIX
    checkParentDir(fs,out)
    writeFile(fs,conf,in,out)

  }

  def checkParentDir(fs:FileSystem,out:String):Unit={

    println("检查 负极文件夹")
    val parent:Path=new Path(out).getParent
    if(fs.exists(parent)){
      println("创建文件夹")
      fs.mkdirs(parent,FsPermission.getDirDefault)
    }

  }


  def  writeFile(fs :FileSystem,conf:Configuration,in:String,out:String):Unit={

    var inputStream:FSDataInputStream=null
    var writer:SequenceFile.Writer=null
    try{
      println("开始写文件了，做好准备具体写文件")
      if(in.contains("biz.log")||in.contains("info.log")||in.contains("ad_status.log")||in.contains("ad_behavior.log ")) {
        inputStream = fs.open(new Path(in))
        val seqFile = new Path(out)
        val buff: BufferedReader = new BufferedReader(new InputStreamReader(inputStream))
        val EMPTY_KEY: BytesWritable = new BytesWritable()
        val codec: CompressionCodec = ReflectionUtils.newInstance(Class.forName(GZIP), conf).asInstanceOf[CompressionCodec]
        writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(seqFile),
          SequenceFile.Writer.keyClass(classOf[BytesWritable]),
          SequenceFile.Writer.valueClass(classOf[Text]),
          SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, codec))
        println("把文件" + in + "好像写完到了" + out)
        var str = ""
        while ((str = buff.readLine()) != null) {
          writer.append(EMPTY_KEY, new Text(str))

        }
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      println("关闭文件流")

      IOUtils.closeStream(inputStream)
      IOUtils.closeStream(writer)
    }
  }
}
