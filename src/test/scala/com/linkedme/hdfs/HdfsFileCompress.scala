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
  val HELP_MEG=""
  val   GZIP="org.apache.hadoop.io.compress.GzipCodec"
  val loop=new Breaks;


  def main(args: Array[String]): Unit = {
    println("helo world")
    if (args!=null &&args.length<2){
      return

    }
    var in=""
    var out=""
    val conf=new Configuration()
    if(!StringUtils.startsWithIgnoreCase(args(0),HDFS_SCHEMA)){
      in=FS+args(0)
    }
    if(!StringUtils.startsWithIgnoreCase(args(1),HDFS_SCHEMA)){
      out=FS+args(1)
    }
    compress(in,out,conf)
  }
  def compress(inputPath:String,outputPath:String,conf:Configuration):Unit={
    if(StringUtils.isBlank(inputPath)||StringUtils.isBlank(outputPath)){
      return
    }
    conf.set("fs.defaultFS",FS)
    try{
      val fs:FileSystem=FileSystem.get(conf)
      val input:Path=new Path(inputPath)
      if (fs.exists(input)){
        return
      }
      if(!StringUtils.endsWith(outputPath,"/")){
        return

      }
      val stat:FileStatus=fs.getFileStatus(input)
      if(stat.isFile){
        write(fs,conf,stat,outputPath)
      }else if(stat.isDirectory){
        val subinputFile:Array[FileStatus]=fs.listStatus(input)
        for (fileStatus:FileStatus<-subinputFile){
          if(fileStatus.isFile){
            write(fs,conf,fileStatus,outputPath)
          }else{
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

    val parent:Path=new Path(out).getParent
    if(fs.exists(parent)){
      fs.mkdirs(parent,FsPermission.getDirDefault)
    }

  }

  def  writeFile(fs :FileSystem,conf:Configuration,in:String,out:String):Unit={

    var inputStream:FSDataInputStream=null
    var writer:SequenceFile.Writer=null
    try{
      inputStream=fs.open(new Path(in))
      val seqFile=new Path(out)
      val buff:BufferedReader=new BufferedReader(new InputStreamReader(inputStream))
      var EMPTY_KEY:BytesWritable=new BytesWritable()
      val codec:CompressionCodec= ReflectionUtils.newInstance(Class.forName(GZIP),conf).asInstanceOf[CompressionCodec]
      writer=SequenceFile.createWriter(conf,SequenceFile.Writer.file(seqFile),
        SequenceFile.Writer.keyClass(classOf[BytesWritable]),
        SequenceFile.Writer.valueClass(classOf[Text]),
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,codec))

      var str=""
      while ((str=buff.readLine())!=null){
        writer.append(EMPTY_KEY,new Text(str))
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      IOUtils.closeStream(inputStream)
      IOUtils.closeStream(writer)
    }
  }
}
