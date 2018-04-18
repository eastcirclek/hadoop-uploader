package com.skt.data

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

case class Config(inputFilePath: String = "",
                  outputDirPath: String = "")

object Extractor {
  val log = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("extractor") {
      head("extractor")

      opt[String]("inputFilePath")
        .required()
        .action((x,c) => c.copy(inputFilePath = x))
        .validate{
          x => if (x.startsWith("hdfs://")) {
            success
          } else {
            failure("inputPath should start with hdfs:// but " + x)
          }
        }
        .text("Input path on HDFS")

      opt[String]("outputDirPath")
        .required()
        .action((x,c) => c.copy(outputDirPath = x))
        .validate{
          x => if (x.startsWith("hdfs://")) {
            success
          } else {
            failure("outputPath should start with hdfs:// but " + x)
          }
        }
        .text("Input path on HDFS")
    }

    val config = parser.parse(args, Config()) match {
      case Some(config) => config
      case None => throw new RuntimeException("Failed to get a valid configuration object")
    }

    import config._

    val conf = new SparkConf().setAppName("Extractor")
    val sc = new SparkContext(conf)

    sc.sequenceFile(inputFilePath, classOf[Text], classOf[BytesWritable])
      .foreachPartition { iter =>
        val conf = new Configuration()
        val fs = FileSystem.get(URI.create(outputDirPath), conf)

        iter.foreach { case (key, value) => {
          val destPath = new Path(outputDirPath, key.toString)
          val outputStream = fs.create(destPath)
          outputStream.write(value.getBytes, 0, value.getLength)
          outputStream.close()
        }}
      }
  }
}
