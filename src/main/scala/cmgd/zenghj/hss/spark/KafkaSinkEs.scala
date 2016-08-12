package cmgd.zenghj.hss.spark

import java.io.File

import cmgd.zenghj.hss.spark.common.CommonUtils._
import cmgd.zenghj.hss.spark.es.EsUtils._
import com.twitter.chill.KryoInjection

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.elasticsearch.spark._

import scala.util.{Failure, Success, Try}

/**
  * Created by cookeem on 16/8/2.
  */
object KafkaSinkEs extends App {
  val conf = new SparkConf().setMaster(configSparkMaster).setAppName(configSparkAppName)
  val hssLibFile = new File("hss-lib")
  val hssLibPath = hssLibFile.getAbsolutePath
  //通过设置executor的classpath保证executor上也可以执行高版本KryoInjection
  conf.set("spark.executor.extraClassPath", s"$hssLibPath/*")
  conf.set("spark.executor.extraLibraryPath", s"$hssLibPath/*")
  conf.set("es.nodes", configEsNodes)
  conf.set("es.resource", s"$configEsIndexName/$configEsTypeName")
  val ssc = new StreamingContext(conf, Seconds(configSparkStreamInterval))
  val topicsSet = Set(configKafkaRecordsTopic)
  val kafkaParams = Map[String, String](
    //此选项用于设置kafka重新开始获取数据
//    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest",
    "zookeeper.connect" -> configKafkaZkUri,
    "bootstrap.servers" -> configKafkaBrokers,
    "group.id" -> configKafkaConsumeGroup,
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
  )

  //初始化elasticsearch的index
  indexInit()

  //TODO: KafkaUtils.createDirectStream有数据丢失的问题, 实际2732349的记录, es中只是保存了2633902条记录
  val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
  val records = messages.map(_._2)
  records.foreachRDD { rdd =>
    rdd.map { bytes =>
      var m: Map[String, String] = null
      try {
        KryoInjection.invert(bytes) match {
          case Success(obj) =>
            Try(obj.asInstanceOf[Map[String, String]]) match {
              case Success(recordMap) =>
                m = recordMap.map { case (k, v) =>
                  if (k == "LogType(v1.0)") {
                    ("LogType", v)
                  } else {
                    (k, v)
                  }
                }
              case Failure(e) =>
                consoleLog("ERROR", s"transform to Map[String, String] error: ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTraceString}")
                m = null
            }
          case Failure(e) =>
            consoleLog("ERROR", s"KryoInjection.invert(bytes) error: ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTraceString}")
            m = null
        }
      } catch {
        case e: Throwable =>
          consoleLog("ERROR", s"KryoInjection.invert(bytes) fail error: ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTraceString}")
          m = null
      }
      m
    }.filter(_ != null).saveToEs(s"$configEsIndexName/$configEsTypeName")
  }
  ssc.start()
  ssc.awaitTermination()
}
