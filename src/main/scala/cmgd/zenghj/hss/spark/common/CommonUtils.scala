package cmgd.zenghj.hss.spark.common

import java.io.File
import java.net.URI

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/8/9.
  */
object CommonUtils {
  val hdfsUri = "hdfs://localhost:9000/hss/conf/application.conf"
  val confTmpFile = File.createTempFile("application", "tmp")
  confTmpFile.deleteOnExit()
  val hdfsConf = new Configuration()
  hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  val hdfsFile = FileSystem.get(new URI(hdfsUri), hdfsConf)
  hdfsFile.copyToLocalFile(new Path(hdfsUri), new Path(confTmpFile.getAbsolutePath))

  val config = ConfigFactory.parseFile(confTmpFile)

  val configSpark = config.getConfig("spark")
  val configSparkMaster = configSpark.getString("master")
  val configSparkAppName = configSpark.getString("app-name")
  val configSparkStreamInterval = configSpark.getInt("stream-interval")

  val configEs = config.getConfig("elasticsearch")
  val configEsNodes = configEs.getString("nodes")
  val configEsClusterName = configEs.getString("cluster-name")
  val configEsHosts: Array[(String, Int)] = configEs.getConfigList("hosts").map { conf =>
    (conf.getString("host"), conf.getInt("port"))
  }.toArray
  val configEsIndexName = configEs.getString("index-name")
  val configEsTypeName = configEs.getString("type-name")
  val configEsNumberOfShards = configEs.getInt("number-of-shards")
  val configEsNumberOfReplicas = configEs.getInt("number-of-replicas")

  val configKafka = config.getConfig("kafka")
  val configKafkaZkUri = configKafka.getString("zookeeper-uri")
  val configKafkaBrokers = configKafka.getString("brokers-list")
  val configKafkaRecordsTopic = configKafka.getString("kafka-records-topic")
  val configKafkaConsumeGroup = configKafka.getString("consume-group")

  def consoleLog(logType: String, msg: String) = {
    val timeStr = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    println(s"[$logType] $timeStr: $msg")
  }


}
