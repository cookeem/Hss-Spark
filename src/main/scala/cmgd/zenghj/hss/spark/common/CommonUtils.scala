package cmgd.zenghj.hss.spark.common

import java.io.{FilenameFilter, File}
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
  val localConfigPath = "conf"
  val localConfigFileName = "application.conf"
  val localLibPath = "hss-lib"
  val localConfig = ConfigFactory.parseFile(new File(s"$localConfigPath/$localConfigFileName"))
  val configHadoop = localConfig.getConfig("hadoop")
  val configHadoopHostUri = configHadoop.getString("host-uri")
  val configHadoopRootPath = configHadoop.getString("root-path")
  val configHadoopConfigPath = configHadoop.getString("config-path")
  val configHadoopLibPath = configHadoop.getString("lib-path")

  val hdfsConf = new Configuration()
  hdfsConf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  val hdfsFileSystem = FileSystem.get(new URI(configHadoopHostUri) ,hdfsConf)
  hdfsFileSystem.delete(new Path(s"$configHadoopHostUri/$configHadoopRootPath"), true)

  //把本地的conf/application.conf复制到hdfs上,让executor也可以读取
  hdfsFileSystem.mkdirs(new Path(s"$configHadoopHostUri/$configHadoopRootPath/$configHadoopConfigPath"))
  hdfsFileSystem.copyFromLocalFile(false, true, new Path(s"$localConfigPath/$localConfigFileName"), new Path(s"$configHadoopHostUri/$configHadoopRootPath/$configHadoopConfigPath/$localConfigFileName"))
  val confTmpFile = File.createTempFile(localConfigFileName, "tmp")
  confTmpFile.deleteOnExit()
  //从hdfs上获取application.conf文件,让executor也可以读取
  hdfsFileSystem.copyToLocalFile(new Path(s"$configHadoopHostUri/$configHadoopRootPath/$configHadoopConfigPath/$localConfigFileName"), new Path(confTmpFile.getAbsolutePath))

  //把lib的jar依赖包复制到hdfs上
  hdfsFileSystem.mkdirs(new Path(s"$configHadoopHostUri/$configHadoopRootPath/$configHadoopLibPath"))
  val libPath = new File(localLibPath)
  libPath.list().filter(_.endsWith(".jar")).foreach { libFileName =>
    hdfsFileSystem.copyFromLocalFile(false, true, new Path(s"$localLibPath/$libFileName"), new Path(s"$configHadoopHostUri/$configHadoopRootPath/$configHadoopLibPath/$libFileName"))
  }

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
  val configKafkaFromBeginning = configKafka.getInt("from-beginning")
  val configKafkaZkUri = configKafka.getString("zookeeper-uri")
  val configKafkaBrokers = configKafka.getString("brokers-list")
  val configKafkaRecordsTopic = configKafka.getString("kafka-records-topic")
  val configKafkaConsumeGroup = configKafka.getString("consume-group")

  def consoleLog(logType: String, msg: String) = {
    val timeStr = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    println(s"[$logType] $timeStr: $msg")
  }

}
