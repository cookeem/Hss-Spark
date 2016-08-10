package cmgd.zenghj.hss.spark.es

import cmgd.zenghj.hss.spark.common.CommonUtils._

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory

/**
  * Created by cookeem on 16/8/2.
  */
object EsUtils extends App {
  def indexInit() = {
    try {
      val settings = Settings.settingsBuilder().put("cluster.name", configEsClusterName).build()
      val esClient = TransportClient.builder().settings(settings).build()
      configEsHosts.foreach { case (host, port) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
      }
      val isExists = esClient.admin().indices().prepareExists(configEsIndexName).execute().actionGet().isExists
      if (!isExists) {
        val indexMapping = esClient.admin().indices().prepareCreate(configEsIndexName)
          .setSettings(
            XContentFactory.jsonBuilder()
              .startObject()
                .field("number_of_shards", configEsNumberOfShards)
                .field("number_of_replicas", configEsNumberOfReplicas)
                .startObject("analysis")
                  .startObject("analyzer")
                    .startObject("my_analyzer")
                      .field("type", "custom")
                      .field("tokenizer", "standard")
                      .startArray("filter")
                        .value("lowercase")
                      .endArray()
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
          )
          .addMapping(configEsTypeName,
            XContentFactory.jsonBuilder()
              .startObject()
                .startObject(configEsTypeName)
                  .startObject("properties")
                    .startObject("SubLogId")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Target")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("ExecuteTime")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("FullRequest")
                      .field("type", "string")
                      .field("analyzer", "my_analyzer")
                      .field("search_analyzer", "my_analyzer")
                    .endObject()
                    .startObject("LogType")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("StartTime")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Operation")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("ResponseCode")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("TransactionId")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Hostname")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("User")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Protocol")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("RootLogId")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("FullResponse")
                      .field("type", "string")
                      .field("analyzer", "my_analyzer")
                      .field("search_analyzer", "my_analyzer")
                    .endObject()
                    .startObject("Instance")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                    .startObject("Status")
                      .field("type", "string")
                      .field("index", "not_analyzed")
                    .endObject()
                  .endObject()
                .endObject()
              .endObject()
          )
        indexMapping.execute().actionGet()
        Thread.sleep(1000)
        consoleLog("SUCCESS", s"es indexInit $configEsIndexName created")
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"es indexInit error: ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTraceString}")
    }
  }

  //删除所有index
  //return: errmsg
  def removeAllIndex(): String = {
    var errmsg = ""
    try {
      val settings = Settings.settingsBuilder().put("cluster.name", configEsClusterName).build()
      val esClient = TransportClient.builder().settings(settings).build()
      configEsHosts.foreach { case (host, port) =>
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
      }
      esClient.admin().indices().prepareGetIndex().execute().actionGet().getIndices.foreach(idxName => {
        esClient.admin().indices().prepareDelete(idxName).execute().actionGet()
      })
    } catch {
      case e: Throwable =>
        errmsg = s"removeAllIndex error: ${e.getMessage}, ${e.getCause}, ${e.getClass}, ${e.getStackTraceString}"
    }
    errmsg
  }
}
