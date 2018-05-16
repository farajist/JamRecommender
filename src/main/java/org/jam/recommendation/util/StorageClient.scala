package org.jam.recommendation.util

import java.net.InetAddress

import grizzled.slf4j.Logging
import org.apache.predictionio.data.storage.{BaseStorageClient, StorageClientConfig, StorageClientException}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.ConnectTransportException
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress



class StorageClient(val config: StorageClientConfig) extends BaseStorageClient
  with Logging {
  override val prefix = "ES"
  val client = try {
    val hosts = config.properties.get("HOSTS").
      map(_.split(",").toSeq).getOrElse(Seq("localhost"))
    val ports = config.properties.get("PORTS").
      map(_.split(",").toSeq.map(_.toInt)).getOrElse(Seq(9300))
    val settings = Settings.builder()
      .put("cluster.name", config.properties.getOrElse("CLUSTERNAME", "elasticsearch"))
    val transportClient = new PreBuiltTransportClient(Settings.builder()
                                                          .put("cluster.name", config.properties.getOrElse("CLUSTERNAME", "elasticsearch"))
                                                          .build()
                                                      );
//    transportClient.settings(settings);


    (hosts zip ports) foreach { hp =>
      transportClient.addTransportAddress(
        new InetSocketTransportAddress(InetAddress.getByName(hp._1), hp._2.toInt))
    }
    transportClient
  } catch {
    case e: ConnectTransportException =>
      throw new StorageClientException(e.getMessage, e)
  }
}
