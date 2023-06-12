import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateTopicsResult, ListTopicsOptions}

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters.MapHasAsJava

object CreateTopic extends App {

  import org.apache.kafka.clients.admin.NewTopic
  import org.apache.kafka.common.config.TopicConfig

  val properties = new Properties()
  properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  val admin: Admin = Admin.create(properties)

  //val topicName = "stock-transactions"
  val topicName = "names3"
  val partitions = 2
  val replicationFactor: Short = 1.toShort

  val newTopicConfig: collection.mutable.Map[String, String] = collection.mutable.Map[String, String]()
  newTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
  //newTopicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "10000")
  //newTopicConfig.put(TopicConfig.SEGMENT_MS_CONFIG, "5000")
  //newTopicConfig.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4")

  val newTopic: NewTopic = new NewTopic(topicName, partitions, replicationFactor).configs(newTopicConfig.asJava)

  val result: CreateTopicsResult = admin.createTopics(Collections.singleton(newTopic))

  result.values().get(topicName).get() // Complete the future

  val listTopicsOptions = new ListTopicsOptions()
  listTopicsOptions.listInternal(true)

  System.out.println(admin.listTopics(listTopicsOptions).names().get())

}
