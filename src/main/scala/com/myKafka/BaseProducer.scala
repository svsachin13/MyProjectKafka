package com.myKafka


import java.util.{UUID, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer => NewKafkaProducer, ProducerRecord, ProducerConfig}
/**
  * Created by sachin on 12/21/2016.
  */
trait BaseProducer {
  def createNewKafkaProducer(brokerList: String,acks: Int = -1,metadataFetchTimeout: Long = 3000L,
                             blockOnBufferFull: Boolean = true,bufferSize: Long = 1024L * 1024L,
                             retries: Int = 0): NewKafkaProducer[Array[Byte], Array[Byte]] = {

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    producerProps.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, metadataFetchTimeout.toString)
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, blockOnBufferFull.toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
    producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "200")
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    new NewKafkaProducer[Array[Byte], Array[Byte]](producerProps)
  }

  val producer = createNewKafkaProducer("localhost:9092")
}

trait Producer extends BaseProducer{

  def pushAnything(topic:String,data:String)={
    val record = new ProducerRecord(topic, 0, "key".getBytes, data.getBytes)
    println(">>>>>>>> PRODUCING")
    producer.send(record)
  }
}

object ConsumerTest extends App with Producer{

  val topic = "test"

  pushAnything(topic,"Sachin Test")

  val consumer = new KafkaConsumer(topic,UUID.randomUUID().toString,"localhost:2181")

  def exec(binaryObject: Array[Byte]) = {
    val message2 = new String(binaryObject)
    println("<<<<<<<<<<< RECEIVING")
    println("---------- >> "+message2)
    consumer.close()
  }

  consumer.read(exec)
}