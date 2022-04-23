package org.asx;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Properties;

  public class KafkaTest {
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"));

    final String topic = "test-topic";
    final String value = "hello world";
    final String groupId = "test-group";

    Properties properties = new Properties();
    Producer producer = new Producer();
    Consumer consumer = new Consumer();

    @BeforeTest
    public void beforeTest() {
      kafkaContainer.start();
      System.out.println("Starting kafka container");
    }

    @Test
    public void TestKafkaProducer() throws Exception {
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

      // Create the producer
      Config conf = Config
          .builder()
          .Topic(topic)
          .Data(value)
          .BootstrapURL(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
          .GroupID(groupId)
          .Properties(properties)
          .build();

      producer.createTopic(conf);
      producer.produceMessages(conf);
      consumer.consumeMessages(conf);
    }

    @AfterTest
    public void StopContainer() {
      kafkaContainer.stop();
      System.out.println("Stopping kafka container");
    }
  }
