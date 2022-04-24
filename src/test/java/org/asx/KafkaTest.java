package org.asx;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import io.qameta.allure.*;

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
    @Step("Start Kafka Docker Container")
    public void beforeTest() {
      kafkaContainer.start();
      System.out.println("Starting kafka container");
    }

    @Severity(SeverityLevel.CRITICAL)
    @Test(description = "E2E test for Kafka stream")
    @Description("Produce and Consume messages on Kafka Topic")
    @Story("Implement Kafka Test Containers")
    public void TestKafkaProducer() throws Exception {
      properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          kafkaContainer.getBootstrapServers());
      properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          StringSerializer.class.getName());
      properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          StringSerializer.class.getName());

      // Create the producer
      Config conf = Config
          .builder()
          .Topic(topic)
          .Data(value)
          .BootstrapURL(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
          .GroupID(groupId)
          .Properties(properties)
          .build();


      CreateKafkaTopic(conf);
      ProduceMessagesOnTopic(conf);
      ConsumeMessagesFromTopic(conf);

    }

    @AfterTest
    @Step("Stopping Kafka Docker Container")
    public void StopContainer() {
      kafkaContainer.stop();
      System.out.println("Stopping kafka container");
    }
    @Step("Create Kafka Topic")
    private void CreateKafkaTopic(Config conf) throws Exception{
      producer.createTopic(conf);
    }
    @Step("Produce Messages")
    private void ProduceMessagesOnTopic(Config conf) throws Exception{
      producer.produceMessages(conf);
    }
    @Step("Consume Messages")
    private void ConsumeMessagesFromTopic(Config conf) throws Exception{
      consumer.consumeMessages(conf);
    }
  }
