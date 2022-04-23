package org.asx;

import java.util.Arrays;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

@Slf4j
public class Consumer {

  private Properties setConsumerConfig(Config config) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapURL());
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupID());
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // "earliest/latest/none"
//    properties.setProperty("schema.registry.url", config.getSchemaRegistry());
//    properties.setProperty("specific.avro.reader", "true");
    return properties;
  }

  /**
   * Consume kafka messages
   *
   * @param config
   */
  public void consumeMessages(Config config) {
    Properties configProperties = setConsumerConfig(config);
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties)) {
      kafkaConsumer.subscribe(Arrays.asList(config.getTopic()));
      System.out.println("Waiting for data");
      while (true) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String>  record : records) {
          String value = record.value();
          System.out.println("Successfully consumed message : "+value);
        }
        kafkaConsumer.commitSync();
        kafkaConsumer.close();

      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }
}
