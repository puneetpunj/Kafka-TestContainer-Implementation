package org.asx;

import org.apache.kafka.common.serialization.StringSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class Producer {
  private Properties getKafkaStreamsConfig(Config config) {
    // Create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapURL());
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return properties;
  }

  /**
   * create topics
   * @param config config
   */
  public void createTopic(Config config) throws ExecutionException, InterruptedException {
    // Create the topic
    System.out.println("Creating topic");
    AdminClient adminClient = AdminClient.create(config.getProperties());
    NewTopic newTopic = new NewTopic(config.getTopic(), 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

    List<NewTopic> newTopics = new ArrayList<NewTopic>();
    newTopics.add(newTopic);

    adminClient.createTopics(newTopics);
    ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
    listTopicsOptions.listInternal(true);
    System.out.println("topics:" + adminClient.listTopics(listTopicsOptions).names().get());

    System.out.println("topic successfully created");
    adminClient.close();

  }


  /**
   * Produce kafka messages
   *
   * @param config
   * @throws Exception
   */
  public void produceMessages(Config config) throws Exception {

    Properties configProperty = getKafkaStreamsConfig(config);
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(configProperty);

    for (int i = 0; i < 10; i++) {

      ProducerRecord<String, String> producerRecord = new ProducerRecord<>(config.getTopic(),
          config.getData() + "_" +i);

      kafkaProducer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (e == null) {
            System.out.println("Received Record Metadata : "+
                "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() + ", " +
                "Offset: " + recordMetadata.offset() + " @ Timestamp: " + recordMetadata.timestamp());
          } else {
            System.out.println(e);
            e.printStackTrace();
          }
        };
      });
    }
      kafkaProducer.flush();
      kafkaProducer.close();
    }
  }
