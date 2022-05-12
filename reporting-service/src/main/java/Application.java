import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Application {
    private static final String REPORT_TOPIC = "valid-transactions" + "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Application kafkaConsumerApplication = new Application();

        String consumerGroup = "reporting-service";
        if(args.length == 1){
            consumerGroup = args[0];
        }
        System.out.println("Consumer Group: " + consumerGroup);
        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApplication.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        kafkaConsumerApplication.consumeMessages(Collections.singletonList(REPORT_TOPIC), kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        kafkaConsumer.subscribe(topics);

        while(true){
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            for(ConsumerRecord<String, Transaction> record : consumerRecords){
                String.format("Received record with key: %s and value: %s and was sent to partition: %d with offset: %d for topic: %s",
                        record.key(), record.value(), record.partition(), record.offset(), record.topic());
                recordTransactionForReporting(record.topic(), record.value());
                System.out.println("Topic: " + record.topic());
            }
            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<String, Transaction>(properties);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        if(topic.equals("suspicious-transactions")){
            System.out.println("Transaction is suspicious: " + transaction);
        } else {
            System.out.println("Transaction is valid: " + transaction);
        }
    }

}
