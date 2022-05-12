import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {

    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();
        Application application = new Application();

        Producer<String, Transaction> kafkaProducer = application.createKafkaProducer(BOOTSTRAP_SERVERS);

        try {
            application.processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }

    }

    public void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {

        while(incomingTransactionsReader.hasNext()){
            Transaction transaction = incomingTransactionsReader.next();

            if(customerAddressDatabase.getUserResidence(transaction.getUser()).equals(transaction.getTransactionLocation())){
                //Printing out Valid transactions
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC, transaction.getUser(), transaction);
                RecordMetadata metadata = kafkaProducer.send(record).get();

                System.out.println(String.format("Record with key: %s and value: %s was sent to partition: %d with offset: %d for topic: %s",
                        record.key(), record.value(), metadata.partition(), metadata.offset(), metadata.topic()));
            } else {
                //Printing out Suspicious transactions
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC, transaction.getUser(), transaction);
                RecordMetadata metadata = kafkaProducer.send(record).get();

                System.out.println(String.format("Record with key: %s and value: %s was sent to partition: %d with offset: %d for topic: %s",
                        record.key(), record.value(), metadata.partition(), metadata.offset(), metadata.topic()));
            }
            //Printing out all transactions
            ProducerRecord<String, Transaction> record = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC + SUSPICIOUS_TRANSACTIONS_TOPIC, transaction.getUser(), transaction);
            RecordMetadata metadata = kafkaProducer.send(record).get();

            System.out.println(String.format("Record with key: %s and value: %s was sent to partition: %d with offset: %d for topic: %s",
                    record.key(), record.value(), metadata.partition(), metadata.offset(), metadata.topic()));
        }



    }

    public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "bank-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

}
