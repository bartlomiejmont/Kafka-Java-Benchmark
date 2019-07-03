import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerExample {

    public static void main(String... args) throws Exception {
        runConsumer();
    }

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));

        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;   int noRecordsCount = 0;
        AtomicInteger timeSum = new AtomicInteger(0);
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(10000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                Long time = System.currentTimeMillis();
                timeSum.set(timeSum.get()+(int)(time-record.timestamp()));
                System.out.printf("Consumer Record:(%s,Partition: %d,Offset: %d Time: %d ms)\n",
                        record.value(),
                        record.partition(), record.offset(), time-record.timestamp());
                if (record.value().equals("STOP")){
                    System.out.printf("Total Time: %.2f s \n",timeSum.floatValue()/1000);
                    System.out.printf("Avg Time: %d ms \n",timeSum.intValue()/1000);
                    consumer.close();
                }
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
        System.out.printf("Avg Time: %d ms \n",timeSum.intValue()/100);
    }

}