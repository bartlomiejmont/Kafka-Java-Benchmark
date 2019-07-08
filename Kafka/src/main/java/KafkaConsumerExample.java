import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;

public class KafkaConsumerExample {

    public static void main(String... args) throws Exception {
        runConsumer();
    }

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    Long timeStart = System.currentTimeMillis();

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
        props.put("fetch.wait.max.ms",2);
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
        AtomicInteger i = new AtomicInteger(0);
        List<Long> times = new ArrayList<Long>();

        System.out.println("Consumer Starts!!!");

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }


            consumerRecords.forEach(record -> {
//                times.add(System.currentTimeMillis()-record.timestamp());

//                timeSum.set(timeSum.get()+(int)(time-record.timestamp()));
//                System.out.printf("Consumer Record:(%s,Partition: %d,Offset: %d Time: %d ms)\n",
//                        record.value(),
//                        record.partition(), record.offset(), time-record.timestamp());
                if (record.value().equals("STOP")){
//                    System.out.printf("Total Time: %.2f s \n",timeSum.floatValue()/10000);
//                    System.out.printf("Avg Time: %d ms \n",timeSum.intValue()/1000);
                      Long time1 = times.get(0);
                      Long time2 = System.currentTimeMillis();
                      System.out.printf("Time1: %d ns Time2: %d ns Time ms: %.2f ms\n",time1,time2, ((double)time2-(double)time1));
//                    for (Long t:times) {
//                    System.out.printf("ts: %d  nr: %d \n",t,i.get());
//                    i.set(i.get()+1);
//                    }
                    //Double average = times.stream().mapToDouble(val -> val).average().orElse(0.0);
                    //System.out.printf("Average: %.3f ", average);
                    consumer.close();
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
        out.println("DONE");
//        System.out.printf("Avg Time: %d ms \n",timeSum.intValue()/10000);
    }

}