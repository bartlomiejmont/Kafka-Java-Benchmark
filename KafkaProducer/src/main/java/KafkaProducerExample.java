import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String... args) throws Exception {
        if (args.length == 0) {
            runProducer(messagesCount);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }

    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static int messagesCount = 1000;

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put("linger.ms",5);
//        props.put("fetch.wait.max.m", 10);
        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC,index,
                                "ID: " + index);

                RecordMetadata metadata = producer.send(record).get();

                //long elapsedTime = System.currentTimeMillis() - time;
//                System.out.printf("sent record(key=%s value=%s) " +
//                                "meta(partition=%d, offset=%d) \n",
//                        record.key(), record.value(), metadata.partition(),
//                        metadata.offset());
                //Thread.sleep(10);
                }
        } finally {

            //Force consumer to stop listening
            final ProducerRecord<Long, String> record =
                    new ProducerRecord<>(TOPIC, System.currentTimeMillis(),
                            "STOP");

            RecordMetadata metadata = producer.send(record).get();

            long elapsedTime = System.currentTimeMillis() - time;
            System.out.printf("Sent %d messages in %.2f seconds\n",messagesCount,(float)elapsedTime/1000);
            System.out.printf("Messages per seconds: %.8f msg/s", ((float)messagesCount/(float)elapsedTime)*1000);
           // producer.flush();
            producer.close();
        }
    }


}