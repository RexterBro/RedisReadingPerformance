import com.example.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.redisson.Redisson;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Properties;
import java.util.UUID;

public class Main {
    private static void spin(int milliseconds) {
        long sleepTime = milliseconds * 1000000L; // convert to nanoseconds
        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) < sleepTime) {
        }
    }

    public static void main(String[] args) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://localhost:6379");

        KafkaProducer<Object, Object> kafkaProducer = createKafkaProducer();
        RedissonClient redisson = Redisson.create(config);
        RSet<String> set = redisson.getSet("mySet");
        while (true) {
            long time = System.currentTimeMillis();
            for (int i = 0; i < 250; i++) {
                User user = User.newBuilder()
                        .setId(1)
                        .setUsername("yuval")
                        .setPasswordHash("321dsada@#SDA").build();
                set.contains(UUID.randomUUID().toString());
                kafkaProducer.send(new ProducerRecord<>("users", user));
            }

            System.out.println("elapsed: " + (System.currentTimeMillis() - time));
        }
    }

    private static <K,V> KafkaProducer<K, V> createKafkaProducer() {
        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("schema.registry.url", "http://localhost:8081");
        config.put("acks", "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return new KafkaProducer<>(config);
    }
}
