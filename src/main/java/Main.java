import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by Aashish Nehete on 12-Apr-18.
 */

public class Main {
    final String TOPIC = "game";
    String username = "";
    int play = 0;
    Map<String, Integer> records = new HashMap<>();

    public static void main(String args[]) {
        System.out.println("Starting Main class...");
        Main main = new Main();
        main.gamePlay();
    }

    public void gamePlay() {
        new Thread() {
            @Override
            public void run() {
                startReceive();
            }
        }.start();
        Scanner sc = new Scanner(System.in);
        System.out.println("Rock Paper Scissor");
        System.out.println("enter username:");
        username = sc.next();
        System.out.println("1. rock\n" +
                "2. paper\n" +
                "3. scissor\n" +
                "enter play: ");
        play = sc.nextInt();

        send(username, play);

        if (!records.isEmpty()) {
            records.forEach((key, value) -> {
                if (!key.equals(username)) {
                    System.out.println(key + " chooses: " + value);
                    System.out.println("You " + isWinner(Integer.valueOf(value)));
                }
            });
        }
    }

    public void send(String username, int play) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        long time = System.currentTimeMillis();

        try {
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, username,
                            String.valueOf(play));
            RecordMetadata metadata = producer.send(record).get();
            long elapsedTime = System.currentTimeMillis() - time;
            System.out.println("Waiting for opponent...");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public void startReceive() {
        String BOOTSTRAP_SERVERS = "localhost:9092";
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);
        // Subscribe to the topic.
//        consumer.subscribe(Collections.singletonList(TOPIC));

        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
        consumer.assign(Arrays.asList(topicPartition));

//        consumer.poll(0);
//        consumer.seekToBeginning(consumer.assignment());

        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                records.put(record.key(), Integer.valueOf(record.value()));
                if (!username.equals("") && play != 0) {
                    if (!username.equals(record.key())) {
                        // Different player record
                        int value = Integer.valueOf(record.value());
                        System.out.println(record.key() + " chooses: " + value);
                        System.out.println("You " + isWinner(value));
                    }
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    private String isWinner(int value) {
        if (value == 1) {
            if (play == 2) return "win";
            else if (play == 3) return "lose";
            else return "tie";
        } else if (value == 2) {
            if (play == 1) return "lose";
            else if (play == 3) return "win";
            else return "tie";
        } else if (value == 3) {
            if (play == 1) return "win";
            else if (play == 2) return "lose";
            else return "tie";
        }
        return "tie";
    }
}
