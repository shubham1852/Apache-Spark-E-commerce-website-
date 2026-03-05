package jobs.stream;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import jobs.TopicDetails;
import jobs.UserActivity;
import util.UserActivityUtil;

public class UserActivityEventProducer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = TopicDetails.consumer_activity.name();

        Random random = new Random();

        while (true) {

            UserActivity activity = UserActivityUtil.generateUserActivity();

            // JSON message
            String json = String.format(
                    "{\"id\":%d,\"campaignid\":%d,\"orderid\":%d,"
                            + "\"total_amount\":%d,\"units\":%d,\"activity\":\"%s\"}",
                    activity.getId(),
                    activity.getCampaignId(),
                    activity.getOrderId(),
                    activity.getAmount(),
                    activity.getUnits(),
                    activity.getActivity()
            );

            String key = String.valueOf(random.nextInt(10000));

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, json);

            producer.send(record);
            System.out.println("Sent: " + json);

            Thread.sleep(1000);
        }
    }
}