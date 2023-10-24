package ec.ferchoc.springboot_kafka.kafka;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import ec.ferchoc.springboot_kafka.event_handler.WikimediaChangesHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class WikimediaChangesProducer {

    @Value("${spring.kafka.producer.wikimedia.producer.topic}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {

        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // to read real time stream data from wikimedia, we use event source
        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(topic, kafkaTemplate);

        BackgroundEventSource backgroundEventSource = new BackgroundEventSource.Builder(eventHandler,
                new EventSource.Builder(
                    ConnectStrategy.http(URI.create(url)))
        ).build();
        backgroundEventSource.start();

        TimeUnit.MINUTES.sleep(10);

    }

}
