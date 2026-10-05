package dev.orders.common;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.*;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.backoff.FixedBackOff;
@Configuration
public class Messaging {
 public static final String ORDERS = "orders.events", INVENTORY = "inventory.events";
 @Bean org.springframework.kafka.core.KafkaAdmin.NewTopics topics() {
  return new org.springframework.kafka.core.KafkaAdmin.NewTopics(
   TopicBuilder.name(ORDERS).partitions(3).replicas(1).build(),
   TopicBuilder.name(INVENTORY).partitions(3).replicas(1).build(),
   TopicBuilder.name(ORDERS+".DLT").partitions(3).replicas(1).build(),
   TopicBuilder.name(INVENTORY+".DLT").partitions(3).replicas(1).build());
 }
 @Bean DefaultErrorHandler errorHandler(KafkaTemplate<String,String> kafka) {
  var recoverer = new DeadLetterPublishingRecoverer(kafka, (r,e) -> new TopicPartition(r.topic()+".DLT",r.partition()));
  recoverer.setFailIfSendResultIsError(true);
  return new DefaultErrorHandler(recoverer,new FixedBackOff(500L,3L));
 }
 public static Event decode(ObjectMapper mapper,String json) throws Exception {
  Event event=mapper.readValue(json,Event.class); event.validate();return event;
 }
}
