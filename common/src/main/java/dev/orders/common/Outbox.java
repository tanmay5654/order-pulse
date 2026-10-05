package dev.orders.common;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.TimeUnit;
@Component
public class Outbox {
 private final JdbcTemplate db; private final ObjectMapper mapper; private final KafkaTemplate<String,String> kafka;
 public Outbox(JdbcTemplate db,ObjectMapper mapper,KafkaTemplate<String,String> kafka){this.db=db;this.mapper=mapper;this.kafka=kafka;}
 // Called inside the caller's SQL transaction; never publish before the business row commits.
 public void add(String topic,Event event) throws Exception {
  db.update("INSERT INTO outbox(event_id,topic,event_key,payload) VALUES (?,?,?,?)",event.eventId(),topic,event.sku(),mapper.writeValueAsString(event));
 }
 @Scheduled(fixedDelayString="${app.outbox-delay:100}")
 @Transactional(rollbackFor=Exception.class)
 public void publish() throws Exception {
  var rows=db.queryForList("SELECT event_id,topic,event_key,payload FROM outbox WHERE published_at IS NULL ORDER BY sequence LIMIT 100 FOR UPDATE SKIP LOCKED");
  for(var row:rows){
   kafka.send((String)row.get("topic"),(String)row.get("event_key"),(String)row.get("payload")).get(10,TimeUnit.SECONDS);
   db.update("UPDATE outbox SET published_at=now() WHERE event_id=?",row.get("event_id"));
  }
 }
}
