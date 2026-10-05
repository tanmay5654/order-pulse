package dev.orders.common;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
@Component
public class Inbox {
 private final JdbcTemplate db;
 public Inbox(JdbcTemplate db){this.db=db;}
 // The marker and all consumer effects share one SQL transaction.
 public boolean first(String consumer,Event event){
  return db.update("INSERT INTO processed_events(consumer,event_id) VALUES (?,?) ON CONFLICT DO NOTHING",consumer,event.eventId())==1;
 }
}
