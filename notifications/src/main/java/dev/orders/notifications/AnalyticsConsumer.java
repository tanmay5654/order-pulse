package dev.orders.notifications;
import dev.orders.common.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.sql.Timestamp;
import java.time.temporal.ChronoUnit;
@Service
public class AnalyticsConsumer {
 private final JdbcTemplate db;private final Inbox inbox;private final ObjectMapper mapper;
 public AnalyticsConsumer(JdbcTemplate db,Inbox inbox,ObjectMapper mapper){this.db=db;this.inbox=inbox;this.mapper=mapper;}
 @KafkaListener(topics={Messaging.ORDERS,Messaging.INVENTORY})
 @Transactional(rollbackFor=Exception.class)
 public void consume(String json)throws Exception{
  Event e=Messaging.decode(mapper,json);
  if(!inbox.first("analytics",e))return;
  if(e.type().equals("OrderCreated")){
   // Inventory completion may arrive first from a different topic; preserve its status.
   db.update("INSERT INTO order_projection(order_id,sku,quantity,status,created_at) VALUES (?,?,?,'PENDING',?) ON CONFLICT(order_id) DO UPDATE SET created_at=EXCLUDED.created_at WHERE order_projection.created_at IS NULL",e.orderId(),e.sku(),e.quantity(),Timestamp.from(e.occurredAt()));
   // Only count the first OrderCreated for an order, even if it was republished under another event ID.
   if(inbox.first("order-count",new Event(e.orderId(),e.type(),e.orderId(),e.sku(),e.quantity(),e.stock(),e.occurredAt())))
    db.update("INSERT INTO minute_metrics(minute,orders) VALUES (?,1) ON CONFLICT(minute) DO UPDATE SET orders=minute_metrics.orders+1",Timestamp.from(e.occurredAt().truncatedTo(ChronoUnit.MINUTES)));
  }else{
   db.update("INSERT INTO order_projection(order_id,sku,quantity,status) VALUES (?,?,?,?) ON CONFLICT(order_id) DO UPDATE SET status=EXCLUDED.status",e.orderId(),e.sku(),e.quantity(),e.type());
   db.update("INSERT INTO stock_projection(sku,available) VALUES (?,?) ON CONFLICT(sku) DO UPDATE SET available=LEAST(stock_projection.available,EXCLUDED.available)",e.sku(),e.stock());
   db.update("INSERT INTO notifications(order_id,status) VALUES (?,?) ON CONFLICT DO NOTHING",e.orderId(),e.type());
  }
 }
}
