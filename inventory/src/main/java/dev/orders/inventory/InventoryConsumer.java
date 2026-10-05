package dev.orders.inventory;
import dev.orders.common.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
@Service
public class InventoryConsumer {
 private final JdbcTemplate db;private final Inbox inbox;private final Outbox outbox;private final ObjectMapper mapper;
 public InventoryConsumer(JdbcTemplate db,Inbox inbox,Outbox outbox,ObjectMapper mapper){this.db=db;this.inbox=inbox;this.outbox=outbox;this.mapper=mapper;}
 @KafkaListener(topics=Messaging.ORDERS)
 @Transactional(rollbackFor=Exception.class)
 public void consume(String json)throws Exception{
  Event event=Messaging.decode(mapper,json);
  if(!event.type().equals("OrderCreated"))throw new IllegalArgumentException("Expected OrderCreated");
  if(!inbox.first("inventory",event))return;
  // Serializes stock changes for a SKU, including competing service instances.
  var rows=db.queryForList("SELECT available FROM stock WHERE sku=? FOR UPDATE",event.sku());
  if(rows.isEmpty())throw new IllegalArgumentException("Unknown SKU: "+event.sku());
  if(db.queryForObject("SELECT count(*) FROM reservations WHERE order_id=?",Long.class,event.orderId())>0)return;
  int stock=((Number)rows.get(0).get("available")).intValue();
  boolean accepted=stock>=event.quantity();
  if(accepted){stock-=event.quantity();db.update("UPDATE stock SET available=? WHERE sku=?",stock,event.sku());}
  String type=accepted?"StockReserved":"StockRejected";
  db.update("INSERT INTO reservations(order_id,sku,quantity,status) VALUES (?,?,?,?)",event.orderId(),event.sku(),event.quantity(),type);
  outbox.add(Messaging.INVENTORY,Event.create(type,event.orderId(),event.sku(),event.quantity(),stock));
 }
}
