package dev.orders.notifications;
import dev.orders.common.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.utility.DockerImageName;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
@SpringBootTest
@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
class AnalyticsIntegrationTest {
 @Container static final PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:16.10-alpine");
 @Container static final KafkaContainer kafka=new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
 @DynamicPropertySource static void configure(DynamicPropertyRegistry r){
  r.add("spring.datasource.url",postgres::getJdbcUrl);r.add("spring.datasource.username",postgres::getUsername);r.add("spring.datasource.password",postgres::getPassword);
  r.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);
 }
 @Autowired JdbcTemplate db; @Autowired ObjectMapper mapper; @Autowired KafkaTemplate<String,String> template;
 void waitFor(java.util.function.BooleanSupplier condition)throws Exception{
  long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);
  while(!condition.getAsBoolean()){if(System.nanoTime()>deadline)fail("Timed out waiting for committed consumer effect");Thread.sleep(100);}
 }

 @Autowired AnalyticsConsumer analytics;
 @Test void crossTopicReorderingPreservesFinalStatusAndDeduplicatesCounts()throws Exception{
  int before=db.queryForObject("SELECT coalesce(sum(orders),0) FROM minute_metrics",Integer.class);
  UUID id=UUID.randomUUID();
  Event created=Event.create("OrderCreated",id,"HEADPHONES",2,null);
  Event reserved=Event.create("StockReserved",id,"HEADPHONES",2,24998);
  // Force the completion-first ordering that can occur across separate topics.
  analytics.consume(mapper.writeValueAsString(reserved));
  analytics.consume(mapper.writeValueAsString(created));
  analytics.consume(mapper.writeValueAsString(created));
  analytics.consume(mapper.writeValueAsString(Event.create("OrderCreated",id,"HEADPHONES",2,null)));
  assertEquals("StockReserved",db.queryForObject("SELECT status FROM order_projection WHERE order_id=?",String.class,id));
  assertEquals(before+1,db.queryForObject("SELECT sum(orders) FROM minute_metrics",Integer.class));
  assertEquals(1,db.queryForObject("SELECT count(*) FROM notifications WHERE order_id=?",Integer.class,id));
 }
 @Test void replayedOlderStockSnapshotCannotIncreaseAvailableStock()throws Exception{
  analytics.consume(mapper.writeValueAsString(Event.create("StockReserved",UUID.randomUUID(),"LAPTOP",1,9000)));
  analytics.consume(mapper.writeValueAsString(Event.create("StockReserved",UUID.randomUUID(),"LAPTOP",1,9500)));
  assertEquals(9000,db.queryForObject("SELECT available FROM stock_projection WHERE sku='LAPTOP'",Integer.class));
 }
 @Test void kafkaEventIsAggregatedIntoSql()throws Exception{
  Event e=Event.create("OrderCreated",UUID.randomUUID(),"KEYBOARD",1,null);
  template.send(Messaging.ORDERS,e.sku(),mapper.writeValueAsString(e)).get();
  waitFor(()->db.queryForObject("SELECT count(*) FROM order_projection WHERE order_id=?",Integer.class,e.orderId())==1);
  assertEquals("PENDING",db.queryForObject("SELECT status FROM order_projection WHERE order_id=?",String.class,e.orderId()));
 }
}
