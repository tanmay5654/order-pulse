package dev.orders.inventory;
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
class InventoryIntegrationTest {
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

 @Test void kafkaRedeliveryDoesNotReduceStockTwice()throws Exception{
  int before=db.queryForObject("SELECT available FROM stock WHERE sku='LAPTOP'",Integer.class);
  Event e=Event.create("OrderCreated",UUID.randomUUID(),"LAPTOP",3,null);
  String payload=mapper.writeValueAsString(e);
  template.send(Messaging.ORDERS,e.sku(),payload).get();template.send(Messaging.ORDERS,e.sku(),payload).get();
  waitFor(()->db.queryForObject("SELECT count(*) FROM reservations WHERE order_id=?",Integer.class,e.orderId())==1);
  Thread.sleep(500);
  assertEquals(before-3,db.queryForObject("SELECT available FROM stock WHERE sku='LAPTOP'",Integer.class));
  assertEquals(1,db.queryForObject("SELECT count(*) FROM processed_events WHERE consumer='inventory' AND event_id=?",Integer.class,e.eventId()));
 }
 @Test void insufficientStockIsBusinessRejection()throws Exception{
  Event e=Event.create("OrderCreated",UUID.randomUUID(),"KEYBOARD",1000000,null);
  int before=db.queryForObject("SELECT available FROM stock WHERE sku='KEYBOARD'",Integer.class);
  template.send(Messaging.ORDERS,e.sku(),mapper.writeValueAsString(e)).get();
  waitFor(()->db.queryForObject("SELECT count(*) FROM reservations WHERE order_id=?",Integer.class,e.orderId())==1);
  assertEquals("StockRejected",db.queryForObject("SELECT status FROM reservations WHERE order_id=?",String.class,e.orderId()));
  assertEquals(before,db.queryForObject("SELECT available FROM stock WHERE sku='KEYBOARD'",Integer.class));
 }
 @Test void invalidSkuRollsBackInboxAndReachesDeadLetterTopic()throws Exception{
  Event e=Event.create("OrderCreated",UUID.randomUUID(),"UNKNOWN",1,null);
  Properties props=new Properties();props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBootstrapServers());props.put(ConsumerConfig.GROUP_ID_CONFIG,"dlt-test-"+UUID.randomUUID());props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
  try(var consumer=new KafkaConsumer<String,String>(props,new StringDeserializer(),new StringDeserializer())){
   consumer.subscribe(List.of(Messaging.ORDERS+".DLT"));
   String json=mapper.writeValueAsString(e);template.send(Messaging.ORDERS,e.sku(),json).get();
   long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);boolean found=false;
   while(!found&&System.nanoTime()<end)for(var record:consumer.poll(Duration.ofMillis(500)))if(record.value().equals(json))found=true;
   assertTrue(found,"Poison event must reach DLT");
   assertEquals(0,db.queryForObject("SELECT count(*) FROM processed_events WHERE event_id=?",Integer.class,e.eventId()));
   assertEquals(0,db.queryForObject("SELECT count(*) FROM reservations WHERE order_id=?",Integer.class,e.orderId()));
  }
 }
}
