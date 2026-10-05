package dev.orders.orders;
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
@org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
@SpringBootTest
@org.springframework.test.annotation.DirtiesContext(classMode=org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS)
@Testcontainers
class OrdersIntegrationTest {
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

 @Autowired org.springframework.test.web.servlet.MockMvc mvc;
 @Test void repeatedHttpRequestReturnsSameOrderAndOneOutboxEvent()throws Exception{
  String key=UUID.randomUUID().toString();
  var request=org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/orders").header("Idempotency-Key",key).contentType("application/json").content("{\"sku\":\"LAPTOP\",\"quantity\":2}");
  String first=mvc.perform(request).andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isCreated()).andReturn().getResponse().getContentAsString();
  String second=mvc.perform(request).andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isOk()).andReturn().getResponse().getContentAsString();
  assertEquals(mapper.readTree(first).get("id"),mapper.readTree(second).get("id"));
  String id=mapper.readTree(first).get("id").asText();
  assertEquals(1,db.queryForObject("SELECT count(*) FROM outbox WHERE payload::jsonb->>'orderId'=?",Integer.class,id));
  waitFor(()->db.queryForObject("SELECT count(*) FROM outbox WHERE payload::jsonb->>'orderId'=? AND published_at IS NOT NULL",Integer.class,id)==1);
  mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/orders").header("Idempotency-Key",key).contentType("application/json").content("{\"sku\":\"LAPTOP\",\"quantity\":3}")).andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isConflict());
 }
 @Test void invalidQuantityIsRejectedBeforePublishing()throws Exception{
  mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post("/api/orders").header("Idempotency-Key",UUID.randomUUID().toString()).contentType("application/json").content("{\"sku\":\"LAPTOP\",\"quantity\":0}")).andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.status().isBadRequest());
 }
}
