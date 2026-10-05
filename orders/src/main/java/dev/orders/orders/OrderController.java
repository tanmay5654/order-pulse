package dev.orders.orders;
import dev.orders.common.*;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
@RestController
@RequestMapping("/api/orders")
public class OrderController {
 public record Request(@Pattern(regexp="LAPTOP|HEADPHONES|KEYBOARD") @NotBlank String sku,@Min(1) @Max(1000) int quantity){}
 private final JdbcTemplate db;private final Outbox outbox;
 public OrderController(JdbcTemplate db,Outbox outbox){this.db=db;this.outbox=outbox;}
 @PostMapping @Transactional(rollbackFor=Exception.class)
 public ResponseEntity<Map<String,Object>> create(@RequestHeader("Idempotency-Key") @Size(min=1,max=128) String key,@Valid @RequestBody Request request) throws Exception {
  if(key.isBlank() || key.length()>128) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid Idempotency-Key");
  UUID id=UUID.randomUUID();
  int inserted=db.update("INSERT INTO orders(id,idempotency_key,sku,quantity) VALUES (?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING",id,key,request.sku(),request.quantity());
  var row=db.queryForMap("SELECT id,sku,quantity,created_at FROM orders WHERE idempotency_key=?",key);
  if(!row.get("sku").equals(request.sku()) || ((Number)row.get("quantity")).intValue()!=request.quantity())
   throw new ResponseStatusException(HttpStatus.CONFLICT,"Idempotency-Key already used with another payload");
  if(inserted==1)outbox.add(Messaging.ORDERS,Event.create("OrderCreated",id,request.sku(),request.quantity(),null));
  return ResponseEntity.status(inserted==1?HttpStatus.CREATED:HttpStatus.OK).body(row);
 }
 @GetMapping("/{id}") public Map<String,Object> get(@PathVariable UUID id){
  var rows=db.queryForList("SELECT id,sku,quantity,created_at FROM orders WHERE id=?",id);
  if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Order not found");return rows.get(0);
 }
}
