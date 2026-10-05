package dev.orders.notifications;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
@RestController
public class MetricsController {
 private final JdbcTemplate db;public MetricsController(JdbcTemplate db){this.db=db;}
 @GetMapping("/api/metrics")public Map<String,Object> metrics(){
  return Map.of("summary",db.queryForMap("SELECT count(*) FILTER(WHERE created_at IS NOT NULL) AS total_orders,count(*) FILTER(WHERE status='StockReserved') AS reserved,count(*) FILTER(WHERE status='StockRejected') AS rejected,count(*) FILTER(WHERE status='PENDING') AS pending FROM order_projection"),
   "minutes",db.queryForList("SELECT series.minute,coalesce(m.orders,0) AS orders FROM generate_series(date_trunc('minute',now())-interval '29 minutes',date_trunc('minute',now()),interval '1 minute') AS series(minute) LEFT JOIN minute_metrics m USING(minute) ORDER BY series.minute"),
   "recent",db.queryForList("SELECT order_id,sku,quantity,status,created_at FROM order_projection ORDER BY created_at DESC NULLS LAST LIMIT 15"),
   "stock",db.queryForList("SELECT sku,available FROM stock_projection ORDER BY sku"));
 }
}
