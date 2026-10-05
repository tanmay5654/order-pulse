package dev.orders.inventory;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import java.util.*;
@RestController
public class StockController {
 private final JdbcTemplate db;public StockController(JdbcTemplate db){this.db=db;}
 @GetMapping("/api/stock")public List<Map<String,Object>> stock(){return db.queryForList("SELECT sku,available FROM stock ORDER BY sku");}
}
