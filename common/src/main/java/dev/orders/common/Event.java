package dev.orders.common;
import java.time.Instant;
import java.util.UUID;
public record Event(UUID eventId, String type, UUID orderId, String sku, int quantity, Integer stock, Instant occurredAt) {
 public static Event create(String type, UUID orderId, String sku, int quantity, Integer stock) {
  return new Event(UUID.randomUUID(), type, orderId, sku, quantity, stock, Instant.now());
 }
 public void validate() {
  if(eventId == null || orderId == null || occurredAt == null || sku == null || sku.isBlank() || quantity <= 0)
   throw new IllegalArgumentException("Invalid event envelope");
  if(!java.util.Set.of("OrderCreated", "StockReserved", "StockRejected").contains(type == null ? "" : type))
   throw new IllegalArgumentException("Unknown event type");
  if(!"OrderCreated".equals(type) && (stock == null || stock < 0)) throw new IllegalArgumentException("Invalid stock");
 }
}
