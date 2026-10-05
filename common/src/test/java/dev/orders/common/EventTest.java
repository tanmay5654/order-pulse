package dev.orders.common;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.UUID;
class EventTest {
 @Test void envelopeRejectsMissingFieldsAndInvalidTypes(){
  assertThrows(IllegalArgumentException.class,()->Event.create("Unknown",UUID.randomUUID(),"LAPTOP",1,null).validate());
  assertThrows(IllegalArgumentException.class,()->Event.create("OrderCreated",UUID.randomUUID(),"LAPTOP",0,null).validate());
  assertThrows(IllegalArgumentException.class,()->Event.create("StockReserved",UUID.randomUUID(),"LAPTOP",1,-1).validate());
  assertDoesNotThrow(()->Event.create("StockRejected",UUID.randomUUID(),"LAPTOP",1,0).validate());
 }
}
