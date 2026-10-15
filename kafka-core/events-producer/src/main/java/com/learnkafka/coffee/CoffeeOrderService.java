package com.learnkafka.coffee;

import com.learnavro.domain.generated.Address;
import com.learnavro.domain.generated.CoffeeOrder;
import com.learnavro.domain.generated.OrderLineItem;
import com.learnavro.domain.generated.PickUp;
import com.learnavro.domain.generated.Size;
import com.learnavro.domain.generated.Store;
import com.learnkafka.domain.CoffeeOrderEvent;
import com.learnkafka.domain.CoffeeOrderRequest;
import com.learnkafka.domain.CoffeeOrderResponse;
import com.learnkafka.event.EventMetadataFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Places a coffee order with the transactional outbox: the order row and its CoffeeOrder.PLACED event
 * are written in ONE database transaction — both or neither. The app never talks to Kafka for coffee
 * orders; Debezium reads the outbox row from the write-ahead log and publishes it to "coffee-orders"
 * (key = order id, partition = the VIP rule's choice, value = the Avro bytes stored here).
 */
@Service
public class CoffeeOrderService {

    /** Topic the Outbox Event Router routes these events to (outbox_event.aggregatetype). */
    public static final String TOPIC = "coffee-orders";

    private final CoffeeOrderRepository orders;
    private final OutboxRepository outbox;
    private final CoffeeOrderPayloadSerializer payloadSerializer;
    private final EventMetadataFactory eventMetadataFactory;
    private final VipPartitionRule vipPartitionRule;
    private final int partitions;

    public CoffeeOrderService(CoffeeOrderRepository orders, OutboxRepository outbox,
                              CoffeeOrderPayloadSerializer payloadSerializer, EventMetadataFactory eventMetadataFactory,
                              @Value("${app.coffee.vip-customers:}") String vipCustomers,
                              @Value("${app.coffee.partitions}") int partitions) {
        this.orders = orders;
        this.outbox = outbox;
        this.payloadSerializer = payloadSerializer;
        this.eventMetadataFactory = eventMetadataFactory;
        this.vipPartitionRule = VipPartitionRule.parse(vipCustomers);
        this.partitions = partitions;
    }

    @Transactional
    public CoffeeOrderResponse place(CoffeeOrderRequest request, String correlationId) {
        var event = CoffeeOrderEvent.placed(eventMetadataFactory.newEvent(CoffeeOrderEvent.PLACED, correlationId), request);
        var order = buildCoffeeOrder(event);
        orders.insert(order);
        var key = order.getId().toString();
        outbox.append(new OutboxEvent(event.metadata().eventId(), TOPIC, key, CoffeeOrderEvent.PLACED,
                payloadSerializer.serialize(order), vipPartitionRule.partition(event.name(), key, partitions), correlationId));
        return CoffeeOrderResponse.from(order, event.metadata());
    }

    private static CoffeeOrder buildCoffeeOrder(CoffeeOrderEvent event) {
        var address = Address.newBuilder()
                .setAddressLine1("123 Coffee Lane")
                .setCity("Seattle")
                .setStateProvince("WA")
                .setCountry("USA")
                .setZip("98101")
                .build();

        var store = Store.newBuilder()
                .setId(1)
                .setAddress(address)
                .build();

        var lineItem = OrderLineItem.newBuilder()
                .setName("Latte")
                .setSize(Size.MEDIUM)
                .setQuantity(1)
                .setCost(new BigDecimal("4.50"))
                .build();

        return CoffeeOrder.newBuilder()
                .setMetadata(event.metadata().toAvro())
                .setId(event.orderId())
                .setName(event.name())
                .setNickName(event.nickName())
                .setStore(store)
                .setOrderLineItems(List.of(lineItem))
                .setOrderedTime(Instant.now().truncatedTo(ChronoUnit.MILLIS))   // timestamp-millis: same value in the row and the event
                .setPickUp(PickUp.IN_STORE)
                .setStatus("NEW")
                .build();
    }
}
