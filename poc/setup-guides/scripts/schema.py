ORDER_EVENT_SCHEMA = {
    "type": "record",
    "name": "OrderEvent",
    "namespace": "com.poc.events",
    "fields": [
        {"name": "order_id",    "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "product_id",  "type": "string"},
        {"name": "amount",      "type": "double"},
        {"name": "currency",    "type": "string"},
        {"name": "timestamp",   "type": "long"},   # epoch milliseconds
        {"name": "status",      "type": "string"}
    ]
}