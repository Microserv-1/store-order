import os
from types import SimpleNamespace
import pytest

os.environ.setdefault("DATABASE_URL", "sqlite:///./test_order.db")
os.environ.setdefault("JWT_SECRET", "order-unit-test-secret-with-32-plus-bytes!!")
os.environ.setdefault("RABBITMQ_URL", "memory://local")
import app.main as order_main
from app.main import InMemoryBroker, RabbitMQBroker

class Channel:
    def __init__(self):
        self.acked = []
        self.requeued = []

    def basic_get(self, **kwargs):
        assert kwargs == {"queue": "order.events", "auto_ack": False}
        return SimpleNamespace(delivery_tag=7), None, b'{"order_id":"fixture"}'

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue):
        self.requeued.append((delivery_tag, requeue))

def test_ack_waits_for_successful_processing_and_failure_is_requeued():
    channel = Channel()
    closed = []
    broker = RabbitMQBroker("amqp://unused")
    broker._connect = lambda: (SimpleNamespace(close=lambda: closed.append(True)), channel)

    def database_failure(raw):
        assert channel.acked == []
        raise RuntimeError("simulated database commit failure")

    with pytest.raises(RuntimeError):
        broker.consume_one("order.events", database_failure)
    assert channel.acked == []
    assert channel.requeued == [(7, True)]
    assert closed == [True]

    processed = []
    def success(raw):
        assert channel.acked == []
        processed.append(raw)
    assert broker.consume_one("order.events", success)
    assert processed and channel.acked == [7]
    assert closed == [True, True]


def test_in_memory_processing_failure_preserves_message():
    broker = InMemoryBroker()
    broker.publish("order.events", "fixture")

    def failure(raw):
        raise RuntimeError("commit failed")

    with pytest.raises(RuntimeError, match="commit failed"):
        broker.consume_one("order.events", failure)
    received = []
    assert broker.consume_one("order.events", received.append)
    assert received == ["fixture"]
    assert not broker.consume_one("order.events", received.append)


def test_failed_dlq_publication_requeues_original(monkeypatch):
    broker = InMemoryBroker()
    broker.publish(order_main.ORDER_EVENTS_QUEUE, "invalid json")
    original_publish = broker.publish

    def fail_dlq(queue, raw):
        if queue == order_main.ORDER_EVENTS_DLQ:
            raise RuntimeError("DLQ unavailable")
        original_publish(queue, raw)

    monkeypatch.setattr(broker, "publish", fail_dlq)
    monkeypatch.setattr(order_main, "broker", broker)
    with pytest.raises(RuntimeError, match="DLQ unavailable"):
        broker.consume_one(order_main.ORDER_EVENTS_QUEUE, order_main.process_single_event_message)
    assert broker.get_one(order_main.ORDER_EVENTS_QUEUE) == "invalid json"


@pytest.mark.parametrize("failed", [False, True])
def test_publish_requires_broker_confirmation_and_routing(monkeypatch, failed):
    operations = []

    class PublishingChannel:
        def queue_declare(self, **kwargs):
            operations.append("declare")

        def confirm_delivery(self):
            operations.append("confirm")

        def basic_publish(self, **kwargs):
            assert operations == ["declare", "confirm"]
            assert kwargs["mandatory"] is True
            assert kwargs["body"] == b"fixture"
            assert kwargs["properties"].delivery_mode == 2
            operations.append("publish")
            if failed:
                raise RuntimeError("broker rejected publication")

    broker = RabbitMQBroker("amqp://unused")
    monkeypatch.setattr(broker, "_connect", lambda: (
        SimpleNamespace(close=lambda: operations.append("close")), PublishingChannel()
    ))
    if failed:
        with pytest.raises(RuntimeError, match="broker rejected publication"):
            broker.publish("order.events", "fixture")
    else:
        broker.publish("order.events", "fixture")
    assert operations == ["declare", "confirm", "publish", "close"]
