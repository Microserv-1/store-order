import collections
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
import os
import threading
import time
from typing import Callable, Optional
import uuid
from fastapi import Depends, FastAPI, HTTPException, Query, Request
import httpx
import pika
from prometheus_client import Counter
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, ForeignKey, Integer, Numeric, String, Text, create_engine, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from .common import identity, instrument, logger

engine = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, pool_size=5, max_overflow=5)
Session = sessionmaker(engine, expire_on_commit=False)

PRODUCT_URL = os.environ.get("PRODUCT_URL", "http://product:8000").rstrip("/")
EXCHANGE_URL = os.environ.get("EXCHANGE_URL", "http://exchange:8000").rstrip("/")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
ORDER_EVENTS_QUEUE = os.environ.get("ORDER_EVENTS_QUEUE", "order.events")
ORDER_EVENTS_DLQ = f"{ORDER_EVENTS_QUEUE}.dlq"

EVENTS_PUBLISHED = Counter("order_events_published_total", "Total order events published to broker")
EVENTS_CONSUMED = Counter("order_events_consumed_total", "Total order events processed by consumer")
EVENTS_DUPLICATE_SKIPPED = Counter("order_events_duplicate_skipped_total", "Duplicate order events skipped by idempotency check")
EVENTS_DLQ = Counter("order_events_dlq_total", "Failed order events routed to DLQ")


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(36), index=True)
    date: Mapped[str] = mapped_column(String(32))
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    items: Mapped[list["OrderItem"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class OrderItem(Base):
    __tablename__ = "order_items"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), index=True)
    product_id: Mapped[str] = mapped_column(String(36))
    quantity: Mapped[int] = mapped_column(Integer)
    total: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    order: Mapped[Order] = relationship(back_populates="items")


class OrderOutboxEvent(Base):
    __tablename__ = "order_outbox_events"
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(36), index=True)
    account_id: Mapped[str] = mapped_column(String(36), index=True)
    payload_json: Mapped[str] = mapped_column(Text)
    published: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[str] = mapped_column(String(32))


class OrderNotification(Base):
    __tablename__ = "order_notifications"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    account_id: Mapped[str] = mapped_column(String(36), index=True)
    event_id: Mapped[str] = mapped_column(String(36))
    status: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(String(255))
    duplicate_deliveries: Mapped[int] = mapped_column(Integer, default=0)
    processed_at: Mapped[str] = mapped_column(String(32))


class OrderConsumerControl(Base):
    __tablename__ = "order_consumer_control"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


class InMemoryBroker:
    """Thread-safe queue broker used when RABBITMQ_URL starts with memory:// for self-contained unit tests."""

    def __init__(self):
        self._queues: dict[str, collections.deque[str]] = collections.defaultdict(collections.deque)
        self._lock = threading.Lock()
        self.available = True

    def ping(self) -> bool:
        if not self.available:
            raise RuntimeError("In-memory broker simulated offline")
        return True

    def publish(self, queue: str, body: str) -> None:
        if not self.available:
            raise RuntimeError("In-memory broker simulated offline")
        with self._lock:
            self._queues[queue].append(body)

    def get_one(self, queue: str) -> Optional[str]:
        if not self.available:
            raise RuntimeError("In-memory broker simulated offline")
        with self._lock:
            q = self._queues[queue]
            return q.popleft() if q else None

    def queue_depth(self, queue: str) -> int:
        if not self.available:
            return 0
        with self._lock:
            return len(self._queues[queue])


class RabbitMQBroker:
    def __init__(self, url: str):
        self.url = url

    def _connect(self):
        params = pika.URLParameters(self.url)
        params.socket_timeout = 2.0
        params.blocked_connection_timeout = 2.0
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.queue_declare(queue=ORDER_EVENTS_DLQ, durable=True)
        ch.queue_declare(queue=ORDER_EVENTS_QUEUE, durable=True)
        return conn, ch

    def ping(self) -> bool:
        conn, _ = self._connect()
        conn.close()
        return True

    def publish(self, queue: str, body: str) -> None:
        conn, ch = self._connect()
        try:
            ch.queue_declare(queue=queue, durable=True)
            ch.basic_publish(
                exchange="",
                routing_key=queue,
                body=body.encode("utf-8"),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
            )
        finally:
            conn.close()

    def get_one(self, queue: str) -> Optional[str]:
        conn, ch = self._connect()
        try:
            method_frame, _header_frame, body = ch.basic_get(queue=queue, auto_ack=False)
            if method_frame is None:
                return None
            ch.basic_ack(delivery_tag=method_frame.delivery_tag)
            return body.decode("utf-8")
        finally:
            conn.close()

    def queue_depth(self, queue: str) -> int:
        conn, ch = self._connect()
        try:
            res = ch.queue_declare(queue=queue, durable=True, passive=True)
            return res.method.message_count
        finally:
            conn.close()


def build_broker(url: str):
    if url.startswith("memory://"):
        return InMemoryBroker()
    return RabbitMQBroker(url)


broker = build_broker(RABBITMQ_URL)
_worker_stop = threading.Event()
_worker_wake = threading.Event()


def default_http_client_factory() -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, connect=2.5), follow_redirects=False)


http_client_factory: Callable[[], httpx.Client] = default_http_client_factory


def round_money(value: float | str | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def is_consumer_enabled() -> bool:
    with Session() as session:
        ctrl = session.get(OrderConsumerControl, 1)
        return True if ctrl is None else bool(ctrl.enabled)


def set_consumer_enabled(enabled: bool) -> bool:
    with Session() as session:
        ctrl = session.get(OrderConsumerControl, 1)
        if ctrl is None:
            ctrl = OrderConsumerControl(id=1, enabled=enabled)
            session.add(ctrl)
        else:
            ctrl.enabled = enabled
        session.commit()
    if enabled:
        _worker_wake.set()
    return enabled


def flush_outbox() -> int:
    published_count = 0
    with Session() as session:
        pending = session.scalars(
            select(OrderOutboxEvent)
            .where(OrderOutboxEvent.published.is_(False))
            .order_by(OrderOutboxEvent.created_at)
            .limit(50)
        ).all()
        for ev in pending:
            try:
                broker.publish(ORDER_EVENTS_QUEUE, ev.payload_json)
                ev.published = True
                session.commit()
                EVENTS_PUBLISHED.inc()
                published_count += 1
            except Exception as exc:
                session.rollback()
                logger.warning(json.dumps({"service": "order", "event": "outbox_publish_deferred", "error": str(exc)}))
                break
    return published_count


def process_single_event_message(raw_message: str) -> None:
    try:
        payload = json.loads(raw_message)
        order_id = str(payload["order_id"])
        account_id = str(payload["account_id"])
        event_id = str(payload.get("event_id") or uuid.uuid4())
        total_usd = float(payload.get("total_usd", 0.0))
    except Exception as exc:
        logger.error(json.dumps({"service": "order", "event": "invalid_event_payload", "error": str(exc)}))
        try:
            broker.publish(ORDER_EVENTS_DLQ, raw_message)
            EVENTS_DLQ.inc()
        except Exception:
            pass
        return

    with Session() as session:
        existing = session.scalar(select(OrderNotification).where(OrderNotification.order_id == order_id))
        if existing is not None:
            existing.duplicate_deliveries += 1
            session.commit()
            EVENTS_DUPLICATE_SKIPPED.inc()
            logger.info(
                json.dumps(
                    {
                        "service": "order",
                        "event": "duplicate_order_event_skipped",
                        "order_id": order_id,
                        "event_id": event_id,
                        "duplicate_deliveries": existing.duplicate_deliveries,
                    }
                )
            )
            return

        notification = OrderNotification(
            id=str(uuid.uuid4()),
            order_id=order_id,
            account_id=account_id,
            event_id=event_id,
            status="PROCESSED",
            message=f"Order {order_id} confirmed asynchronously (total USD {total_usd:.2f})",
            duplicate_deliveries=0,
            processed_at=now_iso(),
        )
        session.add(notification)
        try:
            session.commit()
            EVENTS_CONSUMED.inc()
            logger.info(
                json.dumps(
                    {
                        "service": "order",
                        "event": "order_notification_processed",
                        "order_id": order_id,
                        "event_id": event_id,
                    }
                )
            )
        except IntegrityError:
            session.rollback()
            existing_after = session.scalar(select(OrderNotification).where(OrderNotification.order_id == order_id))
            if existing_after is not None:
                existing_after.duplicate_deliveries += 1
                session.commit()
            EVENTS_DUPLICATE_SKIPPED.inc()


def drain_queue_once(max_messages: int = 50) -> int:
    flush_outbox()
    if not is_consumer_enabled():
        return 0
    processed = 0
    for _ in range(max_messages):
        try:
            raw = broker.get_one(ORDER_EVENTS_QUEUE)
        except Exception as exc:
            logger.warning(json.dumps({"service": "order", "event": "broker_consume_error", "error": str(exc)}))
            break
        if raw is None:
            break
        process_single_event_message(raw)
        processed += 1
    return processed


def background_worker():
    while not _worker_stop.is_set():
        try:
            drain_queue_once()
        except Exception as exc:
            logger.warning(json.dumps({"service": "order", "event": "worker_cycle_error", "error": str(exc)}))
        _worker_wake.wait(timeout=0.5)
        _worker_wake.clear()


@asynccontextmanager
async def lifespan(app):
    with engine.begin() as connection:
        if connection.dialect.name == "postgresql":
            connection.execute(text("SELECT pg_advisory_xact_lock(20261111)"))
        Base.metadata.create_all(connection)
    with Session() as session:
        if session.get(OrderConsumerControl, 1) is None:
            session.add(OrderConsumerControl(id=1, enabled=True))
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
    _worker_stop.clear()
    worker_thread = threading.Thread(target=background_worker, name="order-events-worker", daemon=True)
    worker_thread.start()
    yield
    _worker_stop.set()
    _worker_wake.set()
    worker_thread.join(timeout=3.0)
    engine.dispose()


app = FastAPI(title="Store Order", lifespan=lifespan)
instrument(app)


class OrderItemIn(BaseModel):
    idProduct: str = Field(min_length=1, max_length=36)
    quantity: int = Field(gt=0, le=100_000)


class OrderIn(BaseModel):
    items: list[OrderItemIn] = Field(min_length=1, max_length=100)


class ConsumerControlIn(BaseModel):
    enabled: bool


def fetch_product(client: httpx.Client, product_id: str, authorization: str) -> dict:
    try:
        response = client.get(
            f"{PRODUCT_URL}/products/{product_id}",
            headers={"Authorization": authorization},
        )
    except httpx.HTTPError as exc:
        logger.error(json.dumps({"service": "order", "event": "product_upstream_error", "error": str(exc)}))
        raise HTTPException(503, "Product service unavailable")
    if response.status_code == 404:
        raise HTTPException(400, f"Product does not exist: {product_id}")
    if response.status_code != 200:
        raise HTTPException(503, "Product service returned unexpected status")
    return response.json()


def fetch_exchange_sell_rate(client: httpx.Client, target_currency: str, authorization: str) -> Decimal:
    cleaned = target_currency.strip().upper()
    if len(cleaned) != 3 or not cleaned.isalpha():
        raise HTTPException(422, f"Unsupported currency code: {target_currency}")
    try:
        response = client.get(
            f"{EXCHANGE_URL}/exchanges/USD/{cleaned}",
            headers={"Authorization": authorization},
        )
    except httpx.HTTPError as exc:
        logger.error(json.dumps({"service": "order", "event": "exchange_upstream_error", "error": str(exc)}))
        raise HTTPException(503, "Exchange service unavailable")
    if response.status_code == 422:
        raise HTTPException(422, f"Unsupported currency code: {cleaned}")
    if response.status_code != 200:
        raise HTTPException(503, "Exchange service unavailable")
    data = response.json()
    try:
        sell_rate = Decimal(str(data["sell"]))
        if sell_rate <= Decimal("0"):
            raise ValueError("Non-positive exchange rate")
        return sell_rate
    except (KeyError, ValueError, TypeError):
        raise HTTPException(502, "Invalid response from Exchange service")


def serialize_order_item(item: OrderItem, rate: Decimal = Decimal("1.00")) -> dict:
    converted_total = round_money(Decimal(str(item.total)) * rate)
    return {
        "id": item.id,
        "product": {"id": item.product_id},
        "quantity": item.quantity,
        "total": float(converted_total),
    }


@app.get("/ready")
def ready():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    broker_ok = True
    try:
        broker.ping()
    except Exception:
        broker_ok = False
    return {"status": "ready", "broker_connected": broker_ok}


@app.get("/orders/events/summary")
def get_events_summary(account_id: str = Depends(identity)):
    drain_queue_once()
    broker_ok = False
    depth = 0
    try:
        broker_ok = bool(broker.ping())
        depth = broker.queue_depth(ORDER_EVENTS_QUEUE)
    except Exception:
        broker_ok = False
    with Session() as session:
        user_orders = session.scalars(select(Order).where(Order.account_id == account_id)).all()
        user_notifications = session.scalars(
            select(OrderNotification).where(OrderNotification.account_id == account_id).order_by(OrderNotification.processed_at)
        ).all()
        pending_outbox = session.scalars(
            select(OrderOutboxEvent).where(
                OrderOutboxEvent.account_id == account_id,
                OrderOutboxEvent.published.is_(False),
            )
        ).all()
        return {
            "consumer_active": is_consumer_enabled(),
            "broker_connected": broker_ok,
            "queue_depth": depth,
            "user_orders_total": len(user_orders),
            "user_notifications_processed": len(user_notifications),
            "user_outbox_pending": len(pending_outbox),
            "user_duplicate_deliveries_skipped": sum(n.duplicate_deliveries for n in user_notifications),
            "notifications": [
                {
                    "id": n.id,
                    "order_id": n.order_id,
                    "event_id": n.event_id,
                    "status": n.status,
                    "message": n.message,
                    "duplicate_deliveries": n.duplicate_deliveries,
                    "processed_at": n.processed_at,
                }
                for n in user_notifications
            ],
        }


@app.post("/orders/events/consumer")
def control_consumer(body: ConsumerControlIn, _account_id: str = Depends(identity)):
    state = set_consumer_enabled(body.enabled)
    processed = drain_queue_once() if state else 0
    return {"consumer_active": state, "drained_messages": processed}


@app.post("/orders", status_code=201)
def create_order(body: OrderIn, request: Request, account_id: str = Depends(identity)):
    authorization = request.headers.get("authorization", "")
    resolved_items: list[tuple[str, int, Decimal]] = []

    with http_client_factory() as client:
        product_cache: dict[str, dict] = {}
        for item_in in body.items:
            pid = item_in.idProduct.strip()
            if not pid:
                raise HTTPException(400, "Product ID must not be blank")
            if pid not in product_cache:
                product_cache[pid] = fetch_product(client, pid, authorization)
            unit_price = round_money(product_cache[pid]["price"])
            item_total = round_money(unit_price * Decimal(item_in.quantity))
            resolved_items.append((pid, item_in.quantity, item_total))

    order_id = str(uuid.uuid4())
    order_date = now_iso()
    order_total = round_money(sum((item[2] for item in resolved_items), Decimal("0.00")))
    event_id = str(uuid.uuid4())
    event_payload = {
        "event_id": event_id,
        "event_type": "order.created",
        "order_id": order_id,
        "account_id": account_id,
        "total_usd": float(order_total),
        "items_count": len(resolved_items),
        "created_at": order_date,
    }

    with Session() as session:
        order = Order(
            id=order_id,
            account_id=account_id,
            date=order_date,
            total=order_total,
            items=[
                OrderItem(
                    id=str(uuid.uuid4()),
                    order_id=order_id,
                    product_id=pid,
                    quantity=qty,
                    total=item_total,
                )
                for pid, qty, item_total in resolved_items
            ],
        )
        outbox = OrderOutboxEvent(
            event_id=event_id,
            order_id=order_id,
            account_id=account_id,
            payload_json=json.dumps(event_payload),
            published=False,
            created_at=order_date,
        )
        session.add(order)
        session.add(outbox)
        session.commit()
        response_payload = {
            "id": order.id,
            "date": order.date,
            "items": [serialize_order_item(i) for i in order.items],
            "total": float(round_money(order.total)),
        }

    flush_outbox()
    _worker_wake.set()
    return response_payload


@app.get("/orders")
def list_orders(account_id: str = Depends(identity)):
    with Session() as session:
        orders = session.scalars(
            select(Order).where(Order.account_id == account_id).order_by(Order.date, Order.id)
        ).all()
        return [
            {
                "id": o.id,
                "date": o.date,
                "total": float(round_money(o.total)),
            }
            for o in orders
        ]


@app.get("/orders/{order_id}")
def get_order(
    order_id: str,
    request: Request,
    currency: Optional[str] = Query(default=None, max_length=10),
    account_id: str = Depends(identity),
):
    with Session() as session:
        order = session.get(Order, order_id)
        if order is None or order.account_id != account_id:
            raise HTTPException(404, "Order not found")

        target_currency = (currency or "USD").strip().upper()
        if not target_currency:
            target_currency = "USD"

        rate = Decimal("1.00")
        if target_currency != "USD":
            authorization = request.headers.get("authorization", "")
            with http_client_factory() as client:
                rate = fetch_exchange_sell_rate(client, target_currency, authorization)

        serialized_items = [serialize_order_item(i, rate=rate) for i in order.items]
        if target_currency == "USD":
            total_value = float(round_money(order.total))
        else:
            total_value = float(round_money(sum((Decimal(str(i["total"])) for i in serialized_items), Decimal("0.00"))))

        return {
            "id": order.id,
            "date": order.date,
            "currency": target_currency,
            "items": serialized_items,
            "total": total_value,
        }


@app.get("/orders/{order_id}/notifications")
def get_order_notifications(order_id: str, account_id: str = Depends(identity)):
    drain_queue_once()
    with Session() as session:
        order = session.get(Order, order_id)
        if order is None or order.account_id != account_id:
            raise HTTPException(404, "Order not found")
        notification = session.scalar(select(OrderNotification).where(OrderNotification.order_id == order_id))
        if notification is None:
            return {"order_id": order_id, "status": "PENDING", "notification": None}
        return {
            "order_id": order_id,
            "status": notification.status,
            "notification": {
                "id": notification.id,
                "order_id": notification.order_id,
                "event_id": notification.event_id,
                "status": notification.status,
                "message": notification.message,
                "duplicate_deliveries": notification.duplicate_deliveries,
                "processed_at": notification.processed_at,
            },
        }


@app.post("/orders/{order_id}/events/replay")
def replay_order_event(order_id: str, account_id: str = Depends(identity)):
    with Session() as session:
        order = session.get(Order, order_id)
        if order is None or order.account_id != account_id:
            raise HTTPException(404, "Order not found")
        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": "order.created",
            "order_id": order.id,
            "account_id": account_id,
            "total_usd": float(round_money(order.total)),
            "items_count": len(order.items),
            "created_at": order.date,
        }
    broker.publish(ORDER_EVENTS_QUEUE, json.dumps(payload))
    EVENTS_PUBLISHED.inc()
    drain_queue_once()
    return get_order_notifications(order_id, account_id=account_id)
