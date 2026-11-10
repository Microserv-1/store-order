import os
import time
import uuid

os.environ["DATABASE_URL"] = "sqlite:///./test_order.db"
os.environ["JWT_SECRET"] = "order-unit-test-secret-with-32-plus-bytes!!"
os.environ["RABBITMQ_URL"] = "memory://local"
os.environ["PRODUCT_URL"] = "http://product:8000"
os.environ["EXCHANGE_URL"] = "http://exchange:8000"

from fastapi.testclient import TestClient
import httpx
import jwt
import app.main as order_main
from app.main import app, broker, drain_queue_once


def make_token(sub: str | None = None) -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "sub": sub or str(uuid.uuid4()),
            "iss": "store-auth",
            "aud": "store-api",
            "iat": now,
            "exp": now + 600,
        },
        os.environ["JWT_SECRET"],
        algorithm="HS256",
    )


def test_order_creation_isolation_conversion_and_async_messaging():
    p1_id = "0195abfb-7074-73a9-9d26-b4b9fbaab0a8"
    p2_id = "0195abfe-e416-7052-be3b-27cdaf12a984"
    products_db = {
        p1_id: {"id": p1_id, "name": "Tomato", "price": 10.12, "unit": "kg"},
        p2_id: {"id": p2_id, "name": "Cheese", "price": 0.62, "unit": "slice"},
    }

    def mock_upstream(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.startswith("http://product:8000/products/"):
            pid = url.rsplit("/", 1)[-1]
            if pid in products_db:
                return httpx.Response(200, json=products_db[pid])
            return httpx.Response(404, json={"detail": "Product not found"})
        if url == "http://exchange:8000/exchanges/USD/BRL":
            return httpx.Response(
                200,
                json={"sell": 6.0, "buy": 5.8, "date": "2026-11-10 14:00:00", "id-account": "acc"},
            )
        if url.startswith("http://exchange:8000/exchanges/USD/"):
            return httpx.Response(422, json={"detail": "Unsupported currency code"})
        return httpx.Response(503, text="Unexpected upstream")

    order_main.http_client_factory = lambda: httpx.Client(transport=httpx.MockTransport(mock_upstream))

    user1 = str(uuid.uuid4())
    user2 = str(uuid.uuid4())
    auth1 = {"Authorization": f"Bearer {make_token(user1)}"}
    auth2 = {"Authorization": f"Bearer {make_token(user2)}"}

    with TestClient(app) as client:
        assert client.get("/health-check").status_code == 200
        assert client.get("/ready").status_code == 200
        assert client.get("/orders").status_code == 401

        # Nonexistent product returns 400 Bad Request
        bad_order = client.post(
            "/orders",
            json={"items": [{"idProduct": "nonexistent-id", "quantity": 1}]},
            headers=auth1,
        )
        assert bad_order.status_code == 400

        # Valid order creation calculates exact USD totals (2 * 10.12 + 10 * 0.62 = 20.24 + 6.20 = 26.44)
        create_res = client.post(
            "/orders",
            json={
                "items": [
                    {"idProduct": p1_id, "quantity": 2},
                    {"idProduct": p2_id, "quantity": 10},
                ]
            },
            headers=auth1,
        )
        assert create_res.status_code == 201
        created = create_res.json()
        order_id = created["id"]
        assert created["total"] == 26.44
        assert created["items"][0]["total"] == 20.24
        assert created["items"][1]["total"] == 6.2

        # List orders isolates by user
        list1 = client.get("/orders", headers=auth1)
        assert list1.status_code == 200
        assert len(list1.json()) == 1
        assert list1.json()[0]["id"] == order_id
        assert list1.json()[0]["total"] == 26.44

        list2 = client.get("/orders", headers=auth2)
        assert list2.status_code == 200
        assert list2.json() == []

        # Detail of another user's order returns 404
        assert client.get(f"/orders/{order_id}", headers=auth2).status_code == 404

        # Detail in default USD
        detail_usd = client.get(f"/orders/{order_id}", headers=auth1)
        assert detail_usd.status_code == 200
        assert detail_usd.json()["currency"] == "USD"
        assert detail_usd.json()["total"] == 26.44

        # Detail converted to BRL (sell = 6.0 -> 20.24 * 6 = 121.44, 6.20 * 6 = 37.20, total = 158.64)
        detail_brl = client.get(f"/orders/{order_id}?currency=BRL", headers=auth1)
        assert detail_brl.status_code == 200
        brl_body = detail_brl.json()
        assert brl_body["currency"] == "BRL"
        assert brl_body["items"][0]["total"] == 121.44
        assert brl_body["items"][1]["total"] == 37.2
        assert brl_body["total"] == 158.64

        # Stored USD values remain untouched after conversion query
        detail_usd_after = client.get(f"/orders/{order_id}", headers=auth1)
        assert detail_usd_after.status_code == 200
        assert detail_usd_after.json()["currency"] == "USD"
        assert detail_usd_after.json()["total"] == 26.44

        # Unsupported currency returns 422
        assert client.get(f"/orders/{order_id}?currency=XYZ", headers=auth1).status_code == 422

        # Async messaging: verify event was consumed and notification created
        drain_queue_once()
        notif_res = client.get(f"/orders/{order_id}/notifications", headers=auth1)
        assert notif_res.status_code == 200
        assert notif_res.json()["status"] == "PROCESSED"
        assert notif_res.json()["notification"]["duplicate_deliveries"] == 0

        # Replay duplicate event -> idempotency skips duplicate processing
        replay_res = client.post(f"/orders/{order_id}/events/replay", headers=auth1)
        assert replay_res.status_code == 200
        assert replay_res.json()["notification"]["duplicate_deliveries"] == 1

        # Demonstrate consumer unavailability & resumption:
        # Pause consumer -> create order -> order succeeds (201) while notification stays PENDING
        client.post("/orders/events/consumer", json={"enabled": False}, headers=auth1)
        paused_order = client.post(
            "/orders",
            json={"items": [{"idProduct": p1_id, "quantity": 1}]},
            headers=auth1,
        )
        assert paused_order.status_code == 201
        paused_order_id = paused_order.json()["id"]
        assert client.get(f"/orders/{paused_order_id}/notifications", headers=auth1).json()["status"] == "PENDING"

        # Resume consumer -> queued event is drained and notification becomes PROCESSED
        client.post("/orders/events/consumer", json={"enabled": True}, headers=auth1)
        assert client.get(f"/orders/{paused_order_id}/notifications", headers=auth1).json()["status"] == "PROCESSED"

        # Also test broker temporary outage during order creation -> Outbox recovers when broker comes back
        broker.available = False
        outage_order = client.post(
            "/orders",
            json={"items": [{"idProduct": p2_id, "quantity": 5}]},
            headers=auth1,
        )
        assert outage_order.status_code == 201
        outage_order_id = outage_order.json()["id"]
        broker.available = True
        drain_queue_once()
        assert client.get(f"/orders/{outage_order_id}/notifications", headers=auth1).json()["status"] == "PROCESSED"
