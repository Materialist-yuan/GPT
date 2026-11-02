#!/usr/bin/env python3
"""Functional checks against a running API; uses a new player each time."""
import json
import os
import urllib.error
import urllib.request
import uuid

base = os.environ.get("API_URL", "http://127.0.0.1:8080").rstrip("/")
player = "smoke_" + uuid.uuid4().hex


def request(path, body=None, status=200):
    req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        response = urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        result = json.load(response)
        assert response.status == status, (response.status, result)
        return result


assert request("/healthz")["status"] == "ok"
assert request("/catalog")["sword"] == 80
created = request("/players", {"player_id": player, "request_id": "create"}, 201)
assert created["player"]["coins"] == 100
assert request("/players", {"player_id": player, "request_id": "create"}, 201)["replayed"]
path = "/players/" + player
bought = request(path + "/purchases", {"request_id": "buy", "item": "sword"})
assert bought["player"]["coins"] == 20
assert bought["player"]["inventory"] == {"sword": 1}
assert request(path + "/purchases", {"request_id": "buy", "item": "sword"})["replayed"]
assert request(path + "/purchases", {"request_id": "poor", "item": "shield"}, 409)["code"] == "insufficient_coins"
assert request(path + "/rewards", {"request_id": "reward", "amount": 50})["player"]["coins"] == 70
assert request(path + "/purchases", {"request_id": "poor", "item": "shield"}, 409)["replayed"]
assert request(path + "/rewards", {"request_id": "reward", "amount": 51}, 409)["code"] == "idempotency_conflict"
final = request(path)
assert final["coins"] == 70 and final["inventory"] == {"sword": 1}, final
print("PASS: health, catalog, create, purchase, reward, idempotency, insufficient funds, conflict; player=" + player)
