import time
import uuid
import hashlib


def _gen_seed(text: str) -> int:
    return int(
        hashlib.sha1(
            text.encode(),
        ).hexdigest(),
        16,
    )


def _gen_sku_price(sku: str) -> float:
    seed = _gen_seed(sku)
    return (1 + (seed % 10000)) / 100


def _gen_timestamp() -> str:
    # return datetime.datetime.utcnow().isoformat().replace("T", " ")
    return int(time.time() * 1000)


def gen_session_id() -> str:
    return uuid.uuid4().hex


def gen_client_id(
    session_id: str,
    max_clients: int = 1,
) -> str:
    seed = _gen_seed(session_id)
    return f"User_{ seed % max_clients }"


def gen_sku(max_skus: int = 1) -> str:
    seed = _gen_seed(uuid.uuid4().hex)
    return f"SKU_{ seed % max_skus }"


def gen_shop_id(
    session_id: str,
    max_shops: int = 1,
) -> str:
    seed = _gen_seed(session_id[::-1])
    return f"SHOP_{ seed % max_shops }"


def gen_payload_status(
    session_id: str = None,
    client_id: str = None,
    status: int = None,
    max_clients: int = 1,
    max_shops: int = 1,
) -> dict:
    session_id_value = gen_session_id() if session_id is None else session_id
    payload = {
        "ts": _gen_timestamp(),
        "session_id": session_id_value,
    }
    if isinstance(status, int):
        payload["client_id"] = (
            gen_client_id(
                session_id_value,
                max_clients=max_clients,
            )
            if client_id is None
            else client_id
        )
        payload["status"] = status
        payload["shop_id"] = gen_shop_id(session_id_value, max_shops=max_shops)
    return payload


def gen_payload_basket(
    session_id: str = None,
    client_id: str = None,
    sku: str = None,
    qty: int = 1,
    max_clients: int = 1,
    max_skus: int = 1,
    max_shops: int = 1,
) -> dict:
    payload = gen_payload_status(
        session_id=session_id,
        client_id=client_id,
        max_clients=max_clients,
        max_shops=max_shops,
    )

    sku_value = gen_sku(max_skus=max_skus) if sku is None else sku

    return {
        **payload,
        "sku": sku_value,
        "qty": qty,
        "unit_price": _gen_sku_price(sku_value),
    }
