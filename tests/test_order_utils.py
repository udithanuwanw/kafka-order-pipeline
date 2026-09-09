from order_utils import generate_order, validate_order


def test_generate_order_has_required_fields():
    order = generate_order(seq=0, invalid_rate=0.0)
    assert set(order.keys()) == {"orderId", "product", "price"}


def test_generate_order_id_uses_sequence():
    order = generate_order(seq=5, invalid_rate=0.0)
    assert order["orderId"] == "1005"


def test_generate_order_invalid_rate_zero_always_valid():
    for seq in range(50):
        order = generate_order(seq=seq, invalid_rate=0.0)
        assert order["price"] >= 0


def test_generate_order_invalid_rate_one_always_invalid():
    for seq in range(50):
        order = generate_order(seq=seq, invalid_rate=1.0)
        assert order["price"] < 0


def test_validate_order_valid():
    order = {"orderId": "1001", "product": "Item1", "price": 10.0}
    assert validate_order(order) is None


def test_validate_order_missing_order_id():
    order = {"orderId": "", "product": "Item1", "price": 10.0}
    assert validate_order(order) == "missing_order_id"


def test_validate_order_missing_product():
    order = {"orderId": "1001", "product": "", "price": 10.0}
    assert validate_order(order) == "missing_product"


def test_validate_order_negative_price():
    order = {"orderId": "1001", "product": "Item1", "price": -5.0}
    assert validate_order(order) == "negative_price"
