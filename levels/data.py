"""Small, hand-made datasets for the level tests - small enough to check results by eye."""
from datetime import date, datetime


def junior(spark):
    customers = spark.createDataFrame([
        (1, "Ana", "NA"), (2, "Ben", "EMEA"), (3, "Chen", "APAC"),
        (4, "Dee", "NA"), (5, "Eli", "LATAM"), (6, "Fay", "EMEA"),
    ], "customer_id int, name string, region string")
    orders = spark.createDataFrame([
        (101, 1, 120.0, "delivered"), (102, 1, 35.5, "returned"), (103, 2, 80.0, "delivered"),
        (104, 2, 220.0, "delivered"), (105, 3, 49.99, "shipped"), (106, 3, 150.0, "delivered"),
        (107, 4, 10.0, "delivered"), (108, 4, 300.0, "placed"), (109, 1, 60.0, "delivered"),
        (110, 2, 149.99, "delivered"),
    ], "order_id int, customer_id int, amount double, status string")
    return {"customers": customers, "orders": orders}


def mid(spark):
    raw_orders = spark.createDataFrame([
        (1, "placed", datetime(2025, 1, 1, 9)), (1, "shipped", datetime(2025, 1, 2, 9)),
        (1, "delivered", datetime(2025, 1, 4, 9)), (2, "placed", datetime(2025, 1, 1, 10)),
        (3, "placed", datetime(2025, 1, 3, 8)), (3, "returned", datetime(2025, 1, 9, 8)),
        (3, "delivered", datetime(2025, 1, 6, 8)),     # arrived out of order: older than 'returned'
        (4, "shipped", datetime(2025, 1, 5, 12)),
    ], "order_id int, status string, ingested_at timestamp")
    orders = spark.createDataFrame([
        (1, 10, date(2025, 1, 5), 100.0), (2, 10, date(2025, 1, 20), 50.0), (3, 10, date(2025, 2, 3), 75.0),
        (4, 10, date(2025, 3, 15), 20.0), (5, 20, date(2025, 1, 11), 200.0), (6, 20, date(2025, 2, 11), 90.0),
        (7, 20, date(2025, 2, 11), 60.0), (8, 30, date(2025, 3, 1), 300.0), (9, 30, date(2025, 3, 2), 10.0),
        (10, 10, date(2025, 3, 15), 5.0),
    ], "order_id int, customer_id int, order_date date, amount double")
    dim_hist = spark.createDataFrame([
        (10, "NA", date(2024, 1, 1), date(2025, 2, 1)), (10, "EMEA", date(2025, 2, 1), None),
        (20, "APAC", date(2024, 6, 1), None),
        (30, "NA", date(2024, 1, 1), date(2025, 1, 15)), (30, "LATAM", date(2025, 1, 15), date(2025, 3, 1)),
        (30, "APAC", date(2025, 3, 1), None),
    ], "customer_id int, region string, valid_from date, valid_to date")
    sales = spark.createDataFrame([
        (1, 10, date(2025, 1, 31)), (2, 10, date(2025, 2, 1)), (3, 20, date(2025, 2, 2)),
        (4, 30, date(2025, 1, 14)), (5, 30, date(2025, 2, 28)), (6, 30, date(2025, 3, 1)),
        (7, 99, date(2025, 3, 1)),                       # unknown customer -> region NULL
    ], "sale_id int, customer_id int, sale_date date")
    return {"raw_orders": raw_orders, "orders": orders, "dim_hist": dim_hist, "sales": sales}


def senior(spark):
    events = spark.createDataFrame([
        ("u1", datetime(2025, 5, 1, 9, 0)), ("u1", datetime(2025, 5, 1, 9, 10)),
        ("u1", datetime(2025, 5, 1, 9, 35)), ("u1", datetime(2025, 5, 1, 10, 30)),
        ("u1", datetime(2025, 5, 1, 10, 45)), ("u2", datetime(2025, 5, 1, 12, 0)),
        ("u2", datetime(2025, 5, 1, 12, 31)), ("u2", datetime(2025, 5, 1, 12, 40)),
        ("u3", datetime(2025, 5, 1, 8, 0)), ("u3", datetime(2025, 5, 1, 8, 30)),   # exactly 30 min: same session
    ], "user_id string, ts timestamp")
    change_log = spark.createDataFrame([
        (1, "NA", date(2025, 1, 1), datetime(2025, 1, 1, 1)),
        (1, "EMEA", date(2025, 3, 1), datetime(2025, 3, 1, 1)),
        (1, "APAC", date(2025, 2, 1), datetime(2025, 3, 5, 1)),     # late-arriving change
        (1, "LATAM", date(2025, 3, 1), datetime(2025, 3, 7, 1)),    # correction for 03-01: latest ingest wins
        (2, "NA", date(2025, 1, 1), datetime(2025, 1, 1, 1)),
        (2, "NA", date(2025, 2, 1), datetime(2025, 2, 1, 1)),       # no-op "change": must not create a version
        (2, "EMEA", date(2025, 4, 1), datetime(2025, 4, 1, 1)),
        (3, "APAC", date(2025, 6, 1), datetime(2025, 6, 1, 1)),
    ], "customer_id int, region string, effective_date date, ingested_at timestamp")
    orders = spark.createDataFrame([
        (1, date(2025, 1, 1)), (1, date(2025, 1, 2)), (1, date(2025, 1, 2)), (1, date(2025, 1, 3)),
        (1, date(2025, 1, 10)), (1, date(2025, 1, 11)), (2, date(2025, 1, 5)), (2, date(2025, 1, 7)),
        (3, date(2025, 2, 27)), (3, date(2025, 2, 28)), (3, date(2025, 3, 1)), (3, date(2025, 3, 2)),
    ], "customer_id int, order_date date")
    return {"events": events, "change_log": change_log, "orders": orders}


def senior_lesson(spark):
    """Walkthrough-only data (different from the senior test on purpose)."""
    t = lambda h, m: datetime(2025, 5, 1, h, m)  # noqa: E731
    readings = spark.createDataFrame([
        ("m1", t(8, 0), "OK"), ("m1", t(8, 5), "OK"), ("m1", t(8, 10), "FAIL"), ("m1", t(8, 15), "FAIL"),
        ("m1", t(8, 20), "OK"), ("m1", t(8, 25), "FAIL"),
        ("m2", t(8, 0), "FAIL"), ("m2", t(8, 5), "OK"), ("m2", t(8, 10), "FAIL"),
        ("m3", t(8, 0), "OK"), ("m3", t(8, 5), "OK"),
    ], "machine string, ts timestamp, status string")
    price_log = spark.createDataFrame([
        ("sku1", 10.0, date(2025, 1, 1), datetime(2025, 1, 1)),
        ("sku1", 12.0, date(2025, 4, 1), datetime(2025, 4, 1)),
        ("sku1", 11.0, date(2025, 2, 15), datetime(2025, 4, 3)),   # late-arriving change
        ("sku1", 11.0, date(2025, 3, 1), datetime(2025, 4, 3)),    # no-op: same price as previous version
        ("sku2", 20.0, date(2025, 1, 1), datetime(2025, 1, 1)),
        ("sku2", 25.0, date(2025, 3, 1), datetime(2025, 3, 1)),
        ("sku2", 22.0, date(2025, 3, 1), datetime(2025, 3, 10)),   # correction of the 03-01 price
        ("sku3", 5.0, date(2025, 4, 1), datetime(2025, 4, 1)),     # not effective yet on 03-15
    ], "sku string, price double, effective_date date, ingested_at timestamp")
    return {"readings": readings, "price_log": price_log}


DATASETS = {"junior": junior, "mid": mid, "senior": senior, "senior_lesson": senior_lesson}
