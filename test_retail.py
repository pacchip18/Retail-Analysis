
import pytest
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config

#testing the customer data frame count
def test_customerdf_count(spark):
    customer_count = read_customers(spark, "LOCAL").count()
    assert customer_count == 12435

#testing the orders data frame count
@pytest.mark.slow
def test_orders_data(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

#testing the filter closed orders
@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    closed_orders = filter_closed_orders(orders_df).count()
    assert closed_orders == 7556

#testing the read app config
@pytest.mark.skip("work in progress so skip this")
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

#testing the count order state
@pytest.mark.transformation()
def test_count_order_state(spark, expected_results):
    customer_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customer_df)
    assert actual_results.collect() == expected_results.collect()

#testing the count closed orders
@pytest.mark.skip()
def test_count_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_rec = filter_orders_generic(orders_df, "CLOSED").count()
    assert filtered_rec == 7556

#testing the count pending orders
@pytest.mark.skip()
def test_count_pending_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_rec = filter_orders_generic(orders_df, "PENDING_PAYMENT").count()
    assert filtered_rec == 15030

#testing the count complete orders
@pytest.mark.skip()
def test_count_complete_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_rec = filter_orders_generic(orders_df, "COMPLETE").count()
    assert filtered_rec == 22899

#testing the check count
@pytest.mark.parametrize(
        "status, count", 
        [("CLOSED", 7556),
        ("PENDING_PAYMENT", 15030),
        ("COMPLETE", 22900)
        ]
)

#testing the check count    
@pytest.mark.latest()
def test_check_count(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_rec_count = filter_orders_generic(orders_df, status).count()
    assert filtered_rec_count == count