# SPKD003 · Count orders for every customer
# Completed: 2026-11-12
# Task: Return customer_id and order_count for every customer, including customers with no orders. Count physical order rows. Ignore orders with NULL or unknown customer_id. Customer IDs are unique and order_id is never NULL.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD003: Count orders for every customer
# Return customer_id and order_count for every customer, including customers with no orders. Count physical order rows. Ignore orders with NULL or unknown customer_id. Customer IDs are unique and order_id is never NULL.
# Implement solve using the supplied DataFrames. Return a DataFrame with exactly the expected columns. Row order does not matter; duplicates do. Use built-in PySpark functions. The starter includes runnable, visible assertions.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD003").getOrCreate()

    def solve(customers, orders):
        # Write your transformation here. Return only the requested columns.
        joined_df = customers.join(orders, on ='customer_id', how = 'left')
        ag_df = joined_df.groupBy(F.col("customer_id")).agg(F.coalesce(F.count(F.col("order_id")),F.lit(0)).alias("order_count"))
        return ag_df.select(F.col("customer_id"),F.col("order_count"))

    # Visible checks: normal input, boundary/duplicate cases and empty input.
    CASES = [{"name": "Customer with no orders", "inputs": {"customers": {"schema": "customer_id int", "rows": [[1], [2], [3]]}, "orders": {"schema": "order_id string, customer_id int", "rows": [["a", 1], ["b", 1], ["c", 2], ["d", None], ["e", 99]]}}, "expected": [{"customer_id": 1, "order_count": 2}, {"customer_id": 2, "order_count": 1}, {"customer_id": 3, "order_count": 0}]}, {"name": "No orders", "inputs": {"customers": {"schema": "customer_id int", "rows": [[1]]}, "orders": {"schema": "order_id string, customer_id int", "rows": []}}, "expected": [{"customer_id": 1, "order_count": 0}]}, {"name": "No customers", "inputs": {"customers": {"schema": "customer_id int", "rows": []}, "orders": {"schema": "order_id string, customer_id int", "rows": [["a", 1]]}}, "expected": []}]
    EXPECTED_COLUMNS = ["customer_id", "order_count"]

    def normalized(rows):
        return sorted(json.dumps(row, sort_keys=True) for row in rows)

    for case in CASES:
        inputs = {name: spark.createDataFrame(spec["rows"], spec["schema"])
                  for name, spec in case["inputs"].items()}
        result = solve(**inputs)
        assert result is not None, "solve must return a DataFrame"
        assert result.columns == EXPECTED_COLUMNS, ("Wrong columns", result.columns)
        actual = [row.asDict(recursive=True) for row in result.collect()]
        assert normalized(actual) == normalized(case["expected"]), (case["name"], actual, case["expected"])
        print("PASS:", case["name"])

    print("ALL 3 CHECKS PASSED")

main()
