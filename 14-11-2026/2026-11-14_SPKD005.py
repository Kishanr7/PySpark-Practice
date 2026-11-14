# SPKD005 · Find orders with unknown customers
# Completed: 2026-11-14
# Task: Return order_id and customer_id for orders whose customer_id is NULL or absent from customers. Do not return valid orders. Keep duplicate order rows.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD005: Find orders with unknown customers
# Return order_id and customer_id for orders whose customer_id is NULL or absent from customers. Do not return valid orders. Keep duplicate order rows.
# Implement solve using the supplied DataFrames. Return a DataFrame with exactly the expected columns. Row order does not matter; duplicates do. Use built-in PySpark functions. The starter includes runnable, visible assertions.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD005").getOrCreate()

    def solve(customers, orders):
        # Write your transformation here. Return only the requested columns.
        customers = customers.alias("cust")
        orders = orders.alias("ord")
    
        joined_df = orders.join(
            customers,
            F.col("ord.customer_id") == F.col("cust.customer_id"),
            how="left"
        )
    
        # Find unmatched orders
        filtered_df = joined_df.filter(
            F.col("cust.customer_id").isNull()
        )

    
        return filtered_df.select(
            F.col("ord.order_id").alias("order_id"),
            F.col("ord.customer_id").alias("customer_id")
        )

    # Visible checks: normal input, boundary/duplicate cases and empty input.
    CASES = [{"name": "NULL and unknown keys", "inputs": {"customers": {"schema": "customer_id int", "rows": [[1], [2]]}, "orders": {"schema": "order_id string, customer_id int", "rows": [["a", 1], ["b", 99], ["c", None]]}}, "expected": [{"order_id": "b", "customer_id": 99}, {"order_id": "c", "customer_id": None}]}, {"name": "No customers", "inputs": {"customers": {"schema": "customer_id int", "rows": []}, "orders": {"schema": "order_id string, customer_id int", "rows": [["a", 1], ["a", 1]]}}, "expected": [{"order_id": "a", "customer_id": 1}, {"order_id": "a", "customer_id": 1}]}, {"name": "No orders", "inputs": {"customers": {"schema": "customer_id int", "rows": [[1]]}, "orders": {"schema": "order_id string, customer_id int", "rows": []}}, "expected": []}]
    EXPECTED_COLUMNS = ["order_id", "customer_id"]

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
