# SPKD002 · Total sales by store
# Completed: 2026-11-11
# Task: Return one row per store_id with total_amount. Ignore NULL amounts when summing; an all-NULL group must have total_amount 0. Keep zero and negative amounts.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD002: Total sales by store
# Return one row per store_id with total_amount. Ignore NULL amounts when summing; an all-NULL group must have total_amount 0. Keep zero and negative amounts.
# Implement solve using the supplied DataFrames. Return a DataFrame with exactly the expected columns. Row order does not matter; duplicates do. Use built-in PySpark functions. The starter includes runnable, visible assertions.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD002").getOrCreate()

    def solve(sales):
        # Write your transformation here. Return only the requested columns.
        return sales.groupBy(F.col("store_id")).agg(F.coalesce(F.sum("amount"), F.lit(0)).alias("total_amount"))

    # Visible checks: normal input, boundary/duplicate cases and empty input.
    CASES = [{"name": "Totals and an all-NULL store", "inputs": {"sales": {"schema": "store_id string, amount int", "rows": [["a", 10], ["a", -2], ["b", None], ["c", 0]]}}, "expected": [{"store_id": "a", "total_amount": 8}, {"store_id": "b", "total_amount": 0}, {"store_id": "c", "total_amount": 0}]}, {"name": "No sales", "inputs": {"sales": {"schema": "store_id string, amount int", "rows": []}}, "expected": []}, {"name": "Repeated rows count twice", "inputs": {"sales": {"schema": "store_id string, amount int", "rows": [["a", 5], ["a", 5]]}}, "expected": [{"store_id": "a", "total_amount": 10}]}]
    EXPECTED_COLUMNS = ["store_id", "total_amount"]

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
