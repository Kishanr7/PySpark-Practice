# SPKD008 · Running balance per account
# Completed: 2026-11-17
# Task: Add a running balance for each account.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD008: Running balance per account
# GOAL: Add a running balance for each account.
# RETURN COLUMNS: account_id, event_id, amount, running_total
# - Running sum per account, in ascending event_id order.
# - Use a row-based window (rowsBetween).
# Assume: event_id is unique per account. Amounts are non-NULL integers.
# Inspect the supplied DataFrames first, then implement solve. You may use show(), printSchema(), select(), filter() and explain() while exploring. When ready, turn RUN_CHECKS on and return exactly the expected columns. Row order does not matter; duplicates do.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD008").getOrCreate()

    # Start here. Change to True only when solve() is ready for the checks.
    RUN_CHECKS = True

    def solve(events):
        # Write your final transformation here. Return only the requested columns.
        w = Window.partitionBy("account_id").orderBy(
            F.col("event_id").asc()
        )
    
        final = (
            events
            .withColumn("running_total", F.sum(F.col("amount")).over(w))
            .select("account_id", "event_id", "amount","running_total")
        )
        return final

    # All test data is visible. You can edit or add synthetic rows while learning.
    CASES = [{"name": "Out-of-order input and separate accounts", "inputs": {"events": {"schema": "account_id int, event_id int, amount int", "rows": [[1, 2, -3], [1, 1, 10], [2, 1, 5], [1, 3, 0]]}}, "expected": [{"account_id": 1, "event_id": 1, "amount": 10, "running_total": 10}, {"account_id": 1, "event_id": 2, "amount": -3, "running_total": 7}, {"account_id": 1, "event_id": 3, "amount": 0, "running_total": 7}, {"account_id": 2, "event_id": 1, "amount": 5, "running_total": 5}]}, {"name": "Single negative event", "inputs": {"events": {"schema": "account_id int, event_id int, amount int", "rows": [[1, 1, -2]]}}, "expected": [{"account_id": 1, "event_id": 1, "amount": -2, "running_total": -2}]}, {"name": "Empty input", "inputs": {"events": {"schema": "account_id int, event_id int, amount int", "rows": []}}, "expected": []}]
    EXPECTED_COLUMNS = ["account_id", "event_id", "amount", "running_total"]

    # EXPLORATION AREA: the first example is built before any assertion runs.
    preview = {name: spark.createDataFrame(spec["rows"], spec["schema"])
               for name, spec in CASES[0]["inputs"].items()}
    for name, df in preview.items():
        print("\nINPUT DATAFRAME:", name)
        df.printSchema()
        df.show(truncate=False)

    # Try normal PySpark commands here. Examples:
    # preview["events"].select("*").show(truncate=False)
    # preview["events"].filter(F.lit(True)).explain()
    # trial = solve(**preview)
    # if trial is not None:
    #     trial.show(truncate=False)

    if not RUN_CHECKS:
        print("\nEXPLORATION MODE: inspect the output above, add commands, and rerun.")
        print("When solve() is ready, set RUN_CHECKS = True to run all 3 visible checks.")
        return

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
