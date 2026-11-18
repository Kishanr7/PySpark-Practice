# SPKD009 · Success and failure totals
# Completed: 2026-11-18
# Task: Show success and failure totals for each store.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD009: Success and failure totals
# GOAL: Show success and failure totals for each store.
# RETURN COLUMNS: store_id, success_total, failed_count
# - success_total = sum of amount on SUCCESS rows, with a NULL amount counting as 0.
# - failed_count = number of FAILED rows.
# - Other statuses add to neither; the store still appears.
# Inspect the supplied DataFrames first, then implement solve. You may use show(), printSchema(), select(), filter() and explain() while exploring. When ready, turn RUN_CHECKS on and return exactly the expected columns. Row order does not matter; duplicates do.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD009").getOrCreate()

    # Start here. Change to True only when solve() is ready for the checks.
    RUN_CHECKS = True

    def solve(payments):
      s = (
          payments
          .groupBy("store_id")
          .agg(
              F.sum(
                  F.when(
                      F.col("status") == "SUCCESS",
                      F.coalesce(F.col("amount"), F.lit(0))
                  ).otherwise(0)
              ).alias("success_total"),
  
              F.sum(
                  F.when(
                      F.col("status") == "FAILED", 1
                  ).otherwise(0)
              ).alias("failed_count")
          )
      )
  
      return s

    # All test data is visible. You can edit or add synthetic rows while learning.
    CASES = [{"name": "Mixed statuses", "inputs": {"payments": {"schema": "store_id string, status string, amount int", "rows": [["a", "SUCCESS", 10], ["a", "SUCCESS", None], ["a", "FAILED", 20], ["b", None, 4]]}}, "expected": [{"store_id": "a", "success_total": 10, "failed_count": 1}, {"store_id": "b", "success_total": 0, "failed_count": 0}]}, {"name": "Repeated failures", "inputs": {"payments": {"schema": "store_id string, status string, amount int", "rows": [["a", "FAILED", 1], ["a", "FAILED", 1]]}}, "expected": [{"store_id": "a", "success_total": 0, "failed_count": 2}]}, {"name": "Empty input", "inputs": {"payments": {"schema": "store_id string, status string, amount int", "rows": []}}, "expected": []}]
    EXPECTED_COLUMNS = ["store_id", "success_total", "failed_count"]

    # EXPLORATION AREA: the first example is built before any assertion runs.
    preview = {name: spark.createDataFrame(spec["rows"], spec["schema"])
               for name, spec in CASES[0]["inputs"].items()}
    for name, df in preview.items():
        print("\nINPUT DATAFRAME:", name)
        df.printSchema()
        df.show(truncate=False)

    # Try normal PySpark commands here. Examples:
    # preview["payments"].select("*").show(truncate=False)
    # preview["payments"].filter(F.lit(True)).explain()
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
