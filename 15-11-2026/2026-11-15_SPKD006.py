# SPKD006 · Latest event per account
# Completed: 2026-11-15
# Task: Return account_id, event_id and event_time for the latest event per account. event_time is an ISO timestamp string in UTC; choose the greatest event_id alphabetically when times tie. Keys and times are non-NULL.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD006: Latest event per account
# Return account_id, event_id and event_time for the latest event per account. event_time is an ISO timestamp string in UTC; choose the greatest event_id alphabetically when times tie. Keys and times are non-NULL.
# Inspect the supplied DataFrames first, then implement solve. You may use show(), printSchema(), select(), filter() and explain() while exploring. When ready, turn RUN_CHECKS on and return exactly the expected columns. Row order does not matter; duplicates do.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD006").getOrCreate()

    # Start here. Change to True only when solve() is ready for the checks.
    RUN_CHECKS = True

    def solve(events):
        # Write your final transformation here. Return only the requested columns.
        w = Window.partitionBy("account_id").orderBy(
            F.col("event_time").desc(),
            F.col("event_id").desc()
        )
    
        final = (
            events
            .withColumn("rnk", F.row_number().over(w))
            .filter(F.col("rnk") == 1)
            .select("account_id", "event_id", "event_time")
        )
    
        return final

    # All test data is visible. You can edit or add synthetic rows while learning.
    CASES = [{"name": "Latest row and a tie", "inputs": {"events": {"schema": "account_id int, event_id string, event_time string", "rows": [[1, "a", "2026-01-01T10:00:00"], [1, "b", "2026-01-02T10:00:00"], [1, "c", "2026-01-02T10:00:00"], [2, "d", "2026-01-01T10:00:00"]]}}, "expected": [{"account_id": 1, "event_id": "c", "event_time": "2026-01-02T10:00:00"}, {"account_id": 2, "event_id": "d", "event_time": "2026-01-01T10:00:00"}]}, {"name": "Exact replay", "inputs": {"events": {"schema": "account_id int, event_id string, event_time string", "rows": [[1, "a", "2026-01-01T10:00:00"], [1, "a", "2026-01-01T10:00:00"]]}}, "expected": [{"account_id": 1, "event_id": "a", "event_time": "2026-01-01T10:00:00"}]}, {"name": "Empty input", "inputs": {"events": {"schema": "account_id int, event_id string, event_time string", "rows": []}}, "expected": []}]
    EXPECTED_COLUMNS = ["account_id", "event_id", "event_time"]

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
