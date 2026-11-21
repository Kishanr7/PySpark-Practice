# SPKD012 · Flag changed records
# Completed: 2026-11-21
# Task: Flag new and changed records.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD012: Flag changed records
# GOAL: Flag new and changed records.
# RETURN COLUMNS: id, value, change_type
# - id not in current → 'INSERT'.
# - id exists but value differs → 'UPDATE'. NULL→value and value→NULL are changes; NULL = NULL is not.
# - Leave out unchanged rows. Ignore rows that were deleted.
# Assume: id is unique and never NULL.
# Inspect the supplied DataFrames first, then implement solve. You may use show(), printSchema(), select(), filter() and explain() while exploring. When ready, turn RUN_CHECKS on and return exactly the expected columns. Row order does not matter; duplicates do.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD012").getOrCreate()

    # Start here. Change to True only when solve() is ready for the checks.
    RUN_CHECKS = True

    def solve(incoming, current):
        # Write your final transformation here. Return only the requested columns.
        inc = incoming.alias("inc")
        cur = current.alias("cur")
    
        joined_df = inc.join(
            cur,
            F.col("inc.id") == F.col("cur.id"),
            how="left"
        )
    
        result = (
            joined_df
            .withColumn(
                "change_type",
                F.when(
                    F.col("cur.id").isNull(),
                    F.lit("INSERT")
                )
                .when(
                    ~F.col("inc.value").eqNullSafe(F.col("cur.value")),
                    F.lit("UPDATE")
                )
            )
            .filter(F.col("change_type").isNotNull())
            .select(
                F.col("inc.id").alias("id"),
                F.col("inc.value").alias("value"),
                F.col("change_type")
            )
        )
    
        return result
    # All test data is visible. You can edit or add synthetic rows while learning.
    CASES = [{"name": "Insert, update, unchanged and NULL", "inputs": {"incoming": {"schema": "id int, value string", "rows": [[1, "a"], [2, "new"], [3, None], [4, "x"]]}, "current": {"schema": "id int, value string", "rows": [[1, "a"], [2, "old"], [3, None], [5, "gone"]]}}, "expected": [{"id": 2, "value": "new", "change_type": "UPDATE"}, {"id": 4, "value": "x", "change_type": "INSERT"}]}, {"name": "NULL transitions", "inputs": {"incoming": {"schema": "id int, value string", "rows": [[1, None], [2, "a"]]}, "current": {"schema": "id int, value string", "rows": [[1, "a"], [2, None]]}}, "expected": [{"id": 1, "value": None, "change_type": "UPDATE"}, {"id": 2, "value": "a", "change_type": "UPDATE"}]}, {"name": "Empty incoming", "inputs": {"incoming": {"schema": "id int, value string", "rows": []}, "current": {"schema": "id int, value string", "rows": [[1, "a"]]}}, "expected": []}]
    EXPECTED_COLUMNS = ["id", "value", "change_type"]

    # EXPLORATION AREA: the first example is built before any assertion runs.
    preview = {name: spark.createDataFrame(spec["rows"], spec["schema"])
               for name, spec in CASES[0]["inputs"].items()}
    for name, df in preview.items():
        print("\nINPUT DATAFRAME:", name)
        df.printSchema()
        df.show(truncate=False)

    # Try normal PySpark commands here. Examples:
    # preview["incoming"].select("*").show(truncate=False)
    # preview["incoming"].filter(F.lit(True)).explain()
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
