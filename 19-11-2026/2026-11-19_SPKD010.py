# SPKD010 · Expand product tags
# Completed: 2026-11-19
# Task: Turn each product tag into its own row.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD010: Expand product tags
# GOAL: Turn each product tag into its own row.
# RETURN COLUMNS: product_id, tag
# - One row per item in the tags array.
# - Skip NULL arrays, empty arrays and NULL items. Do not add placeholder rows.
# - Keep repeated tags.
# Inspect the supplied DataFrames first, then implement solve. You may use show(), printSchema(), select(), filter() and explain() while exploring. When ready, turn RUN_CHECKS on and return exactly the expected columns. Row order does not matter; duplicates do.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD010").getOrCreate()

    # Start here. Change to True only when solve() is ready for the checks.
    RUN_CHECKS = True

    def solve(products):
        result = (
            products
            .select(
                F.col("product_id"),
                F.explode(F.col("tags")).alias("tag")
            )
            .filter(F.col("tag").isNotNull())
        )
  
        return result

    # All test data is visible. You can edit or add synthetic rows while learning.
    CASES = [{"name": "Arrays, empty arrays and NULLs", "inputs": {"products": {"schema": "product_id string, tags array<string>", "rows": [["a", ["red", "large"]], ["b", []], ["c", None], ["d", ["blue", None]]]}}, "expected": [{"product_id": "a", "tag": "red"}, {"product_id": "a", "tag": "large"}, {"product_id": "d", "tag": "blue"}]}, {"name": "Repeated tag", "inputs": {"products": {"schema": "product_id string, tags array<string>", "rows": [["a", ["red", "red"]]]}}, "expected": [{"product_id": "a", "tag": "red"}, {"product_id": "a", "tag": "red"}]}, {"name": "Empty input", "inputs": {"products": {"schema": "product_id string, tags array<string>", "rows": []}}, "expected": []}]
    EXPECTED_COLUMNS = ["product_id", "tag"]

    # EXPLORATION AREA: the first example is built before any assertion runs.
    preview = {name: spark.createDataFrame(spec["rows"], spec["schema"])
               for name, spec in CASES[0]["inputs"].items()}
    for name, df in preview.items():
        print("\nINPUT DATAFRAME:", name)
        df.printSchema()
        df.show(truncate=False)

    # Try normal PySpark commands here. Examples:
    # preview["products"].select("*").show(truncate=False)
    # preview["products"].filter(F.lit(True)).explain()
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
