# SPKD007 · Top two products in each category
# Completed: 2026-11-16
# Task: Return category, product_id and revenue for at most two products per category. Sort by revenue descending, then product_id ascending to break ties. Input has one row per product, with no NULLs.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice

# SPKD007: Top two products in each category
# Return category, product_id and revenue for at most two products per category. Sort by revenue descending, then product_id ascending to break ties. Input has one row per product, with no NULLs.
# Inspect the supplied DataFrames first, then implement solve. You may use show(), printSchema(), select(), filter() and explain() while exploring. When ready, turn RUN_CHECKS on and return exactly the expected columns. Row order does not matter; duplicates do.
# Keep main() and its final call. Edit only solve inside it.

def main():
    from pyspark.sql import SparkSession, functions as F, Window
    import json

    spark = SparkSession.builder.appName("SPKD007").getOrCreate()

    # Start here. Change to True only when solve() is ready for the checks.
    RUN_CHECKS = True

    def solve(products):
        # Write your final transformation here. Return only the requested columns.
        w = Window.partitionBy("category").orderBy(
            F.col("revenue").desc(),
            F.col("product_id").asc()
        )
    
        final = (
            products
            .withColumn("rnk", F.row_number().over(w))
            .filter(F.col("rnk") < 3)
            .select("category", "product_id", "revenue")
        )
        return final

    # All test data is visible. You can edit or add synthetic rows while learning.
    CASES = [{"name": "Tie at the boundary", "inputs": {"products": {"schema": "category string, product_id string, revenue int", "rows": [["a", "p3", 50], ["a", "p2", 100], ["a", "p1", 100], ["b", "p4", 10]]}}, "expected": [{"category": "a", "product_id": "p1", "revenue": 100}, {"category": "a", "product_id": "p2", "revenue": 100}, {"category": "b", "product_id": "p4", "revenue": 10}]}, {"name": "Negative revenues", "inputs": {"products": {"schema": "category string, product_id string, revenue int", "rows": [["a", "p1", -2], ["a", "p2", 0], ["a", "p3", -1]]}}, "expected": [{"category": "a", "product_id": "p2", "revenue": 0}, {"category": "a", "product_id": "p3", "revenue": -1}]}, {"name": "Empty input", "inputs": {"products": {"schema": "category string, product_id string, revenue int", "rows": []}}, "expected": []}]
    EXPECTED_COLUMNS = ["category", "product_id", "revenue"]

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
