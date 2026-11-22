# SPKD013 · Find missing sequence numbers
# Completed: 2026-11-22
# Task: Find the missing sequence numbers for each account.
# Validation: External checks reported
# Repository: https://github.com/Kishanr7/PySpark-Practice


# SPKD013: Find missing sequence numbers
# GOAL: Find missing event_ids between each account's min and max.
# RETURN COLUMNS: account_id, missing_id

from pyspark.sql import SparkSession, functions as F, Window
import json


spark = SparkSession.builder.appName("SPKD013").getOrCreate()


def solve(events):
    from pyspark.sql import SparkSession, functions as F, Window
    import json
    # Step 1: Remove duplicate records
    events = events.dropDuplicates()

    # Step 2: Find min and max event_id for each account
    min_max_df = (
        events.groupBy("account_id")
        .agg(
            F.min("event_id").alias("min_value"),
            F.max("event_id").alias("max_value")
        )
    )

    # Step 3: Generate all sequence numbers between min and max
    sequence = (
        min_max_df.select(
            "account_id",
            F.explode(
                F.sequence(F.col("min_value"), F.col("max_value"))
            ).alias("event_id")
        )
    )

    # Step 4: Left join with actual events
    joined_df = (
        sequence.alias("seq")
        .join(
            events.alias("eve"),
            (F.col("seq.account_id") == F.col("eve.account_id")) &
            (F.col("seq.event_id") == F.col("eve.event_id")),
            how="left"
        )
    )

    # Step 5: Return missing sequence numbers
    return (
        joined_df
        .filter(F.col("eve.event_id").isNull())
        .select(
            F.col("seq.account_id").alias("account_id"),
            F.col("seq.event_id").alias("missing_id")
        )
    )


# ---------------- VALIDATION ----------------

CASES = [
    {
        "name": "Gaps and a complete account",
        "inputs": {
            "events": {
                "schema": "account_id int, event_id int",
                "rows": [[1, 1], [1, 4], [1, 4], [2, 2], [2, 3]]
            }
        },
        "expected": [
            {"account_id": 1, "missing_id": 2},
            {"account_id": 1, "missing_id": 3}
        ]
    },
    {
        "name": "Single row",
        "inputs": {
            "events": {
                "schema": "account_id int, event_id int",
                "rows": [[1, 5]]
            }
        },
        "expected": []
    },
    {
        "name": "Empty input",
        "inputs": {
            "events": {
                "schema": "account_id int, event_id int",
                "rows": []
            }
        },
        "expected": []
    }
]

EXPECTED_COLUMNS = ["account_id", "missing_id"]


def normalized(rows):
    import json
    return sorted(json.dumps(row, sort_keys=True) for row in rows)


for case in CASES:
    inputs = {}

    for name, input_spec in case["inputs"].items():
        inputs[name] = spark.createDataFrame(
            input_spec["rows"],
            input_spec["schema"]
        )

    result = solve(**inputs)

    assert result is not None, "solve must return a DataFrame"

    assert result.columns == EXPECTED_COLUMNS, (
        "Wrong columns", result.columns
    )

    actual = [
        row.asDict(recursive=True)
        for row in result.collect()
    ]

    assert normalized(actual) == normalized(case["expected"]), (
        case["name"], actual, case["expected"]
    )

    print("PASS:", case["name"])


print("ALL 3 CHECKS PASSED")
