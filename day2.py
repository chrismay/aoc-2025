from textwrap import wrap
def main():
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as sf
    from pyspark.sql import Window

    spark = SparkSession.builder.master("local[*]").appName("aoc-example").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # ty:ignore[possibly-missing-attribute]

    df = spark.createDataFrame(
        [(line,) for line in puzzle_input.split(",")], ["line"]
    ).select(
        sf.col("line"),
        sf.split(sf.col("line"), "-").getItem(0).cast("long").alias("start"),
        sf.split(sf.col("line"), "-").getItem(1).cast("long").alias("end"),
    )
    df = df.withColumn("range", sf.expr("sequence(start, end)")).select("*", sf.explode("range").alias("num"))
    df = df.withColumn("length", sf.length(sf.col("num").cast("string")))
   # df = df.where(sf.col("length") % 2 == 0)
    df = df.withColumn("LHS", sf.substring(sf.col("num").cast("string"), 1, sf.col("length") / 2))
    df = df.withColumn("RHS", sf.substring(sf.col("num").cast("string"), sf.col("length") / 2 + 1, sf.col("length") / 2))
    df = df.withColumn("has_repetition", sf.udf(has_repetition, "boolean")(sf.col("num")))
    df = df.drop("range")
    df.show()
#    df = df.where(sf.col("has_repetition") )
#    df = df.where(sf.col("LHS") == sf.col("RHS")).select(sf.sum(sf.col("num")).alias("total"))
    df = df.where(sf.col("has_repetition") ).select(sf.sum(sf.col("num")).alias("total"))
    df.show()

def has_repetition(num:int):
    # return true if the string representation of num is made exclusively from two or more 
    # repeating sequences
    # e.g.  11, 12341234, 123123123  -> True
    #     1234, 1231234, 121  -> False
    max_substring_len = len(str(num)) // 2
    for sub_len in range(1, max_substring_len + 1):
        substrings = wrap(str(num), sub_len)
        if len(substrings) * sub_len == len(str(num)):
            if all(s == substrings[0] for s in substrings):
                return True
    return False

mini_input = "110-112"
example_input = """1-22,95-115,998-1012,1188511880-1188511890,222220-222224,1698522-1698528,446443-446449,38593856-38593862,565653-565659,824824821-824824827,2121212118-2121212124"""

puzzle_input = """96952600-96977512,6599102-6745632,32748217-32835067,561562-594935,3434310838-3434398545,150-257,864469-909426,677627997-677711085,85-120,2-19,3081-5416,34-77,35837999-36004545,598895-706186,491462157-491543875,5568703-5723454,6262530705-6262670240,8849400-8930122,385535-477512,730193-852501,577-1317,69628781-69809331,2271285646-2271342060,282-487,1716-2824,967913879-967997665,22-33,5722-11418,162057-325173,6666660033-6666677850,67640049-67720478,355185-381658,101543-146174,24562-55394,59942-93946,967864-1031782"""

if __name__ == "__main__":
    try:
        #print(has_repetition(111))
        main()
    except ImportError as e:
        print("PySpark is not installed or not available in this environment:", e)
        print(
            "Install pyspark (e.g., pip install pyspark) or configure your environment to run Spark."
        )
