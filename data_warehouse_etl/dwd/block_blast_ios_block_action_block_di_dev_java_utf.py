from pyspark.sql import SparkSession



if __name__ == "__main__":
    # 创建 Spark 会话
    # 创建 SparkSession
    print('''开始执行''')
    spark = (SparkSession.builder
             .appName("dws_block_blast_ios_block_action_di_udf")
             .enableHiveSupport().getOrCreate())
    print('''注册java函数''')
    spark.udf.registerJavaFunction("get_area_complex_value", "com.hungrystudio.utf05.ToUpperCaseUDF")
    test1 = '''[[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10]]'''
    test2 = '''[[10,10,10,10,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10],[10,10,10,-1,-1,-1,10,10]]'''

    print('''创建示例 DataFrame''')
    # 创建示例 DataFrame
    data = [(test1,), (test2,)]
    columns = ["input"]
    df = spark.createDataFrame(data, columns)

    # 使用 Java UDF
    print('''使用 Java UDF''')
    # 使用 SQL 表达式调用 UDF
    df = df.selectExpr("input", "get_area_complex_value(input) as output")

    # 显示结果
    print('''显示结果''')
    df.show()

    # 停止 SparkSession
    print('''停止 SparkSession''')
    spark.stop()

