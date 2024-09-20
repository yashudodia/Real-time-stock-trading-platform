import streamlit as st
import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import FloatType
from pyspark.ml.evaluation import RegressionEvaluator

def predict_stock_prices(ticker):
    # API key setup and data fetch
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize=full&apikey=73D0V4NT052FRZJN'
    r = requests.get(url)
    data = r.json()

    # Prepare data from JSON
    time_series_data = data['Time Series (Daily)']
    rows = [(date, float(details['1. open']), float(details['2. high']), float(details['3. low']), float(details['4. close']), float(details['5. volume']))
            for date, details in time_series_data.items()]
    df = spark.createDataFrame(rows, schema=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])

    # Define windows for moving averages and other calculations
    windowSpec5 = Window.orderBy("Date").rowsBetween(-4, 0)
    windowSpec20 = Window.orderBy("Date").rowsBetween(-19, 0)
    volatility_window = Window.orderBy("Date").rowsBetween(-9, 0)

    # Calculate additional features
    df = df.withColumn("5_day_MA", F.avg("Close").over(windowSpec5))
    df = df.withColumn("20_day_MA", F.avg("Close").over(windowSpec20))
    df = df.withColumn("10_day_Volatility", F.stddev("Close").over(volatility_window))
    df = df.withColumn("Volume_Change", F.col("Volume") - F.lag("Volume", 1).over(Window.orderBy("Date")))

    # Filter data for last 5 years
    five_years_ago = datetime.now() - timedelta(days=5*365)
    df = df.filter(F.col("Date") >= five_years_ago.strftime('%Y-%m-%d'))

    # Data splitting for model training
    training_df = df.filter(F.col("Date") < '2023-04-11')  # Adjust the date as needed
    testing_df = df.filter(F.col("Date") >= '2023-04-11')  # Adjust the date as needed

    # Feature vector assembly
    featureAssembler = VectorAssembler(
        inputCols=[ 'Volume', 'Volume_Change', '10_day_Volatility', '5_day_MA', '20_day_MA'],
        outputCol='features'
    )

    # Prepare training and testing data
    training_data = featureAssembler.transform(training_df).select('Date', 'features', 'Close').withColumnRenamed('Close', 'label')
    testing_data = featureAssembler.transform(testing_df).select('Date', 'features', 'Close').withColumnRenamed('Close', 'label')

    # Linear Regression model
    lr = LinearRegression(featuresCol='features', labelCol='label')
    lr_model = lr.fit(training_data)

    # Making predictions
    predictions = lr_model.transform(testing_data)

    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

    # Display predictions with actual prices
    predictions.select("Date", "label", "prediction").show(10)
    # return predictions
    return predictions, training_df

# Initialize SparkSession (ensure it is initialized outside the function if used in a script or app context)
spark = SparkSession.builder.appName("FinancialDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

predictions =predict_stock_prices('TSLA')

# predictions.show(10)
