# Databricks notebook source
import mlflow
import pandas as pd

logged_model = 'runs:/2c83c6c5c59d4acc992816a17e75ef3a/model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.

data = {
    'AT': [14.96],
    'V': [41.76],
    'AP': [1024.07],
    'RH': [73.17]
}

df = pd.DataFrame(data)
print(df)

loaded_model.predict(pd.DataFrame(df))
