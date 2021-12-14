import os

os.environ["MLFLOW_TRACKING_URI"] = "postgresql://postgres:postgrespassword@localhost:5432/postgres?options=-csearch_path=mlflow"
os.environ["MLFLOW_ARTIFACT_LOCATION"] = "s3://gainy-mlflow-dev"


import mlflow
import pandas as pd

import time
from numpy import mean

from data_access.repository import DatabaseTickerRepository
from gainy_compute.industries.lifecycle import cross_validation
from gainy_compute.industries.lifecycle import IndustryAssignmentModel
from gainy_compute.industries.tfidf_model import TfIdfIndustryAssignmentModel
from industries.console import IndustryAssignmentRunner


def run_cross_validation(model: IndustryAssignmentModel, n_splits: int = 3):
    industry_tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/industry_tickers.csv")
    industry_tickers = industry_tickers.rename(columns={"Industry Name": "industry_name", "Code": "symbol"})

    tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/tickers_full.csv")[["symbol", "description"]]

    # TMP - only consider tickets with features
    # tickers_with_features_df = pd.read_csv("/Users/vasilii/dev/features/distil-bert-tickers.csv")
    # tickers = tickers.merge(tickers_with_features_df[["symbol"]], how="inner", on=["symbol"])

    tickers_with_industries = tickers.merge(industry_tickers, how="inner", on=["symbol"])
    tickers_with_industries["symbol"] = tickers_with_industries["symbol"].astype(str)
    tickers_with_industries["description"] = tickers_with_industries["description"].astype(str)

    industry_counts = tickers_with_industries[["ind_name", "symbol"]].groupby("ind_name").count()
    industry_counts.reset_index(inplace=True)

    selected_industry_counts = industry_counts[industry_counts["symbol"] >= n_splits]
    selected_industries = selected_industry_counts[["industry_name"]]

    tickers_with_industries = tickers_with_industries.merge(selected_industries, on=["industry_name"], how="inner")

    X = tickers_with_industries[["description"]]
    y = tickers_with_industries[["industry_name"]]

    return cross_validation(model, X, y, n_splits=n_splits)


def run_train_from_db():
    repo = DatabaseTickerRepository(
        db_host="localhost",
        db_port=5432,
        db_user="postgres",
        db_password="postgrespassword",
        db_name="postgres"
    )
    runner = IndustryAssignmentRunner(repo)

    runner.run_predict()



start_time = time.time()

run_train_from_db()
# results = run_cross_validation(TfIdfIndustryAssignmentModel())
# print(results)
# print("AVG: %s" % mean(results))
print("TIME: %s" % (time.time() - start_time))
