import pandas as pd

from gainy.compute.industries.bert_model import BertModel
from gainy.compute.industries.idf_model import TfIdfIndustryAssigmentModel
from gainy.compute.industries.lifecycle import cross_validation
from gainy.compute.industries.model import IndustryAssignmentModel


def run_cross_validation(model: IndustryAssignmentModel, n_splits: int = 3):
    industry_tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/industry_tickers.csv")
    industry_tickers = industry_tickers.rename(columns={"Industry Name": "ind_name", "Code": "symbol"})

    tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/tickers.csv")[["symbol", "description"]]

    # TMP - only consider tickets with features
    tickers_with_features_df = pd.read_csv("/Users/vasilii/dev/features/distil-bert-tickers.csv")
    tickers = tickers.merge(tickers_with_features_df[["symbol"]], how="inner", on=["symbol"])

    tickers_with_industries = tickers.merge(industry_tickers, how="inner", on=["symbol"])
    tickers_with_industries["symbol"] = tickers_with_industries["symbol"].astype(str)
    tickers_with_industries["description"] = tickers_with_industries["description"].astype(str)

    industry_counts = tickers_with_industries[["ind_name", "symbol"]].groupby("ind_name").count()
    industry_counts.reset_index(inplace=True)

    selected_industry_counts = industry_counts[industry_counts["symbol"] >= n_splits]
    selected_industries = selected_industry_counts[["ind_name"]]

    tickers_with_industries = tickers_with_industries.merge(selected_industries, on=["ind_name"], how="inner")

    X = tickers_with_industries[["description"]]
    y = tickers_with_industries[["ind_name"]]

    res = cross_validation(model, X, y, n_splits=n_splits)

    print(res)


run_cross_validation(TfIdfIndustryAssigmentModel())
