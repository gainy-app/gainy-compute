from abc import ABC
import pandas as pd
import psycopg2


class TickerRepository(ABC):

    def load_tickers(self) -> pd.DataFrame:
        pass

    def load_manual_ticker_industries(self) -> pd.DataFrame:
        pass

    def save_auto_ticker_industries(self,
                                    tickers_with_predictions: pd.DataFrame):
        pass


class DatabaseTickerRepository(TickerRepository):

    _public_schema = "public"
    _raw_schema = "raw_data"

    def __init__(self, db_host, db_port, db_name, db_user, db_password):
        self._db_conn_uri = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    def load_tickers(self) -> pd.DataFrame:
        stmt = f"""SELECT code AS symbol, general ->> 'Description' AS description 
        FROM {self._raw_schema}.eod_fundamentals
        """
        tickers = pd.read_sql(stmt, self._db_conn_uri)
        desc_filter = (tickers["description"].notnull()) & (
            tickers["description"].str.len() >= 10)

        return tickers.loc[desc_filter]

    def load_manual_ticker_industries(self) -> pd.DataFrame:
        ticker_industries = pd.read_sql_table("gainy_ticker_custom_industries",
                                              self._db_conn_uri,
                                              schema=self._raw_schema)
        ticker_industries = ticker_industries.rename(columns={
            "industry name": "industry_name",
            "code": "symbol"
        })

        industries = pd.read_sql_table("gainy_custom_industries",
                                       self._db_conn_uri,
                                       schema=self._raw_schema)
        industries = industries.rename(columns={
            "name": "industry_name",
            "id": "industry_id"
        })
        industries["industry_id"] = industries["industry_id"].astype(int)

        return ticker_industries.merge(industries,
                                       how="inner",
                                       on=["industry_name"
                                           ])[["symbol", "industry_id"]]

    def save_auto_ticker_industries(self,
                                    tickers_with_predictions: pd.DataFrame):
        tickers_with_predictions.to_sql("auto_ticker_industries",
                                        self._db_conn_uri,
                                        schema=self._raw_schema,
                                        if_exists="replace",
                                        index=False)
