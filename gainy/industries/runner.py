import os
import traceback
import sys
from numpy import mean
from gainy.industries.repository import TickerRepository, DatabaseTickerRepository
from gainy.industries.tfidf_model import TfIdfIndustryAssignmentModel
import logging
from gainy.industries.lifecycle import cross_validation, test_model
import mlflow
import pandas as pd
from mlflow.tracking import MlflowClient
from gainy.utils import env
import psutil

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IndustryAssignmentRunner:
    MIN_X_SCORE = 0.7

    _artifact_location = os.environ["MLFLOW_ARTIFACT_LOCATION"]
    _registered_name = "Industry Assignment"

    def __init__(self, repo: TickerRepository):
        self.repo = repo
        self.model = TfIdfIndustryAssignmentModel()

    @property
    def _model_version_stage(self):
        return "Production" if env().lower().startswith("prod") else "Staging"

    def run_train(self):
        tickers = self.repo.load_tickers()
        manual_industries = self.repo.load_manual_ticker_industries()

        tickers_with_industries = tickers.merge(manual_industries,
                                                how="inner",
                                                on=["symbol"])

        self._set_mlflow_experiment()
        with mlflow.start_run() as run:
            x_score = mean(self._cross_validation(tickers_with_industries))
            logger.info(
                f"Model cross validation score (average MAP) is {x_score}")
            mlflow.log_metric("Cross-validation MAP", x_score)

            X = tickers_with_industries[["description"]]
            y = tickers_with_industries[["industry_id"]]
            self.model.fit(X, y)

            train_score = test_model(self.model, X, y)
            logger.info(
                f"Model train score (MAP on training data) is {train_score}")
            mlflow.log_metric("Train MAP", train_score)

            mlflow.pyfunc.log_model(artifact_path=self.model.name(),
                                    python_model=self.model)

            if x_score < self.MIN_X_SCORE:
                logger.error(
                    f"The cross-validation score is too low, expected={self.MIN_X_SCORE}, actual={x_score}"
                )
                raise Exception(
                    "The cross-validation score is too low, review the data and run it again."
                )
            else:
                self._register_model(run)

    def _cross_validation(self, tickers_with_industries, n_splits: int = 3):
        industry_counts = tickers_with_industries[[
            "industry_id", "symbol"
        ]].groupby("industry_id").count()
        industry_counts.reset_index(inplace=True)

        selected_industry_counts = industry_counts[
            industry_counts["symbol"] >= n_splits]
        selected_industries = selected_industry_counts[["industry_id"]]

        tickers_with_industries = tickers_with_industries.merge(
            selected_industries, on=["industry_id"], how="inner")

        X = tickers_with_industries[["description"]]
        y = tickers_with_industries[["industry_id"]]

        return cross_validation(self.model, X, y, n_splits)

    def _register_model(self, run):
        logger.info(
            f"Register model `{self._registered_name}` with run_id `{run.info.run_id}`"
        )
        model_version = mlflow.register_model(f"runs:/{run.info.run_id}",
                                              self._registered_name)

        client = MlflowClient()
        client.update_model_version(
            name=model_version.name,
            version=model_version.version,
            description=f"[{env().upper()}]: {self.model.description()}")

        client.transition_model_version_stage(name=model_version.name,
                                              version=model_version.version,
                                              stage=self._model_version_stage)

    def _set_mlflow_experiment(self):
        experiment = mlflow.get_experiment_by_name(self._registered_name)
        if not experiment:
            experiment_id = mlflow.create_experiment(
                name=self._registered_name,
                artifact_location=self._artifact_location)
        else:
            experiment_id = experiment.experiment_id
        mlflow.set_experiment(experiment_id=experiment_id)

    def run_predict(self):
        self._load_model()

        tickers = self.repo.load_tickers()[["symbol", "description"]]
        tickers = tickers[tickers["description"] ==
                          tickers["description"]]  # Remove Nones, NaNs, etc
        tickers.reset_index(inplace=True, drop=True)

        batch_size = 1000
        ticker_descriptions = tickers[["description"]]
        predictions_list = []
        for start in range(0, len(tickers), batch_size):
            predictions_list += self.model.predict(
                ticker_descriptions.iloc[start:start + batch_size],
                n=2,
                include_distances=False)

        predictions = pd.DataFrame(data=predictions_list,
                                   columns=["industry_id_1", "industry_id_2"])

        manual_ticker_industries = self.repo.load_manual_ticker_industries()
        tickers_with_industries = tickers.merge(manual_ticker_industries,
                                                how="left",
                                                on=["symbol"])

        tickers_with_predictions = pd.concat(
            [tickers_with_industries, predictions],
            axis=1)[["symbol", "industry_id_1", "industry_id_2"]]

        self.repo.save_auto_ticker_industries(tickers_with_predictions)

    def _load_model(self):
        client = MlflowClient()
        latest_version = client.get_latest_versions(
            self._registered_name, [self._model_version_stage])[0]

        artifact_uri = client.get_model_version_download_uri(
            latest_version.name, latest_version.version)

        model_uri = f"{artifact_uri}/{self.model.name()}"
        loaded_model = mlflow.pyfunc.load_model(model_uri)

        # TODO: A hack to get the original model. Need to handle it in more MLflow'ish way.
        self.model = loaded_model._model_impl.python_model


def cli(args=None):
    try:
        if not args:
            args = sys.argv[1:]

        repo = DatabaseTickerRepository(db_host=os.environ["PG_ADDRESS"],
                                        db_port=os.environ["PG_PORT"],
                                        db_user=os.environ["PG_USERNAME"],
                                        db_password=os.environ["PG_PASSWORD"],
                                        db_name=os.environ["PG_DATABASE"])
        runner = IndustryAssignmentRunner(repo)

        command = args[0]
        if "train" == command:
            runner.run_train()
        elif "predict" == command:
            runner.run_predict()
    except:
        traceback.print_exc()
