from math import trunc
from typing import List

import joblib
import pandas as pd
from annoy import AnnoyIndex
import numpy as np

from gainy.compute.collection_utils import batch_iter
from gainy.compute.industries.model import IndustryAssignmentModel


class BertModel(IndustryAssignmentModel):

    dims = 768
    BATCH_SIZE = 100

    def __init__(self):
        self.centroid_index = None
        self.label_to_index = None
        self.index_to_label = None

        # TMP pre-calculated features
        ticker_features_df = pd.read_csv("/Users/vasilii/dev/features/distil-bert-tickers.csv")
        ticker_df = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/tickers.csv")[["symbol", "description"]]
        features_with_desc_df = ticker_df.merge(ticker_features_df, how="inner", on=["symbol"])
        features_with_desc_df = features_with_desc_df[features_with_desc_df.columns[1:]]

        self.desc_to_features_index = {}
        for row in features_with_desc_df.to_numpy():
            self.desc_to_features_index[row[0]] = row[1:]

    # def save(self):
    #     # joblib.dump(self)
    #     pass
    #
    # def load(self, ):
    #     pass

    def fit(self, X: pd.DataFrame, y: pd.DataFrame):
        features_list = []
        for descriptions_batch in batch_iter(X[X.columns[0]].to_numpy(), self.BATCH_SIZE):
            features_list += self._features(descriptions_batch)

        features = pd.DataFrame(data=features_list, index=X.index)
        self._fit(features, y)

    def _fit(self, features: pd.DataFrame, y: pd.DataFrame):
        self.centroid_index = AnnoyIndex(self.dims, metric="angular")
        self.label_to_index = {}
        self.index_to_label = {}

        features_with_classes = pd.concat([features, y], axis=1)

        centroids = features_with_classes.groupby(y.columns[0]).aggregate("mean")
        centroids.reset_index(inplace=True)

        for index, row in enumerate(centroids.to_numpy()):
            self.centroid_index.add_item(index, row[1:])
            self.label_to_index[row[0]] = index
            self.index_to_label[index] = row[0]

        self.centroid_index.build(50)

    def _features(self, description_list):
        # TODO: implement me
        return [self.desc_to_features_index[desc] for desc in description_list]

    def classify(self, descriptions, n: int = 2, include_distances: bool = False):
        features_list = []
        for descriptions_batch in batch_iter(descriptions[descriptions.columns[0]].to_numpy(), self.BATCH_SIZE):
            features_list += self._features(descriptions_batch)

        return self._classify(features_list, n, include_distances)

    def _classify(self, features_list, n: int = 1, include_distances: bool = False):
        if not self.centroid_index or not self.index_to_label or not self.label_to_index:
            raise Exception("Fit model first")

        labels, distances = [], []
        for features in features_list:
            nns_output = self.centroid_index.get_nns_by_vector(features, n=n, include_distances=include_distances)
            nn_labels = [self.index_to_label.get(i) for i in (nns_output[0] if include_distances else nns_output)]

            labels.append(nn_labels)
            distances.append([1.0 - d for d in nns_output[1]])

        if include_distances:
            return labels, distances
        else:
            return labels

