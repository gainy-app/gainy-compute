from sklearn.metrics import average_precision_score
import logging
import pandas as pd

from sklearn.model_selection import StratifiedKFold

from gainy.utils import batch_iter
from gainy.industries.model import IndustryAssignmentModel


def test_model(model: IndustryAssignmentModel, X_test, y_test) -> float:
    label_col = y_test.columns[0]
    label_list = sorted(set(y_test[label_col]))

    sum_ap = 0.0
    batch_size = 100
    offset = 0
    # TODO: is this batch_iter still needed?
    for batch in batch_iter(X_test.to_numpy(), batch_size):
        labels, distances = model.predict(pd.DataFrame(data=batch), 2, include_distances=True)

        for index, labels_with_distances in enumerate(zip(labels, distances)):
            expected_labels = [l == y_test.iloc[offset + index][label_col] for l in label_list]

            labels_distances = dict(zip(labels_with_distances[0], labels_with_distances[1]))
            actual_labels = [labels_distances.get(l, 0.0) for l in label_list]

            sum_ap += average_precision_score(expected_labels, actual_labels)

        offset += len(batch)

    return sum_ap / len(X_test)


def cross_validation(model: IndustryAssignmentModel, X, y, n_splits: int = 3):
    skf = StratifiedKFold(n_splits=n_splits)
    splits = skf.split(X, y)

    scores = []
    for index, split in enumerate(splits):
        logging.info("Processing split: %s" % index)

        X_train = X.iloc[split[0]]
        y_train = y.iloc[split[0]]

        model.fit(X_train, y_train)

        X_test = X.iloc[split[1]]
        y_test = y.iloc[split[1]]
        model_score = test_model(model, X_test, y_test)

        scores.append(model_score)

    return scores
