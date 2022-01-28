from gainy_compute.recommendations.core import DimVector
from gainy_compute.recommendations.match_score import profile_ticker_similarity, SimilarityLevel


def test_ticker_match_score():
    profile_interests_v = [
        DimVector(10, {"1": 1}),
        DimVector(20, {"1": 1, "2": 1, "3": 1})
    ]
    profile_categories_v = DimVector(1, {"1": 1, "2": 1})

    ticker_industries_v = DimVector("TICKER", {"1": 1})
    ticker_categories_v = DimVector("TICKER", {"2": 1})

    risk_mapping = {"1": 1, "2": 3}

    match_score = profile_ticker_similarity(
        profile_categories_v,
        ticker_categories_v,
        risk_mapping,
        profile_interests_v,
        ticker_industries_v
    )

    assert match_score.match_score() == 80

    explanation = match_score.explain()
    assert explanation.category_level == SimilarityLevel.HIGH
    assert explanation.category_matches == [2]
    assert explanation.interest_level == SimilarityLevel.HIGH
    assert explanation.interest_matches == [10, 20]
