import pytest

from gainy.context_container import ContextContainer
from gainy.recommendation.serivce import RecommendationService


def test_get_recommended_collections_personalized():
    profile_id = 1

    with ContextContainer() as context_container:
        with context_container.db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO app.profile_scoring_settings (profile_id, created_at, risk_level, average_market_return, investment_horizon, unexpected_purchases_source, damage_of_failure, stock_market_risk_level, trading_experience, if_market_drops_20_i_will_buy, if_market_drops_40_i_will_buy, risk_score)
                VALUES (%(profile_id)s, '2021-10-20 16:02:34.514475 +00:00', 0.5, 6, 0.5, 'checking_savings', 0.5, 'very_risky', 'never_tried', 0.5, 0.5, 2)
                on conflict do nothing;

                INSERT INTO app.profile_interests (profile_id, interest_id) VALUES (%(profile_id)s, 5) on conflict do nothing;
                INSERT INTO app.profile_categories (profile_id, category_id) VALUES (%(profile_id)s, 2), (%(profile_id)s, 5), (%(profile_id)s, 7) on conflict do nothing;
                
                INSERT INTO collection_ticker_actual_weights (date, profile_id, collection_id, collection_uniq_id, symbol, weight)
                VALUES ('2022-08-22', null, 83, '0_83', 'AAPL', 1)
                on conflict do nothing;
                """, {"profile_id": profile_id})

        limit = 10
        collections = context_container.recommendation_service._get_recommended_collections_personalized(
            profile_id, limit)
        assert [(83, '0_83')] == list(collections)


def test_get_recommended_collections_global():
    profile_id = 1

    with ContextContainer() as context_container:
        limit = 10
        collections = context_container.recommendation_service._get_recommended_collections_global(
            profile_id, limit)
        assert [(83, '0_83'), (275, '0_275'),
                (277, '0_277')] == list(collections)


def get_test_data():
    return [
        ({
            "profile_id": 22183,
            "risk_level": 0.23200755,
            "average_market_return": 6,
            "investment_horizon": 0.56439394,
            "unexpected_purchases_source": "checking_savings",
            "damage_of_failure": 0.28314394,
            "stock_market_risk_level": "somewhat_risky",
            "trading_experience": "etfs_and_safe_stocks",
            "if_market_drops_20_i_will_buy": 0.5,
            "if_market_drops_40_i_will_buy": 0.5,
        }, 2),
        ({
            "profile_id": 22225,
            "risk_level": 0.5104166,
            "average_market_return": 6,
            "investment_horizon": 0.62689394,
            "unexpected_purchases_source": "other_loans",
            "damage_of_failure": 0.45549244,
            "stock_market_risk_level": "somewhat_risky",
            "trading_experience": "etfs_and_safe_stocks",
            "if_market_drops_20_i_will_buy": 0.5,
            "if_market_drops_40_i_will_buy": 0.5,
        }, 2),
        ({
            "profile_id": 22492,
            "risk_level": 0.4985207,
            "average_market_return": 15,
            "investment_horizon": 1,
            "unexpected_purchases_source": "checking_savings",
            "damage_of_failure": 0.50147927,
            "stock_market_risk_level": "neutral",
            "trading_experience": "very_little",
            "if_market_drops_20_i_will_buy": 0.5,
            "if_market_drops_40_i_will_buy": 0.5,
        }, 2),
    ]


@pytest.mark.parametrize("payload,expected_score", get_test_data())
def test_calculate_risk_score(payload, expected_score):
    service = RecommendationService(None)
    assert expected_score == service.calculate_risk_score(payload)
