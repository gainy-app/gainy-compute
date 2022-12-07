from gainy.context_container import ContextContainer


def test_ticker_match_score():
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
        collections_personalized = context_container.recommendation_service._get_recommended_collections_personalized(
            profile_id, limit)
        assert [(83, '0_83')] == list(collections_personalized)
        collections_global = context_container.recommendation_service._get_recommended_collections_global(
            profile_id, limit)
        assert [(8, '0_8'), (2, '0_2'),
                (83, '0_83')] == list(collections_global)
