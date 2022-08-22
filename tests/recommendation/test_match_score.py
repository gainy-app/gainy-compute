from gainy.utils import db_connect
from gainy.recommendation.compute import generate_all_match_scores
from psycopg2.extras import RealDictCursor


def test_ticker_match_score():
    with db_connect() as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO app.profiles (email, first_name, last_name, gender, created_at, user_id, avatar_url, legal_address) VALUES ('test3@example.com', 'fn', 'ln', 0, '2021-10-20 16:02:34.514475 +00:00', 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3', '', 'legal_address') returning id"
            )

            profile_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO app.profile_holdings (iso_currency_code, quantity, security_id, profile_id, account_id, ref_id,
                                                  created_at, updated_at, plaid_access_token_id)
                VALUES ('USD', 1, 49, %(profile_id)s, 486, 'dvqO7MRMx3HJQxLOxv0NCqj9PZjRY1TbVMzQa_ODbxOoxka6fPx3Xy4xkEIMDPJAPLpOfMPXEry',
                        '2021-12-13 06:34:51.423171 +00:00', '2022-04-08 07:02:36.062043 +00:00', 10);
                INSERT INTO app.profile_scoring_settings (profile_id, created_at, risk_level, average_market_return, investment_horizon, unexpected_purchases_source, damage_of_failure, stock_market_risk_level, trading_experience, if_market_drops_20_i_will_buy, if_market_drops_40_i_will_buy, risk_score)
                VALUES (%(profile_id)s, '2021-10-20 16:02:34.514475 +00:00', 0.5, 6, 0.5, 'checking_savings', 0.5, 'very_risky', 'never_tried', 0.5, 0.5, 2);

                INSERT INTO app.profile_interests (profile_id, interest_id) VALUES (%(profile_id)s, 5);
                INSERT INTO app.profile_categories (profile_id, category_id) VALUES (%(profile_id)s, 2), (%(profile_id)s, 5), (%(profile_id)s, 7);

                INSERT INTO collection_ticker_actual_weights (date, profile_id, collection_id, collection_uniq_id, symbol, weight)
                VALUES ('2022-08-22', null, 83, '0_83', 'AAPL', 1);
                """, {"profile_id": profile_id})

        generate_all_match_scores(db_conn, [profile_id])
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * from app.profile_ticker_match_score where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            ticker_match_score = cursor.fetchone()

            cursor.execute(
                "SELECT * from app.profile_collection_match_score where profile_id = %(profile_id)s",
                {"profile_id": profile_id})

            collection_match_score = cursor.fetchone()

    assert ticker_match_score["profile_id"] == profile_id
    assert ticker_match_score["symbol"] == 'AAPL'
    assert ticker_match_score["match_score"] >= 99
    assert ticker_match_score["fits_risk"] == 2
    assert abs(ticker_match_score["risk_similarity"] -
               0.9990181336784723) < 1e-5
    assert ticker_match_score["fits_categories"] == 2
    assert ticker_match_score["fits_interests"] == 2
    assert ticker_match_score["category_matches"] == '[7]'
    assert ticker_match_score["interest_matches"] == '[5]'
    assert abs(ticker_match_score["category_similarity"] -
               0.8620509848080247) < 1e-5
    assert abs(ticker_match_score["interest_similarity"] -
               0.8232970416257972) < 1e-5
    assert ticker_match_score["matches_portfolio"] == True

    assert collection_match_score["profile_id"] == profile_id
    assert collection_match_score["collection_id"] == 83
    assert collection_match_score["collection_uniq_id"] == '0_83'
    assert collection_match_score["match_score"] >= 99
    assert abs(collection_match_score["risk_similarity"] -
               0.9990181336784723) < 1e-5
    assert abs(collection_match_score["category_similarity"] -
               0.8620509848080247) < 1e-5
    assert abs(collection_match_score["interest_similarity"] -
               0.8232970416257972) < 1e-5
    assert collection_match_score["risk_level"] == 2
    assert collection_match_score["category_level"] == 2
    assert collection_match_score["interest_level"] == 2
