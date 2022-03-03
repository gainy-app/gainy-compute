with ticker_industry_vectors as (
    select symbol, json_object_agg(industry_id, 1.0) as ticker_industry_vector
    from ticker_industries
    group by symbol
),
     ticker_category_vectors as (
         select symbol, json_object_agg(category_id, 1.0) as ticker_category_vector
         from ticker_categories
         group by symbol
     )
select t.symbol, tiv.ticker_industry_vector, tcv.ticker_category_vector
from tickers t
         left join ticker_industry_vectors tiv
                   on t.symbol = tiv.symbol
         left join ticker_category_vectors tcv
                   on t.symbol = tcv.symbol;