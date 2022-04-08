with ticker_riskscore_categorial_weights_frominvestcats as
         (
             select tcc.symbol,
                    cat.risk_score               as risk_category,
                    (1. + max(tcc.sim_dif)) / 2. as weight
             from ticker_categories_continuous tcc
                      join categories cat on cat.id = tcc.category_id
             group by tcc.symbol, cat.risk_score
         ),

     ticker_riskscore_categorial_weights_fromvolatility as
         (
             with ticker_volatility_nrmlzd as
                      (
                          select symbol,
                                 (tm.absolute_historical_volatility_adjusted_current - maxmin.v_min) /
                                 (1e-30 + (maxmin.v_max - maxmin.v_min)) as val
                          from ticker_metrics tm
                                   left join
                               (select max(absolute_historical_volatility_adjusted_current) as v_max,
                                       min(absolute_historical_volatility_adjusted_current) as v_min
                                from ticker_metrics tm
                                where tm.absolute_historical_volatility_adjusted_current is not null) as maxmin on true
                          where tm.absolute_historical_volatility_adjusted_current is not null
                      )
                  -- using parameterized bell function - place 3 sensory bells to measure proximity for each risk category (1,2,3) in one dimension
                  -- desmos: https://www.desmos.com/calculator/olnm6cthlt ("a" stands for the coord, so move it around and look at black graph - it's the sensor region)
                 (select symbol, -- risk = 1, bell-sensor coord={0.0}
                         1::int                                                 as risk_category,
                         1. / (1. + abs(0.0 - tvn.val) ^ 3.8
                             * (8.3 + (4. - 8.3) * abs(0.0 - 0.5) / 0.5) ^ 3.8) as weight
                  from ticker_volatility_nrmlzd tvn)
             union
             (select symbol, -- risk = 2 , bell-sensor coord={0.5}
                     2::int                                                 as risk_category,
                     1. / (1. + abs(0.5 - tvn.val) ^ 3.8
                         * (8.3 + (4. - 8.3) * abs(0.5 - 0.5) / 0.5) ^ 3.8) as weight
              from ticker_volatility_nrmlzd tvn)
             union
             (select symbol, -- risk = 3 , bell-sensor coord={1.0}
                     3::int                                                 as risk_category,
                     1. / (1. + abs(1.0 - tvn.val) ^ 3.8
                         * (8.3 + (4. - 8.3) * abs(1.0 - 0.5) / 0.5) ^ 3.8) as weight
              from ticker_volatility_nrmlzd tvn)
         ),

     ticker_riskscore_categorial_weights as
         (
             select -- in case when we don't have enought risk information from ours invest.categories - we mix with volatility information
                    coalesce(trsc.symbol, trsv.symbol)               as symbol,
                    coalesce(trsc.risk_category, trsv.risk_category) as risk_category,
                    case
                        when coalesce(trsc.weight, 0.) = 0
                            then trsv.weight
                        else trsc.weight
                        end                                          as weight
             from ticker_riskscore_categorial_weights_frominvestcats trsc
                      full outer join ticker_riskscore_categorial_weights_fromvolatility trsv
                                      on trsv.symbol = trsc.symbol and trsv.risk_category = trsc.risk_category
         ),


     ticker_riskscore_onedimensional_weighted as
         (
             select trcw.symbol,
                    case
                        when sum(trcw.weight) = 0
                            then 0.5 -- in probably non-existing case if all weights was 0 (0 volatility of adjusted_close from eod's "historical_prices")
                        else sum(trcw.risk_category * trcw.weight) / (1e-30 + sum(trcw.weight)) - 2. -- [1..3]=>(-1..1)
                        end as risk
             from ticker_riskscore_categorial_weights trcw
             group by trcw.symbol
         ),

-- 0=centered medium risk. but because we were used weighting and mixing we don't effectively touch the negative and positive limits [-1] and [+1]
-- but we need touch the limits in full scale [-1..1] - to interpret the lowest possible risk ticker and highest possible
-- so we now need to renorm negative and positive sides to touch the limits

     scalekoefs as
         (
             select 1e-30 + coalesce((select MAX(risk) from ticker_riskscore_onedimensional_weighted where risk > 0),
                                     0.)                                                                           as risk_k_u,
                    1e-30 + coalesce((select MAX(-risk) from ticker_riskscore_onedimensional_weighted where risk < 0),
                                     0.)                                                                           as risk_k_d
         )


select trod.symbol,
       (1. + trod.risk / (case when trod.risk > 0 then s.risk_k_u else s.risk_k_d end)) / 2. as risk_score, --[0..1]
       now()::timestamp                                                                      as updated_at
from ticker_riskscore_onedimensional_weighted trod
         left join scalekoefs as s on true -- one row
         join tickers using (symbol) -- ticker_metrics has somehow sometimes more tickers than in table tickers and sometimes less, so filter
