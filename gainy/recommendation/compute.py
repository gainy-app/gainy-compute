from typing import Tuple, List, Iterable

from psycopg2._psycopg import connection
import numpy as np
import pandas as pd

from gainy.data_access.optimistic_lock import AbstractOptimisticLockingFunction
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.match_score import MatchScore, profile_ticker_similarity
from gainy.recommendation.models import MatchScoreModel
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.models import ProfileRecommendationsMetadata
from gainy.utils import db_connect, get_logger

MS_EXPL_CATEGORIES_MAX_COUNT = 2
MS_EXPL_INTERESTS_MAX_COUNT = 2


class ComputeRecommendationsAndPersist(AbstractOptimisticLockingFunction):

    def __init__(self, db_conn, profile_id, long_term_cache={}):
        super().__init__(RecommendationRepository(db_conn))
        self.profile_id = profile_id
        self.logger = get_logger(__name__)
        self.long_term_cache = long_term_cache

    def load_version(self, db_conn: connection):
        profile_metadata_list = self.repo.load(db_conn,
                                               ProfileRecommendationsMetadata,
                                               {"profile_id": self.profile_id})
        if len(profile_metadata_list) == 0:
            profile_metadata = ProfileRecommendationsMetadata()
            profile_metadata.profile_id = self.profile_id
            return profile_metadata
        else:
            return profile_metadata_list[0]

    def get_entities(self, db_conn: connection):
        tickers_with_match_score = self._get_and_sort_by_match_score()

        return [
            MatchScoreModel(self.profile_id, ticker, match_score)
            for ticker, match_score in tickers_with_match_score
        ]

    def _do_persist(self, db_conn, entities):
        super()._do_persist(db_conn, entities)

        top_20_tickers = [match_score.symbol for match_score in entities[:20]]
        self.repo.update_personalized_collection(self.profile_id,
                                                 TOP_20_FOR_YOU_COLLECTION_ID,
                                                 top_20_tickers)

    def _get_and_sort_by_match_score(self,
                                     top_k: int = None
                                     ) -> List[Tuple[str, MatchScore]]:

        match_scores_iterable = self.f_profile_vs_alltickers(
            self.profile_id,
            self.repo.get_df_ticker_symbols(),
            self.repo.get_df_profile_categories(self.profile_id),
            self.repo.get_df_profile_interests(self.profile_id),
            self.repo.get_df_profile_scoring_settings(self.profile_id),
            self.get_df_ticker_interests_continuous(),
            self.get_df_ticker_categories_continuous(),
            self.get_df_ticker_riskscore_continuous(),
        )

        if top_k is not None:
            match_scores_iterable = match_scores_iterable[:top_k]

        match_scores_list = list(match_scores_iterable)
        for symbol, match_score in match_scores_list:
            self.logger.debug('Calculated MS: %s', [
                self.profile_id, symbol, match_score.similarity,
                match_score.risk_similarity, match_score.category_similarity,
                match_score.category_matches, match_score.interest_similarity,
                match_score.interest_matches
            ])

        return match_scores_list

    def get_df_ticker_interests_continuous(self):
        if 'df_ticker_interests_continuous' not in self.long_term_cache:
            self.long_term_cache[
                'df_ticker_interests_continuous'] = self.repo.get_df_ticker_interests_continuous(
                )
        return self.long_term_cache['df_ticker_interests_continuous']

    def get_df_ticker_categories_continuous(self):
        if 'df_ticker_categories_continuous' not in self.long_term_cache:
            self.long_term_cache[
                'df_ticker_categories_continuous'] = self.repo.get_df_ticker_categories_continuous(
                )
        return self.long_term_cache['df_ticker_categories_continuous']

    def get_df_ticker_riskscore_continuous(self):
        if 'df_ticker_riskscore_continuous' not in self.long_term_cache:
            self.long_term_cache[
                'df_ticker_riskscore_continuous'] = self.repo.get_df_ticker_riskscore_continuous(
                )
        return self.long_term_cache['df_ticker_riskscore_continuous']

    def f_profile_vs_alltickers(
        self,
        profile_id: int,
        df_t,
        df_p_c,
        df_p_i,
        df_p_r,
        df_t_i,
        df_t_c,
        df_t_r,
        df_p_pi=None  #profile interests from portfolio
    ) -> Iterable[Tuple[str, MatchScore]]:
        ## we need to implement this alike function and process MS of all tickers for the user in a step (coz of "extremalizing ms values for each user's little world")
        # the approach is not speed-optimized, but shows how to do the task

        match_scores = {}
        data_symbols = []
        data_match_scores = []
        for symbol in set(df_t['symbol']):
            match_score = self.f_matchscore(profile_id, symbol, df_p_c, df_p_i,
                                            df_p_r, df_t_i, df_t_c, df_t_r,
                                            df_p_pi)
            match_scores[symbol] = match_score
            data_symbols.append(symbol)
            data_match_scores.append(match_score.similarity)

        df = pd.DataFrame(data={"matchscore": data_match_scores},
                          index=data_symbols)

        if len(df) > 10:
            ## extremalizing ms values for each user's little world
            # [-0.5..0.5]
            df.loc[:, 'matchscore'] = df.loc[:, 'matchscore'] - 0.5

            # rescale amplitude of right and left sides to the amplitude max limits
            max_value = df.matchscore.max(level=None)
            min_value = df.matchscore.min(level=None)
            if max_value > 0:
                df.loc[df['matchscore'] > 0, 'matchscore'] /= max_value
            if min_value < 0:
                df.loc[df['matchscore'] < 0, 'matchscore'] /= -min_value

            # from [-1..1] back to [0..1]
            df.loc[:, 'matchscore'] = (df.loc[:, 'matchscore'] + 1) / 2

        df = df.sort_values(['matchscore'], ascending=False)
        for symbol, row in df.iterrows():
            match_score = match_scores[symbol]
            match_score.similarity = row.matchscore
            yield symbol, match_score

    def f_matchscore(
        self,
        profile_id: int,
        symbol: str,
        df_p_c,  #profile categories
        df_p_i,  #profile interests
        df_p_r,  #profile risk
        df_t_i,  #ticker_interests
        df_t_c,  #ticker_categories_continuous
        df_t_r,  #ticker_risk
        df_p_pi=None  #profile interests from portfolio
    ) -> MatchScore:

        p_cat = set(df_p_c[df_p_c['profile_id'] == profile_id]['category_id'])
        p_int = set(df_p_i[df_p_i['profile_id'] == profile_id]['interest_id'])
        p_rsk = df_p_r.loc[0, 'risk_score']
        # minmax normlztn, from risk categories interval [1..3] to interval [0..1]
        p_rsk = (p_rsk - 1) / 2

        # [-1..1]
        t_cat = df_t_c.loc[df_t_c['symbol'] == symbol].set_index('category_id')
        # [-1..1]
        t_int = df_t_i.loc[df_t_i['symbol'] == symbol].set_index('interest_id')

        # [0..1]
        t_rsk = df_t_r.loc[df_t_r['symbol'] == symbol, 'risk_score']
        if len(t_rsk) == 0:  #if ticker not found
            t_rsk = 0.5
        else:
            t_rsk = t_rsk.iloc[0]

        ## risk match component
        # using parameterized bell function with center coord in user risk score - measure proximity of tickers
        # latex: \frac{1}{1+\left(s_{r}+\left(s_{c}-s_{r}\right)\cdot\frac{\left|a-0.5\right|}{0.5}\right)^{d}\cdot\left|a-x\right|^{d}},\ s_{r}=6.53,\ s_{c}=3.38,\ d=3.8,\ s_{c}\le s_{r},\ 0.\le a\le1.
        # desmos: https://www.desmos.com/calculator/tjkuomazcv ("a" stands for the coord, so move it around and look at black graph - it's the sensor region)
        d = 3.8
        sr = 6.53
        sc = 3.38
        a = p_rsk
        x = t_rsk
        match_comp_risk = 1. / (
            1. + abs(a - x)**d * abs(sr + (sc - sr) * abs(a - 0.5) / 0.5)**d)
        # and move it from [0..1] to [-1..1] . (0.5 there is meaningful threshold via function's parameters choosen)
        match_comp_risk = match_comp_risk * 2 - 1

        ## category match component
        # maxout category of ticker in crossed categories with user
        # if no crosses - put min eq -1.
        match_comp_category = t_cat.loc[list(p_cat.intersection(t_cat.index)),
                                        'sim_dif'].max()
        if np.isnan(match_comp_category):
            match_comp_category = -1

        ## interest match component
        # maxout interest of ticker in crossed interests with user
        # if no crosses - put min eq -1.
        match_comp_interest = t_int.loc[list(p_int.intersection(t_int.index)),
                                        'sim_dif'].max()
        if np.isnan(match_comp_interest):
            match_comp_interest = -1

        ## weighting up ticker by interest matching to portfolio's interests (if df with interests from portfolio is provided, here default=None just for demo with this adjustment and wo)
        if df_p_pi is not None:
            p_p_int = set(
                df_p_pi[df_p_pi['profile_id'] == profile_id]['interest_id'])
            for intrst in p_p_int.intersection(t_int.index):
                # x + (1-x) * ((1-x)/2 * (1 - (1-x)/2))^0.5
                #soft-weight bump 0.5 https://www.wolframalpha.com/input?i=plot+x+%2B+%281-x%29+*+%28%281-x%29%2F2+*+%281+-+%281-x%29%2F2%29%29%5E0.5%2C+x+where+x+from+-1+to+1
                int_adj = t_int[intrst] + (1. - t_int[intrst]) * \
                  abs((1. - t_int[intrst])/2. * (1. - (1.-t_int[intrst])/2.))**0.5
                if match_comp_interest < int_adj:
                    match_comp_interest = int_adj

        # we made 0. in each component as strong "inbetween" point by meaning of all measures
        # and each component is in interval [-1..1]
        # so we can now just make an average deviation from 0.
        msdev = (match_comp_risk + match_comp_category +
                 match_comp_interest) / 3
        # squeeze it /2 and move up by 0.5 to stay in [0..1]
        ms = msdev / 2 + 0.5

        # Explanation
        ms_expl_interests_matched = [{
            "interest_id": key,
            "value": t_int.loc[key, 'sim_dif']
        } for key in p_int.intersection(t_int.index)]
        ms_expl_interests_matched = sorted(
            ms_expl_interests_matched, key=lambda x: x['value'],
            reverse=True)[:MS_EXPL_INTERESTS_MAX_COUNT]
        ms_expl_interests_matched = filter(lambda x: x['value'] > 0,
                                           ms_expl_interests_matched)
        ms_expl_interests_matched = map(lambda x: x['interest_id'],
                                        ms_expl_interests_matched)

        ms_expl_categories_matched = [{
            "category_id": key,
            "value": t_cat.loc[key, 'sim_dif']
        } for key in p_cat.intersection(t_cat.index)]
        ms_expl_categories_matched = sorted(
            ms_expl_categories_matched, key=lambda x: x['value'],
            reverse=True)[:MS_EXPL_CATEGORIES_MAX_COUNT]
        ms_expl_categories_matched = filter(lambda x: x['value'] > 0,
                                            ms_expl_categories_matched)
        ms_expl_categories_matched = map(lambda x: x['category_id'],
                                         ms_expl_categories_matched)

        ms_expl_risk_similarity = match_comp_risk / 2 + 0.5
        ms_expl_category_similarity = match_comp_category / 2 + 0.5
        ms_expl_interest_similarity = match_comp_interest / 2 + 0.5

        ms_expl_is_matchedinterestsinportfolio = (df_p_pi is not None) and len(
            p_p_int.intersection(t_int.keys())) > 0

        #print(ms_expl_interests_matched)
        #print(ms_expl_interests_matched)
        #print(ms_expl_interest_highest_usedformatchscore)
        #print(ms_expl_categories_matched)
        #print(ms_expl_category_highest_usedformatchscore)
        #print(ms_expl_risk_similarity)
        #print(ms_expl_is_matchedinterestsinportfolio)

        return MatchScore(ms, ms_expl_risk_similarity,
                          ms_expl_category_similarity,
                          list(ms_expl_categories_matched),
                          ms_expl_interest_similarity,
                          list(ms_expl_interests_matched))
