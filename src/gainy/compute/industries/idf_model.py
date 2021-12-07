from typing import List

from gainy.compute.industries.cleanse import textclean_createtextstoremove, generate_industrytokenstfidf_vocabs, \
    textclean_all, tokenize_gettf, tfidfcossim
from gainy.compute.industries.model import IndustryAssignmentModel
import pandas as pd


class TfIdfIndustryAssigmentModel(IndustryAssignmentModel):

    def __init__(self):
        # 1. generate 3 permanent dicts for description preparations.
        #    we need to use that 3 dicts all the time we want to prepare any description anywhere
        #    generate this 3 OrderedDicts once, save it and just load every next time
        self.dict_1, self.dict_2, self.dict_3 = textclean_createtextstoremove()
        self.vocab_all_tokens_idf = None
        self.vocab_vocab_industry_tokens_tfidfnorm = None

    def fit(self, X, y):
        # 2. generate per industry tfidf_L2normalized vectors

        #    we need to store examples of tickers with industry that we sure about
        #    and use that data anytime we want to generate industry vectors
        #    and from time to time we can add more examples so that vectors would become better and better
        #      and in between that updates we need to store 2 dicts: 1.total vocab with idf-weights; 2.per-industry vocabs with tfidfL2normalized vectors\

        df = pd.concat([X, y], axis=1)

        df_ind_gen = df[~(df['ind_name'] == '')][
            ['ind_name', 'description']].copy()  # for now we use tickers with assigned industry from our prod db

        tic_ind = list(df_ind_gen['ind_name'])
        tic_desc = list(df_ind_gen['description'])
        tic_desc = textclean_all(tic_desc, self.dict_1, self.dict_2, self.dict_3)  # prepare descriptions (cleaning,steming)

        #    get 2 dictionaries: 1.total vocab with idf-weights; 2.per-industry vocabs with tfidfL2normalized vectors
        self.vocab_all_tokens_idf, self.vocab_vocab_industry_tokens_tfidfnorm = generate_industrytokenstfidf_vocabs(tic_ind, tic_desc)

    def classify(self, descriptions, n: int = 2, include_distances: bool = False):
        if not self.vocab_all_tokens_idf or not self.vocab_vocab_industry_tokens_tfidfnorm:
            raise Exception("Fit model first")

        # 3. Generate topN indutries for any newbie ticker with description (using stored 2 dicts about industries and 3 dicts for description preps from above all the time)
        tic_desc = descriptions[descriptions.columns[0]].to_numpy()

        #    prepare description
        tic_desc = textclean_all(tic_desc, self.dict_1, self.dict_2, self.dict_3)  # prepare descriptions (cleaning,steming)
        #    TermFreq for each description
        tic_tf = tokenize_gettf(tic_desc)
        #    get topN (2 in example) industries names and cossim measures
        tic_topind_names, tic_topind_cossim = tfidfcossim(self.vocab_all_tokens_idf, self.vocab_vocab_industry_tokens_tfidfnorm, tic_tf, n)

        if include_distances:
            return tic_topind_names, tic_topind_cossim
        else:
            return tic_topind_names
