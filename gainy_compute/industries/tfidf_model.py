from collections import \
    OrderedDict, \
    Counter  # we need to preserve order of phrases to clean, but native dict preserve order of keys only from python 3.7 (google colab uses 3.6, sorry)
from typing import Dict
from typing import OrderedDict

import math
import nltk
import numpy as np
import pandas as pd
import regex  # for diacritics characters processing with \p{Mn} (sorry re)
import unicodedata
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from nltk.tokenize import wordpunct_tokenize, PunktSentenceTokenizer

from gainy_compute.industries.model import IndustryAssignmentModel


class TfIdfIndustryAssignmentModel(IndustryAssignmentModel):

    nltk.download("stopwords")
    nltk.download("punkt")

    def __init__(self):
        # 1. generate 3 permanent dicts for description preparations.
        #    we need to use that 3 dicts all the time we want to prepare any description anywhere
        #    generate this 3 OrderedDicts once, save it and just load every next time
        self.stop_words = textclean_createtextstoremove()
        self.vocab_all_tokens_idf = None
        self.vocab_vocab_industry_tokens_tfidfnorm = None

    def description(self) -> str:
        return "Industry assignment model based on TF * IDF similarity of ticker descriptions"

    def fit(self, X, y):
        # 2. generate per industry tfidf_L2normalized vectors

        #    we need to store examples of tickers with industry that we sure about
        #    and use that data anytime we want to generate industry vectors
        #    and from time to time we can add more examples so that vectors would become better and better
        #      and in between that updates we need to store 2 dicts: 1.total vocab with idf-weights; 2.per-industry vocabs with tfidfL2normalized vectors\

        tic_desc = list(X[X.columns[0]])
        tic_ind = list(y[y.columns[0]])
        tic_desc = textclean_all(tic_desc, self.stop_words)  # prepare descriptions (cleaning,steming)

        #    get 2 dictionaries: 1.total vocab with idf-weights; 2.per-industry vocabs with tfidfL2normalized vectors
        self.vocab_all_tokens_idf, self.vocab_vocab_industry_tokens_tfidfnorm = generate_industrytokenstfidf_vocabs(
            tic_ind, tic_desc)

    def predict(self, descriptions, n: int = 2, include_distances: bool = False):
        if not self.vocab_all_tokens_idf or not self.vocab_vocab_industry_tokens_tfidfnorm:
            raise Exception("Fit model first")

        # 3. Generate topN indutries for any newbie ticker with description (using stored 2 dicts about industries and 3 dicts for description preps from above all the time)
        tic_desc = descriptions[descriptions.columns[0]].to_numpy()

        #    prepare description
        tic_desc = textclean_all(tic_desc,  self.stop_words)  # prepare descriptions (cleaning,steming)
        #    TermFreq for each description
        tic_tf = tokenize_gettf(tic_desc)
        #    get topN (2 in example) industries names and cossim measures
        tic_topind_names, tic_topind_cossim = self._tfidfcossim(self.vocab_all_tokens_idf,
                                                                self.vocab_vocab_industry_tokens_tfidfnorm, tic_tf, n)

        if include_distances:
            return tic_topind_names, tic_topind_cossim
        else:
            return tic_topind_names

    def _tfidfcossim(self, d_all: dict,
                     # token -> idf (N=count of all  industries, n=count of industries that have this token. Overal vocab. )
                     d_ind: dict,
                     # industry->token-> tfidf L2-normalized (where tf was averaged from companies in that industry (averaged tf is a "average portrait of companies in that industry in TF spector"))
                     l_tictf: list,  # ticker->token->tf (just freq countings)
                     ntop=2) -> (list, list):
        # d_all struct:
        # vocab_all_tokens_idf{'token':idf, ...}
        # d_ind struct:
        # vocab_industry_tokens_tfidfnorm{'ind':{'token':tfidfnorm,
        #                                       ...},
        #                                ...}
        # l_tictf struct:
        # [{'token':tf, ...},
        # ...]

        # d_all & d_ind are generated once upon a time as ethalon basis from descriptions of tickers with wich we sure about chosen industry

        # unpack d_ind to sparse transposed 2D: row=token, col=industry, cell=tfidfnormalizedL2
        # any order of industry in the dictionary d_ind will be reflected in the numpy coll indices (1st np dim) (til we don't trigger dict rehash..)
        # any order of tokens in the dictionary d_all will be reflected in the numpy row indices (0st np dim) (til we don't trigger dict rehash..)
        # r:token c:industry tfidfnorm_transposed
        tokind_tfidfnrm = np.array([[i_v.get(a_k, 0.) for i_v in d_ind.values()] for a_k in d_all.keys()])

        # unpack l_n to sparse 2D: row=company, col=token, cell=tf
        # drop any token not in vocab d_all (we work in the space of only known tokens)
        # mult by idf from d_all
        # any order again is inherited (rows from l_tictf, cols from d_all)
        # r:ticker, c:token
        tictok_tfidfnrm = np.array([[t_d.get(a_k, 0.) * a_v for a_k, a_v in d_all.items()] for t_d in
                                    l_tictf])  # tfidf, need to normalize row-wise
        tictok_tfidfnrm = tictok_tfidfnrm / (np.sum(tictok_tfidfnrm ** 2, axis=-1, keepdims=True,
                                                    initial=1e-30)) ** 0.5  # now tfidfnrm (1e-30 is epsilon for sake of esc ezd)

        # tictok_tfidfnrm matmul tokind_tfidfnrmt, shapes aligned: (tic,tok)@(tok,ind)->(tic,ind)
        # and coz both are normalized (divided) by L2-norm -> we will get "cosine similarity"
        ticind = np.matmul(tictok_tfidfnrm, tokind_tfidfnrm)
        tictok_tfidfnrm = None
        tokind_tfidfnrm = None

        # returning back 2 lists for tickers:
        # tic_topn_industries_names
        # tic_topn_industries_similarity
        ind_names = list(d_ind.keys())
        topindex = ticind.argsort(axis=-1)[:, ::-1][:,
                   :max(1, ntop)]  # ascending->reverse_index(axis 1)->cut_topn (highest cos_sim)
        tic_topn_industries_names = np.array(ind_names)[topindex].tolist()
        tic_topn_industries_similarity = np.take_along_axis(ticind, topindex, -1).tolist()

        return (tic_topn_industries_names, tic_topn_industries_similarity)


def textclean_createtextstoremove() -> (Dict[str, str], Dict[str, str]):
    list_prep = []

    # english stop-words
    stop_1 = stopwords.words("english")

    list_prep += stop_1

    list_prep = sorted(filter(lambda x: len(x) > 0,
                              set(map(str.lower,
                                      list_prep))),
                       key=lambda x: (-len(x), x), reverse=False)

    stop_words = OrderedDict.fromkeys(list_prep, " ")

    return stop_words


"""Functions to clean the Description strings"""


def remove_accents(text: str) -> str:
    """Give approx. 0.02% in MAP metric"""
    return regex.sub(r"\p{Mn}", "", unicodedata.normalize("NFD", text))


def textclean_all(texts: list, all_words: Dict[str, str]):
    """The function to correctly clean out all the dirt (gold-diger for tokens that are relevant to products&services of company)"""

    sentence_tokenize = PunktSentenceTokenizer().tokenize

    tokens_list = []
    for text in texts:
        text = remove_accents(text)

        text_tokens = []
        for sentence in sentence_tokenize(text):
            for word in wordpunct_tokenize(sentence):
                if not word.isalnum():
                    continue

                if word.isnumeric():
                    continue

                if len(word) <= 1:
                    continue

                if word in all_words:
                    continue

                stemmer = SnowballStemmer("english")
                text_tokens.append(stemmer.stem(word))

        tokens_list.append(" ".join(text_tokens))

    return tokens_list


def generate_industrytokenstfidf_vocabs(
        txts_ind:list, #industries
        txts_des:list  #tickers descriptions
)->(
        dict,  #vocab_all_tokens_idf
        dict): #vocab_industry_tokens_tfidfnorm

    #all txts_des suppose to be correctly preprocessed: words separated by whitespace

    #d_all struct:
    #vocab_all_tokens_idf{'token':idf, ...}
    #d_ind struct:
    #vocab_industry_tokens_tfidfnorm{'ind':{'token':tfidfnorm,
    #                                       ...},
    #                                ...}
    #l_tictf struct:
    #[{'token':tf, ...},
    # ...]

    #we're using safe+soft idf(t)=1+log((1+n)/(1+df(t)))
    #where "n" is the total number of industries, and df(t) is the number of industries set that contain term "t".

    vocab_industry_tokens_cnts = dict()
    for ind,des in zip(txts_ind,txts_des):
        vocab_industry_tokens_cnts.setdefault(ind,Counter()).update([t for t in filter(lambda x: x!="", des.split(" ",maxsplit=-1))])

    ind_cnt = Counter(txts_ind)
    vocab_industry_tokens_tfidfnorm = dict()
    vocab_all_tokens_idf = Counter()
    for k,v in vocab_industry_tokens_cnts.items():
        vocab_industry_tokens_tfidfnorm[k] = dict(zip(v.keys(), map(lambda x: x/ind_cnt[k], v.values())))
        vocab_all_tokens_idf.update(v.keys())
    vocab_industry_tokens_cnts = None
    vocab_all_tokens_idf = dict(vocab_all_tokens_idf)
    for k,v in vocab_all_tokens_idf.items():
        vocab_all_tokens_idf[k] = 1+math.log((1+len(ind_cnt))/(1+v))
    for k_i,v_i in vocab_industry_tokens_tfidfnorm.items():
        for k_t,v_t in v_i.items():
            vocab_industry_tokens_tfidfnorm[k_i][k_t] = v_t * vocab_all_tokens_idf[k_t] #tf * idf
        l2norm = sum(map(lambda x: x**2, vocab_industry_tokens_tfidfnorm[k_i].values()))**0.5
        for k_t,v_t in v_i.items():
            vocab_industry_tokens_tfidfnorm[k_i][k_t] /= (1e-30 + l2norm)
    #...better to use the numpy than this...
    return (vocab_all_tokens_idf, vocab_industry_tokens_tfidfnorm)


def tokenize_gettf(texts:list)->list: #returns list of TF dicts (order of list preserved)
    #all texts suppose to be correctly preprocessed: words separated by whitespace
    return list(map(lambda text: dict(Counter([t for t in filter(lambda x: x!="",
                                                                 text.split(" ",maxsplit=-1))])),
                    texts))