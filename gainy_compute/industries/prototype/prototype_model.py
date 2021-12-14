from gainy_compute.industries.prototype.cleanse import textclean_createtextstoremove, generate_industrytokenstfidf_vocabs, \
    textclean_all, tokenize_gettf
from compute.industries.model import IndustryAssignmentModel
import pandas as pd
import numpy as np


class IndustryAssigmentPrototypeModel(IndustryAssignmentModel):

    """
    The original prototype developed by the Data Science team
    """

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
        tic_topind_names, tic_topind_cossim = self._tfidfcossim(self.vocab_all_tokens_idf, self.vocab_vocab_industry_tokens_tfidfnorm, tic_tf, n)

        if include_distances:
            return tic_topind_names, tic_topind_cossim
        else:
            return tic_topind_names

    def _tfidfcossim(self, d_all:dict,   #token -> idf (N=count of all  industries, n=count of industries that have this token. Overal vocab. )
                    d_ind:dict,   #industry->token-> tfidf L2-normalized (where tf was averaged from companies in that industry (averaged tf is a "average portrait of companies in that industry in TF spector"))
                    l_tictf:list, #ticker->token->tf (just freq countings)
                    ntop=2)->(list,list):
        #d_all struct:
        #vocab_all_tokens_idf{'token':idf, ...}
        #d_ind struct:
        #vocab_industry_tokens_tfidfnorm{'ind':{'token':tfidfnorm,
        #                                       ...},
        #                                ...}
        #l_tictf struct:
        #[{'token':tf, ...},
        # ...]

        # d_all & d_ind are generated once upon a time as ethalon basis from descriptions of tickers with wich we sure about chosen industry

        #unpack d_ind to sparse transposed 2D: row=token, col=industry, cell=tfidfnormalizedL2
        #any order of industry in the dictionary d_ind will be reflected in the numpy coll indices (1st np dim) (til we don't trigger dict rehash..)
        #any order of tokens in the dictionary d_all will be reflected in the numpy row indices (0st np dim) (til we don't trigger dict rehash..)
        #r:token c:industry tfidfnorm_transposed
        tokind_tfidfnrm = np.array([[i_v.get(a_k, 0.) for i_v in d_ind.values()] for a_k in d_all.keys()])

        #unpack l_n to sparse 2D: row=company, col=token, cell=tf
        #drop any token not in vocab d_all (we work in the space of only known tokens)
        #mult by idf from d_all
        #any order again is inherited (rows from l_tictf, cols from d_all)
        #r:ticker, c:token
        tictok_tfidfnrm = np.array([[t_d.get(a_k, 0.)*a_v for a_k, a_v in d_all.items()] for t_d in l_tictf]) #tfidf, need to normalize row-wise
        tictok_tfidfnrm = tictok_tfidfnrm / (np.sum(tictok_tfidfnrm**2, axis=-1, keepdims=True, initial=1e-30))**0.5 #now tfidfnrm (1e-30 is epsilon for sake of esc ezd)

        # tictok_tfidfnrm matmul tokind_tfidfnrmt, shapes aligned: (tic,tok)@(tok,ind)->(tic,ind)
        # and coz both are normalized (divided) by L2-norm -> we will get "cosine similarity"
        ticind = np.matmul(tictok_tfidfnrm, tokind_tfidfnrm)
        tictok_tfidfnrm = None
        tokind_tfidfnrm = None

        #returning back 2 lists for tickers:
        #tic_topn_industries_names
        #tic_topn_industries_similarity
        ind_names = list(d_ind.keys())
        topindex = ticind.argsort(axis=-1)[:,::-1][:,:max(1,ntop)] #ascending->reverse_index(axis 1)->cut_topn (highest cos_sim)
        tic_topn_industries_names = np.array(ind_names)[topindex].tolist()
        tic_topn_industries_similarity = np.take_along_axis(ticind,topindex,-1).tolist()

        return (tic_topn_industries_names, tic_topn_industries_similarity)

