
import pandas as pd

from gainy.compute.collection_utils import batch_iter
from gainy.compute.industries.cleanse import textclean_createtextstoremove, textclean_all, tokenize_gettf, tfidfcossim, \
    generate_industrytokenstfidf_vocabs

from gainy.compute.industries.idf_model import TfIdfIndustryAssigmentModel
from gainy.compute.industries.lifecycle import cross_validation


def test_batch_iter():
    for batch in batch_iter([1, 2, 3, 4, 5, 6, 7, 8, 9], 4):
        print(batch)


def test_process_2():
    industry_tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/industry_tickers.csv")
    industry_tickers = industry_tickers.rename(columns={"Industry Name": "ind_name", "Code": "symbol"})

    tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/tickers.csv")[["symbol", "description"]]

    tickers_with_industries = tickers.merge(industry_tickers, how="inner", on=["symbol"])
    tickers_with_industries["symbol"] = tickers_with_industries["symbol"].astype(str)
    tickers_with_industries["description"] = tickers_with_industries["description"].astype(str)

    model = TfIdfIndustryAssigmentModel()
    model.fit(tickers_with_industries[["description"]], tickers_with_industries[["ind_name"]])

    industries = model.classify("Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. It also sells various related services. The company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, HomePod, iPod touch, and other Apple-branded and third-party accessories. It also provides AppleCare support services; cloud services store services; and operates various platforms, including the App Store, that allow customers to discover and download applications and digital content, such as books, music, video, games, and podcasts. In addition, the company offers various services, such as Apple Arcade, a game subscription service; Apple Music, which offers users a curated listening experience with on-demand radio stations; Apple News+, a subscription news and magazine service; Apple TV+, which offers exclusive original content; Apple Card, a co-branded credit card; and Apple Pay, a cashless payment service, as well as licenses its intellectual property. The company serves consumers, and small and mid-sized businesses; and the education, enterprise, and government markets. It sells and delivers third-party applications for its products through the App Store. The company also sells its products through its retail and online stores, and direct sales force; and third-party cellular network carriers, wholesalers, retailers, and resellers. Apple Inc. was founded in 1977 and is headquartered in Cupertino, California.", n=2)
    print(industries)


def test_cross_validation():
    industry_tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/industry_tickers.csv")
    industry_tickers = industry_tickers.rename(columns={"Industry Name": "ind_name", "Code": "symbol"})

    tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/tickers.csv")[["symbol", "description"]]

    tickers_with_industries = tickers.merge(industry_tickers, how="inner", on=["symbol"])
    tickers_with_industries["symbol"] = tickers_with_industries["symbol"].astype(str)
    tickers_with_industries["description"] = tickers_with_industries["description"].astype(str)

    X = tickers_with_industries[["description"]]
    y = tickers_with_industries[["ind_name"]]

    model = TfIdfIndustryAssigmentModel()
    res = cross_validation(model, X, y)

    print(res)


# def process(df: pd.DataFrame):
#     # 1. generate 3 permanent dicts for description preparations.
#     #    we need to use that 3 dicts all the time we want to prepare any description anywhere
#     #    generate this 3 OrderedDicts once, save it and just load every next time
#     dict_1, dict_2, dict_3 = textclean_createtextstoremove()
#
#     # 2. generate per industry tfidf_L2normalized vectors
#
#     #    we need to store examples of tickers with industry that we sure about
#     #    and use that data anytime we want to generate industry vectors
#     #    and from time to time we can add more examples so that vectors would become better and better
#     #      and in between that updates we need to store 2 dicts: 1.total vocab with idf-weights; 2.per-industry vocabs with tfidfL2normalized vectors
#
#     df_ind_gen = df[~(df['ind_name'] == '')][
#         ['ind_name', 'symbol', 'description']].copy()  # for now we use tickers with assigned industry from our prod db
#
#     tic_ind = list(df_ind_gen['ind_name'])
#     tic_desc = list(df_ind_gen['description'])
#     tic_desc = textclean_all(tic_desc, dict_1, dict_2, dict_3)  # prepare descriptions (cleaning,steming)
#
#     #    get 2 dictionaries: 1.total vocab with idf-weights; 2.per-industry vocabs with tfidfL2normalized vectors
#     vocab_all_tokens_idf, vocab_vocab_industry_tokens_tfidfnorm = generate_industrytokenstfidf_vocabs(tic_ind, tic_desc)
#
#     # 3. Generate topN indutries for any newbie ticker with description (using stored 2 dicts about industries and 3 dicts for description preps from above all the time)
#     df_tic_getind = df[['ind_name', 'symbol', 'description']].copy()
#     tic_symbol = list(df_tic_getind['symbol'])
#     tic_desc = list(df_tic_getind['description'])
#
#     #    prepare description
#     tic_desc = textclean_all(tic_desc, dict_1, dict_2, dict_3)  # prepare descriptions (cleaning,steming)
#     #    TermFreq for each description
#     tic_tf = tokenize_gettf(tic_desc)
#     #    get topN (2 in example) industries names and cossim measures
#     tic_topind_names, tic_topind_cossim = tfidfcossim(vocab_all_tokens_idf, vocab_vocab_industry_tokens_tfidfnorm,
#                                                       tic_tf, 4)
#
#     #   have a look
#     tic_new_industries_df = pd.DataFrame(data=zip(tic_symbol, tic_topind_names, tic_topind_cossim, tic_desc), columns=[
#         ['symbol', 'generated_industries_names', 'generated_industries_cossim', 'description']])
#     return tic_new_industries_df
#
#
# def test_process():
#     # TDACW
#
#     industry_tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/industry_tickers.csv")
#     industry_tickers = industry_tickers.rename(columns={"Industry Name": "ind_name", "Code": "symbol"})
#
#     tickers = pd.read_csv("/Users/vasilii/dev/data/industries/ds1/tickers.csv")[["symbol", "description"]]
#
#     tickers_with_industries = tickers.merge(industry_tickers, how="inner", on=["symbol"])
#     tickers_with_industries["symbol"] = tickers_with_industries["symbol"].astype(str)
#     tickers_with_industries["description"] = tickers_with_industries["description"].astype(str)
#
#     res = process(tickers_with_industries)
#     print(res)