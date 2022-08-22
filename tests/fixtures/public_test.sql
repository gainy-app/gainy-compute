create schema if not exists public_test;

create table if not exists public_test.tickers
(
    symbol           varchar
        constraint tickers_unique_symbol
            unique,
    type             varchar,
    name             varchar,
    description      varchar,
    phone            varchar,
    logo_url         varchar,
    web_url          varchar,
    ipo_date         date,
    sector           varchar,
    industry         varchar,
    gic_sector       varchar,
    gic_group        varchar,
    gic_industry     varchar,
    gic_sub_industry varchar,
    exchange         varchar,
    country_name     text,
    updated_at       timestamp
);
INSERT INTO public_test.tickers (symbol, type, name, description, phone, logo_url, web_url, ipo_date, sector, industry,
                                 gic_sector, gic_group, gic_industry, gic_sub_industry, exchange, country_name,
                                 updated_at)
VALUES ('AAPL', 'common stock', 'Apple Inc',
        '"Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories worldwide. It also sells various related services. In addition, the company offers iPhone, a line of smartphones; Mac, a line of personal computers; iPad, a line of multi-purpose tablets; AirPods Max, an over-ear wireless headphone; and wearables, home, and accessories comprising AirPods, Apple TV, Apple Watch, Beats products, HomePod, and iPod touch. Further, it provides AppleCare support services; cloud services store services; and operates various platforms, including the App Store that allow customers to discover and download applications and digital content, such as books, music, video, games, and podcasts. Additionally, the company offers various services, such as Apple Arcade, a game subscription service; Apple Music, which offers users a curated listening experience with on-demand radio stations; Apple News+, a subscription news and magazine service; Apple TV+, which offers exclusive original content; Apple Card, a co-branded credit card; and Apple Pay, a cashless payment service, as well as licenses its intellectual property. The company serves consumers, and small and mid-sized businesses; and the education, enterprise, and government markets. It distributes third-party applications for its products through the App Store. The company also sells its products through its retail and online stores, and direct sales force; and third-party cellular network carriers, wholesalers, retailers, and resellers. Apple Inc. was incorporated in 1977 and is headquartered in Cupertino, California."',
        '408 996 1010', '/img/logos/US/aapl.png', 'https://www.apple.com', '1980-12-12', 'Technology',
        'Consumer Electronics', 'Information Technology', 'Technology Hardware & Equipment',
        'Technology Hardware, Storage & Peripherals', 'Technology Hardware, Storage & Peripherals', 'NASDAQ',
        'United States', '2022-04-08 13:41:06.234950')
on conflict do nothing;


create table if not exists public_test.ticker_interests
(
    id          varchar
        constraint ticker_interests_unique_id
            unique,
    symbol      text,
    interest_id integer,
    sim_dif     double precision,
    updated_at  timestamp
);
INSERT INTO public_test.ticker_interests (id, symbol, interest_id, sim_dif, updated_at)
VALUES ('AAPL_17', 'AAPL', 17, -0.43486976150896905, '2022-04-08 13:42:02.754064'),
       ('AAPL_12', 'AAPL', 12, 0.6465940832515944, '2022-04-08 13:42:02.754064'),
       ('AAPL_5', 'AAPL', 5, 0.6465940832515944, '2022-04-08 13:42:02.754064'),
       ('AAPL_39', 'AAPL', 39, -0.43486976150896905, '2022-04-08 13:42:02.754064'),
       ('MARK_5', 'MARK', 5, 0.7241019696160493, '2022-04-08 14:03:49.031875')
on conflict do nothing;

create table if not exists public_test.ticker_categories_continuous
(
    id          varchar
        constraint ticker_categories_continuous_unique_id
            unique,
    category_id integer,
    symbol      varchar,
    sim_dif     double precision,
    updated_at  timestamp
);

INSERT INTO public_test.ticker_categories_continuous (id, category_id, symbol, sim_dif, updated_at)
VALUES ('2_AAPL', 2, 'AAPL', -0.6774838263358212, '2022-04-08 14:03:49.031875'),
       ('5_AAPL', 5, 'AAPL', -0.9782500645186989, '2022-04-08 14:03:49.031875'),
       ('6_AAPL', 6, 'AAPL', 0.6701566971735885, '2022-04-08 14:03:49.031875'),
       ('7_AAPL', 7, 'AAPL', 0.7241019696160493, '2022-04-08 14:03:49.031875')
on conflict do nothing;

create table if not exists public_test.ticker_risk_scores
(
    symbol     varchar
        constraint ticker_risk_scores_unique_symbol
            unique,
    risk_score double precision,
    updated_at timestamp
);
INSERT INTO public_test.ticker_risk_scores (symbol, risk_score, updated_at)
VALUES ('AAPL', 0.4752466534540463, '2022-04-11 11:22:20.084558')
on conflict do nothing;

create table if not exists public_test.collection_ticker_actual_weights
(
    date               date,
    profile_id         integer,
    collection_id      integer,
    collection_uniq_id text,
    symbol             text,
    weight             double precision
);
