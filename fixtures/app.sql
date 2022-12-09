create schema if not exists app;

CREATE OR REPLACE FUNCTION "app"."set_current_timestamp_updated_at"()
    RETURNS TRIGGER AS
$$
DECLARE
    _new record;
BEGIN
    _new := NEW;
    _new."updated_at" = NOW();
    RETURN _new;
END;
$$ LANGUAGE plpgsql;

create table if not exists app.profile_categories
(
    profile_id  integer not null,
    category_id integer not null,
    constraint profile_categories_pk
        primary key (profile_id, category_id)
);

create table if not exists app.profile_interests
(
    profile_id  integer not null,
    interest_id integer not null,
    constraint profile_interests_pk
        primary key (profile_id, interest_id)
);

create table if not exists app.profile_scoring_settings
(
    profile_id                    integer not null
        constraint profile_scoring_settings_pk
            primary key,
    created_at                    timestamp with time zone,
    risk_level                    real,
    average_market_return         integer,
    investment_horizon            real,
    unexpected_purchases_source   varchar,
    damage_of_failure             real,
    stock_market_risk_level       varchar,
    trading_experience            varchar,
    if_market_drops_20_i_will_buy real,
    if_market_drops_40_i_will_buy real,
    risk_score                    integer
);

create table if not exists app.profiles
(
    id            serial
        constraint profiles_pk
            primary key,
    email         varchar,
    first_name    varchar,
    last_name     varchar,
    gender        integer,
    created_at    timestamp with time zone,
    user_id       text,
    avatar_url    varchar,
    legal_address varchar
);
INSERT INTO app.profiles (id, email, first_name, last_name, gender, user_id, avatar_url, legal_address)
VALUES (1, 'test3@example.com', 'fn', 'ln', 0, 'AO0OQyz0jyL5lNUpvKbpVdAPvlI3', '', 'legal_address')
on conflict do nothing;
ALTER SEQUENCE app.profiles_id_seq RESTART WITH 2;

CREATE TABLE IF NOT EXISTS "app"."personalized_ticker_collections"
(
    "profile_id"    integer NOT NULL,
    "collection_id" integer NOT NULL,
    "symbol"        varchar NOT NULL,
    PRIMARY KEY ("profile_id", "collection_id", "symbol"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);

CREATE TABLE IF NOT EXISTS "app"."personalized_collection_sizes"
(
    "profile_id"    integer NOT NULL,
    "collection_id" integer NOT NULL,
    "size"          integer NOT NULL,
    PRIMARY KEY ("profile_id", "collection_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);

create table if not exists app.profile_recommendations_metadata
(
    profile_id              integer not null
        constraint profile_recommendations_metadata_pk
            primary key,
    recommendations_version integer,
    updated_at              timestamp with time zone
);

create table if not exists app.profile_ticker_match_score
(
    profile_id          integer not null,
    symbol              text    not null,
    match_score         integer,
    fits_risk           integer,
    risk_similarity     double precision,
    fits_categories     integer,
    fits_interests      integer,
    category_matches    text,
    interest_matches    text,
    updated_at          timestamp with time zone,
    category_similarity double precision,
    interest_similarity double precision,
    matches_portfolio   boolean,
    constraint profile_ticker_match_score_pk
        primary key (profile_id, symbol)
);
CREATE TABLE "app"."profile_collection_match_score"
(
    "profile_id"          integer   NOT NULL,
    "collection_id"       integer   NOT NULL,
    "collection_uniq_id"  text      NOT NULL,
    "match_score"         float8    NOT NULL,
    "risk_similarity"     float8    NOT NULL,
    "category_similarity" float8    NOT NULL,
    "interest_similarity" float8    NOT NULL,
    "updated_at"          timestamp NOT NULL,
    "risk_level"          integer   NOT NULL,
    "category_level"      integer   NOT NULL,
    "interest_level"      integer   NOT NULL,
    PRIMARY KEY ("profile_id", "collection_uniq_id")
);

create table if not exists app.portfolio_securities
(
    id                serial
        primary key,
    close_price       double precision,
    close_price_as_of timestamp,
    iso_currency_code varchar,
    name              varchar                                not null,
    ref_id            varchar                                not null
        unique,
    ticker_symbol     varchar,
    type              varchar,
    created_at        timestamp with time zone default now() not null,
    updated_at        timestamp with time zone default now() not null
);
INSERT INTO app.portfolio_securities (id, close_price, close_price_as_of, iso_currency_code, name, ref_id,
                                      ticker_symbol, type, created_at, updated_at)
VALUES (49, 0.6928, '2022-04-07 00:00:00.000000', 'USD', 'Remark Media, Inc.', 'ODbxOoxka6fPx3Xy4xkEIMDPJAPLpOfMPXEry',
        'MARK', 'equity', '2021-11-19 21:22:29.251714 +00:00', '2022-04-08 07:02:36.062043 +00:00');

create table if not exists app.profile_holdings
(
    id                    serial,
    iso_currency_code     varchar,
    quantity              double precision,
    security_id           integer,
    profile_id            integer,
    account_id            integer,
    ref_id                varchar,
    created_at            timestamp with time zone,
    updated_at            timestamp with time zone,
    plaid_access_token_id integer
);

create table app.drivewealth_auth_tokens
(
    id         serial
        primary key,
    auth_token varchar,
    expires_at timestamp with time zone,
    version    integer                                not null,
    data       json,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null
);
create trigger set_app_drivewealth_auth_tokens_updated_at
    before update
    on app.drivewealth_auth_tokens
    for each row
execute procedure app.set_current_timestamp_updated_at();

CREATE TABLE "app"."invoices"
(
    "id"           serial                  NOT NULL,
    "profile_id"   int,
    "period_id"    varchar,
    "status"       varchar   default 'PENDING',
    "amount"       numeric,
    "due_date"     date,
    "description"  text,
    "period_start" timestamp,
    "period_end"   timestamp,
    "metadata"     json,
    "version"      int,
    "created_at"   timestamp default now() not null,
    PRIMARY KEY ("id"),
    UNIQUE ("profile_id", "period_id")
);

create table app.payment_methods
(
    id            serial
        primary key,
    profile_id    integer                                not null
        references app.profiles
            on update cascade on delete cascade,
    name          varchar                                not null,
    set_active_at timestamp,
    created_at    timestamp with time zone default now() not null,
    updated_at    timestamp with time zone default now() not null,
    provider      varchar                                not null
);

CREATE TABLE if not exists "app"."trading_collection_versions"
(
    "id"                  serial      NOT NULL,
    "profile_id"          int         not null,
    "collection_id"       int         not null,
    "status"              varchar,
    "target_amount_delta" numeric     not null,
    "weights"             json,
    "trading_account_id"  int         not null,
    "created_at"          timestamptz NOT NULL DEFAULT now(),
    "updated_at"          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);

CREATE TABLE "app"."drivewealth_users"
(
    "ref_id"     varchar NOT NULL,
    "profile_id" integer          unique,
    "status"     varchar NOT NULL,
    "data"       json    NOT NULL,
    "created_at"  timestamptz NOT NULL DEFAULT now(),
    "updated_at"  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE restrict ON DELETE restrict
);

CREATE TABLE "app"."trading_accounts"
(
    "id"                            serial      NOT NULL,
    "profile_id"                    integer     NOT NULL unique,
    "name"                          varchar,
    "cash_available_for_trade"      integer,
    "cash_available_for_withdrawal" integer,
    "cash_balance"                  integer,
    "account_no"                    varchar,
    "created_at"                    timestamptz NOT NULL DEFAULT now(),
    "updated_at"                    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade
);

CREATE TABLE "app"."drivewealth_accounts"
(

    "ref_id"                        varchar     NOT NULL,
    "drivewealth_user_id"           varchar     NOT NULL,
    "trading_account_id"            integer,
    "status"                        varchar,
    "ref_no"                        varchar,
    "nickname"                      varchar,
    "cash_available_for_trade"      integer,
    "cash_available_for_withdrawal" integer,
    "cash_balance"                  integer,
    "data"                          json        NOT NULL,
    "created_at"                    timestamptz NOT NULL DEFAULT now(),
    "updated_at"                    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("drivewealth_user_id") REFERENCES "app"."drivewealth_users" ("ref_id") ON UPDATE cascade ON DELETE cascade,
    FOREIGN KEY ("trading_account_id") REFERENCES "app"."trading_accounts" ("id") ON UPDATE set null ON DELETE set null
);

CREATE TABLE if not exists "app"."drivewealth_portfolios"
(
    "ref_id"                 varchar     NOT NULL,
    "profile_id"             int         NOT NULL,
    "drivewealth_account_id" varchar     NOT NULL,
    "holdings"               json,
    "data"                   json,
    "cash_target_weight"     numeric,
    "created_at"             timestamptz NOT NULL DEFAULT now(),
    "updated_at"             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("ref_id"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE restrict ON DELETE restrict,
    FOREIGN KEY ("drivewealth_account_id") REFERENCES "app"."drivewealth_accounts" ("ref_id") ON UPDATE cascade ON DELETE cascade
);

create table app.drivewealth_instruments
(
    ref_id     varchar                                not null
        primary key,
    symbol     varchar                                not null,
    status     varchar                                not null,
    data       json,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null
);

insert into app.drivewealth_instruments (ref_id, symbol, status, data)
values  ('a67422af-8504-43df-9e63-7361eb0bd99e', 'AAPL', 'ACTIVE', '{"symbol": "AAPL", "reutersPrimaryRic": "AAPL.O", "name": "Apple", "description": "Apple Inc. (Apple) designs, manufactures and markets smartphones, personal computers, tablets, wearables and accessories and sells a range of related services. The Company\u2019s products include iPhone, Mac, iPad, AirPods, Apple TV, Apple Watch, Beats products, HomePod, iPod touch and accessories. The Company operates various platforms, including the App Store, which allows customers to discover and download applications and digital content, such as books, music, video, games and podcasts. Apple offers digital content through subscription-based services, including Apple Arcade, Apple Music, Apple News+, Apple TV+ and Apple Fitness+. Apple also offers a range of other services, such as AppleCare, iCloud, Apple Card and Apple Pay. Apple sells its products and resells third-party products in a range of markets, including directly to consumers, small and mid-sized businesses, and education, enterprise and government customers through its retail and online stores and its direct sales force.", "sector": "Technology", "longOnly": true, "orderSizeMax": 10000, "orderSizeMin": 1e-08, "orderSizeStep": 1e-08, "exchangeNickelSpread": false, "close": 0, "descriptionChinese": "Apple Inc\u8bbe\u8ba1\u3001\u5236\u9020\u548c\u9500\u552e\u667a\u80fd\u624b\u673a\u3001\u4e2a\u4eba\u7535\u8111\u3001\u5e73\u677f\u7535\u8111\u3001\u53ef\u7a7f\u6234\u8bbe\u5907\u548c\u914d\u4ef6\uff0c\u5e76\u63d0\u4f9b\u5404\u79cd\u76f8\u5173\u670d\u52a1\u3002\u8be5\u516c\u53f8\u7684\u4ea7\u54c1\u5305\u62eciPhone\u3001Mac\u3001iPad\u4ee5\u53ca\u53ef\u7a7f\u6234\u8bbe\u5907\u3001\u5bb6\u5c45\u548c\u914d\u4ef6\u3002iPhone\u662f\u8be5\u516c\u53f8\u57fa\u4e8eiOS\u64cd\u4f5c\u7cfb\u7edf\u7684\u667a\u80fd\u624b\u673a\u7cfb\u5217\u3002Mac\u662f\u8be5\u516c\u53f8\u57fa\u4e8emacOS\u64cd\u4f5c\u7cfb\u7edf\u7684\u4e2a\u4eba\u7535\u8111\u7cfb\u5217\u3002iPad\u662f\u8be5\u516c\u53f8\u57fa\u4e8eiPadOS\u64cd\u4f5c\u7cfb\u7edf\u7684\u591a\u529f\u80fd\u5e73\u677f\u7535\u8111\u7cfb\u5217\u3002\u53ef\u7a7f\u6234\u8bbe\u5907\u3001\u5bb6\u5c45\u548c\u914d\u4ef6\u5305\u62ecAirPods\u3001Apple TV\u3001Apple Watch\u3001Beats\u4ea7\u54c1\u3001HomePod\u3001iPod touch\u548c\u5176\u4ed6Apple\u54c1\u724c\u53ca\u7b2c\u4e09\u65b9\u914d\u4ef6\u3002AirPods\u662f\u8be5\u516c\u53f8\u53ef\u4ee5\u4e0eSiri\u4ea4\u4e92\u7684\u65e0\u7ebf\u8033\u673a\u3002Apple Watch\u662f\u8be5\u516c\u53f8\u7684\u667a\u80fd\u624b\u8868\u7cfb\u5217\u3002\u5176\u670d\u52a1\u5305\u62ec\u5e7f\u544a\u3001AppleCare\u3001\u4e91\u670d\u52a1\u3001\u6570\u5b57\u5185\u5bb9\u548c\u652f\u4ed8\u670d\u52a1\u3002\u5176\u5ba2\u6237\u4e3b\u8981\u6765\u81ea\u6d88\u8d39\u8005\u3001\u4e2d\u5c0f\u4f01\u4e1a\u3001\u6559\u80b2\u3001\u4f01\u4e1a\u548c\u653f\u5e9c\u5e02\u573a\u3002 ", "id": "a67422af-8504-43df-9e63-7361eb0bd99e", "type": "EQUITY", "exchange": "NSQ", "url": "http://investor.apple.com", "status": "ACTIVE", "closePrior": 142.99, "image": "https://uat-drivewealth.imgix.net/symbols/aapl.png?fit=fillmax&w=125&h=125&bg=FFFFFF", "ISIN": "US0378331005"}')
on conflict do nothing;

create table app.influencers
(
    id         serial
        primary key,
    profile_id integer
                                                      references app.profiles
                                                          on update set null on delete set null,
    email      varchar,
    name       varchar                                not null,
    created_at timestamp with time zone default now() not null,
    updated_at timestamp with time zone default now() not null
);
