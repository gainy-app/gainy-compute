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

insert into app.drivewealth_instruments (ref_id, symbol, status, data, created_at, updated_at)
values  ('82980b2b-48b9-4f17-94ab-385726f4f772', 'ADBE', 'ACTIVE', '{"symbol": "ADBE", "name": "Adobe Systems Inc.", "id": "82980b2b-48b9-4f17-94ab-385726f4f772", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US00724F1012"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('0bfd8059-1491-4b1b-8456-b394992409df', 'AMD', 'ACTIVE', '{"symbol": "AMD", "name": "Advanced Micro Devices, Inc.", "id": "0bfd8059-1491-4b1b-8456-b394992409df", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US0079031078"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('90b7618e-be49-483b-99f4-fe4720be2706', 'BABA', 'ACTIVE', '{"symbol": "BABA", "name": "Alibaba Group", "id": "90b7618e-be49-483b-99f4-fe4720be2706", "type": "ADR", "status": "ACTIVE", "ISIN": "US01609W1027"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('68277cc0-6106-4f3c-849b-1298ba964aba', 'GOOG', 'ACTIVE', '{"symbol": "GOOG", "name": "Alphabet Inc. - Class C Shares", "id": "68277cc0-6106-4f3c-849b-1298ba964aba", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US02079K1079"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('06926627-e950-48f3-9c53-b679f61120ec', 'AMZN', 'ACTIVE', '{"symbol": "AMZN", "name": "Amazon.com Inc.", "id": "06926627-e950-48f3-9c53-b679f61120ec", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US0231351067"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('2271acec-5f58-46ee-891b-ea42730b4de1', 'AMT', 'ACTIVE', '{"symbol": "AMT", "name": "American Tower Corporation", "id": "2271acec-5f58-46ee-891b-ea42730b4de1", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US03027X1000"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('ea0ca2a7-82e0-45b5-98b1-21cffaae127c', 'AON', 'ACTIVE', '{"symbol": "AON", "name": "Aon plc", "id": "ea0ca2a7-82e0-45b5-98b1-21cffaae127c", "type": "EQUITY", "status": "ACTIVE", "ISIN": "IE00BLP1HW54"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a67422af-8504-43df-9e63-7361eb0bd99e', 'AAPL', 'ACTIVE', '{"symbol": "AAPL", "name": "Apple", "id": "a67422af-8504-43df-9e63-7361eb0bd99e", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US0378331005"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('62ee1e09-8833-4485-8cf8-c541076d9610', 'ASML', 'ACTIVE', '{"symbol": "ASML", "name": "ASML Holding NV", "id": "62ee1e09-8833-4485-8cf8-c541076d9610", "type": "EQUITY", "status": "ACTIVE", "ISIN": "USN070592100"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('b93c5cc7-b0ee-4cf4-9963-8515830ca96d', 'BAC', 'ACTIVE', '{"symbol": "BAC", "name": "Bank of America Corporation", "id": "b93c5cc7-b0ee-4cf4-9963-8515830ca96d", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US0605051046"}', '2022-10-31 13:44:08.264891 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('fde3672f-45e7-4180-ae58-9fc11fb0da32', 'AVGO', 'ACTIVE', '{"symbol": "AVGO", "name": "Broadcom", "id": "fde3672f-45e7-4180-ae58-9fc11fb0da32", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US11135F1012"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('21685ed0-30be-4664-9515-1b78983f3a35', 'CVX', 'ACTIVE', '{"symbol": "CVX", "name": "Chevron Corporation", "id": "21685ed0-30be-4664-9515-1b78983f3a35", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US1667641005"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('ee090de4-4b66-483f-8db6-69a0e8749895', 'CB', 'ACTIVE', '{"symbol": "CB", "name": "Chubb Corporation, The", "id": "ee090de4-4b66-483f-8db6-69a0e8749895", "type": "EQUITY", "status": "ACTIVE", "ISIN": "CH0044328745"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('b27eefa0-300e-47ff-8c48-37a9230a1618', 'CSCO', 'ACTIVE', '{"symbol": "CSCO", "name": "Cisco Systems, Inc.", "id": "b27eefa0-300e-47ff-8c48-37a9230a1618", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US17275R1023"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('015eea6e-785a-462b-a0d6-144242cd83c7', 'COP', 'ACTIVE', '{"symbol": "COP", "name": "ConocoPhillips", "id": "015eea6e-785a-462b-a0d6-144242cd83c7", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US20825C1045"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('27d1f95e-38a3-49b0-8c54-78b5ac002792', 'COST', 'ACTIVE', '{"symbol": "COST", "name": "Costco Wholesale", "id": "27d1f95e-38a3-49b0-8c54-78b5ac002792", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US22160K1051"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('de79f84e-7d3b-4b83-9985-5bb727c20daf', 'CCI', 'ACTIVE', '{"symbol": "CCI", "name": "Crown Castle International Corp.", "id": "de79f84e-7d3b-4b83-9985-5bb727c20daf", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US22822V1017"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('cb5d3db7-411b-44aa-b6fb-5c37a3bf40fd', 'D', 'ACTIVE', '{"symbol": "D", "name": "Dominion Resources, Inc.", "id": "cb5d3db7-411b-44aa-b6fb-5c37a3bf40fd", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US25746U1097"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('ee9b12e0-a8cb-4623-8b7c-4ba04bb081ed', 'DUK', 'ACTIVE', '{"symbol": "DUK", "name": "Duke Energy Corporation", "id": "ee9b12e0-a8cb-4623-8b7c-4ba04bb081ed", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US26441C2044"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a846abf4-3671-41ca-9570-fb71383a601c', 'EQIX', 'ACTIVE', '{"symbol": "EQIX", "name": "Equinix, Inc.", "id": "a846abf4-3671-41ca-9570-fb71383a601c", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US29444U7000"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('6830ca80-81e8-11e2-9e96-0800200c9a66', 'XOM', 'ACTIVE', '{"symbol": "XOM", "name": "Exxon Mobil Corp.", "id": "6830ca80-81e8-11e2-9e96-0800200c9a66", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US30231G1022"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('fbcfc675-8082-45ea-b675-430beffb875b', 'HDB', 'ACTIVE', '{"symbol": "HDB", "name": "HDFC Bank Ltd.", "id": "fbcfc675-8082-45ea-b675-430beffb875b", "type": "ADR", "status": "ACTIVE", "ISIN": "US40415F1012"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('5476820a-649b-4203-9c27-087fb00abab8', 'INTC', 'ACTIVE', '{"symbol": "INTC", "name": "Intel Corporation", "id": "5476820a-649b-4203-9c27-087fb00abab8", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US4581401001"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('369fd0dc-5fc6-4531-acdf-7ded3dcf068f', 'IBM', 'ACTIVE', '{"symbol": "IBM", "name": "International Business Machines Corp.", "id": "369fd0dc-5fc6-4531-acdf-7ded3dcf068f", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US4592001014"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a18de4c7-f21f-4c88-b78d-289660773208', 'JPM', 'ACTIVE', '{"symbol": "JPM", "name": "JPMorgan Chase & Co.", "id": "a18de4c7-f21f-4c88-b78d-289660773208", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US46625H1005"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('8c1544fc-1e52-475c-8a1d-18a9aa45350d', 'KR', 'ACTIVE', '{"symbol": "KR", "name": "Kroger Co., The", "id": "8c1544fc-1e52-475c-8a1d-18a9aa45350d", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US5010441013"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('961d9412-ac41-41ee-adad-797703a73aeb', 'MMC', 'ACTIVE', '{"symbol": "MMC", "name": "Marsh & McLennan Companies, Inc.", "id": "961d9412-ac41-41ee-adad-797703a73aeb", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US5717481023"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('e234cc98-cd08-4b04-a388-fe5c822beea6', 'MSFT', 'ACTIVE', '{"symbol": "MSFT", "name": "Microsoft Corporation", "id": "e234cc98-cd08-4b04-a388-fe5c822beea6", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US5949181045"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('c4333b3f-6fde-4698-8aff-779f18fd0a15', 'NEE', 'ACTIVE', '{"symbol": "NEE", "name": "NextEra Energy, Inc.", "id": "c4333b3f-6fde-4698-8aff-779f18fd0a15", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US65339F1012"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('ca33741d-19e5-41fa-bf30-d303ec9c71be', 'NVDA', 'ACTIVE', '{"symbol": "NVDA", "name": "NVIDIA Corporation", "id": "ca33741d-19e5-41fa-bf30-d303ec9c71be", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US67066G1040"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a3b0fed6-6d26-4fe9-8aa0-db75d510275d', 'ORCL', 'ACTIVE', '{"symbol": "ORCL", "name": "Oracle Corp.", "id": "a3b0fed6-6d26-4fe9-8aa0-db75d510275d", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US68389X1054"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a17565d7-bfdf-4be0-bc2e-d05054bbca16', 'PGR', 'ACTIVE', '{"symbol": "PGR", "name": "Progressive Corp.", "id": "a17565d7-bfdf-4be0-bc2e-d05054bbca16", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US7433151039"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('8a266930-4845-490c-a619-abd95fcf320a', 'PLD', 'ACTIVE', '{"symbol": "PLD", "name": "Prologis, Inc.", "id": "8a266930-4845-490c-a619-abd95fcf320a", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US74340W1036"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('514aa491-c48d-4400-b379-feed8c7b2838', 'PSA', 'ACTIVE', '{"symbol": "PSA", "name": "Public Storage", "id": "514aa491-c48d-4400-b379-feed8c7b2838", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US74460D1090"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('61942b6d-9ef6-4f1e-a8bc-2f93c2dd144e', 'QCOM', 'ACTIVE', '{"symbol": "QCOM", "name": "QUALCOMM Incorporated", "id": "61942b6d-9ef6-4f1e-a8bc-2f93c2dd144e", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US7475251036"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('f161d8ea-72d0-46d1-a656-4274409b63e9', 'RY', 'ACTIVE', '{"symbol": "RY", "name": "Royal Bank of Canada", "id": "f161d8ea-72d0-46d1-a656-4274409b63e9", "type": "EQUITY", "status": "ACTIVE", "ISIN": "CA7800871021"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('459ce368-8f88-4fe1-8edb-d71c78af51f1', 'CRM', 'ACTIVE', '{"symbol": "CRM", "name": "Salesforce.com, Inc", "id": "459ce368-8f88-4fe1-8edb-d71c78af51f1", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US79466L3024"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('178b297b-0e7b-4978-adcb-6f070492cc02', 'SRE', 'ACTIVE', '{"symbol": "SRE", "name": "Sempra Energy", "id": "178b297b-0e7b-4978-adcb-6f070492cc02", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US8168511090"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a491928d-1caf-478a-b70f-2d095a7e7ffd', 'SHEL', 'ACTIVE', '{"symbol": "SHEL", "name": "SHELL PLC  - ADS", "id": "a491928d-1caf-478a-b70f-2d095a7e7ffd", "type": "ADR", "status": "ACTIVE", "ISIN": "US7802593050"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('a5d87e61-21da-438f-bf53-efbecec3d093', 'SO', 'ACTIVE', '{"symbol": "SO", "name": "Southern Company", "id": "a5d87e61-21da-438f-bf53-efbecec3d093", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US8425871071"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('9c4343fa-2d17-446b-8ad1-4b67ad66f88f', 'SYY', 'ACTIVE', '{"symbol": "SYY", "name": "Sysco Corporation", "id": "9c4343fa-2d17-446b-8ad1-4b67ad66f88f", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US8718291078"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('2ec4b942-e26e-4b91-81c1-f2c2d4d66373', 'TSM', 'ACTIVE', '{"symbol": "TSM", "name": "Taiwan Semiconductor Manufacturing Company Limited", "id": "2ec4b942-e26e-4b91-81c1-f2c2d4d66373", "type": "ADR", "status": "ACTIVE", "ISIN": "US8740391003"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('d34fc7aa-2e6b-4806-a6b7-b66ca94aaed8', 'TXN', 'ACTIVE', '{"symbol": "TXN", "name": "Texas Instruments Inc.", "id": "d34fc7aa-2e6b-4806-a6b7-b66ca94aaed8", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US8825081040"}', '2022-10-22 07:37:38.711144 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('11a2bf11-4b53-421f-8f2c-2891c08e1141', 'TTE', 'ACTIVE', '{"symbol": "TTE", "name": "TotalEnergies SE", "id": "11a2bf11-4b53-421f-8f2c-2891c08e1141", "type": "ADR", "status": "ACTIVE", "ISIN": "US89151E1091"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('067e00c6-b55a-4576-b6cd-e699add6ead5', 'WMT', 'ACTIVE', '{"symbol": "WMT", "name": "Wal-Mart Stores Inc.", "id": "067e00c6-b55a-4576-b6cd-e699add6ead5", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US9311421039"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('5e94ead5-ece5-4afd-aade-0145ec03dd60', 'WBA', 'ACTIVE', '{"symbol": "WBA", "name": "Walgreens Boots Alliance, Inc.", "id": "5e94ead5-ece5-4afd-aade-0145ec03dd60", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US9314271084"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00'),
        ('2f4db6d9-7e87-4dca-8be3-3139cc3553f3', 'WFC', 'ACTIVE', '{"symbol": "WFC", "name": "Wells Fargo & Co.", "id": "2f4db6d9-7e87-4dca-8be3-3139cc3553f3", "type": "EQUITY", "status": "ACTIVE", "ISIN": "US9497461015"}', '2022-11-10 07:39:41.473869 +00:00', '2022-11-10 07:39:41.473869 +00:00')
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

CREATE TABLE "app"."analytics_profile_data"
(
    "profile_id"   integer     NOT NULL,
    "service_name" text        NOT NULL,
    "metadata"     jsonb       NOT NULL,
    "created_at"   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY ("profile_id", "service_name"),
    FOREIGN KEY ("profile_id") REFERENCES "app"."profiles" ("id") ON UPDATE cascade ON DELETE cascade,
    CONSTRAINT "service_name" CHECK (service_name in ('APPSFLYER', 'FIREBASE')),
    CONSTRAINT "metadata_appsflyer" CHECK (not (service_name = 'APPSFLYER' and (metadata -> 'appsflyer_id') is null)),
    CONSTRAINT "metadata_firebase" CHECK (not (service_name = 'FIREBASE' and (metadata -> 'app_instance_id') is null))
);
insert into app.analytics_profile_data(profile_id, service_name, metadata)
VALUES (1, 'FIREBASE', '{"app_instance_id": "cqtJrlJKYkRerQW1G5960_"}'::jsonb),
       (1, 'APPSFLYER', '{"appsflyer_id": "1677666510758-9043781"}'::jsonb)
on conflict do nothing;
