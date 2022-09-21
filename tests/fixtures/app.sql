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
