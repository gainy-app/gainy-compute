create schema if not exists raw_data;

create table raw_data.ticker_collections
(
    date_upd          varchar,
    symbol            varchar not null,
    ttf_name          varchar not null,
    _sdc_extracted_at timestamp,
    primary key (ttf_name, symbol)
);

insert into raw_data.ticker_collections (ttf_name, date_upd, symbol, _sdc_extracted_at)
values  ('Big Tech', '2022-04-29', 'AAPL', now()),
        ('Big Tech', '2022-04-29', 'MSFT', now()),
        ('Big Tech', '2022-04-29', 'GOOG', now()),
        ('Big Tech', '2022-04-29', 'AMZN', now()),
        ('Big Tech', '2022-04-29', 'FB', now()),
        ('Big Tech', '2022-04-29', 'TSM', now()),
        ('Big Tech', '2022-04-29', 'NVDA', now()),
        ('Big Tech', '2022-04-29', 'BABA', now()),
        ('Big Tech', '2022-04-29', 'AVGO', now()),
        ('Big Tech', '2022-04-29', 'ASML', now()),
        ('Big Tech', '2022-04-29', 'CSCO', now()),
        ('Big Tech', '2022-04-29', 'ORCL', now()),
        ('Big Tech', '2022-04-29', 'INTC', now()),
        ('Big Tech', '2022-04-29', 'ADBE', now()),
        ('Big Tech', '2022-04-29', 'CRM', now()),
        ('Big Tech', '2022-04-29', 'TXN', now()),
        ('Big Tech', '2022-04-29', 'QCOM', now()),
        ('Big Tech', '2022-04-29', 'AMD', now()),
        ('Big Tech', '2022-04-29', 'IBM', now());

create table if not exists raw_data.stats_ttf_clicks
(
    _etl_tstamp       double precision,
    _sdc_batched_at   timestamp,
    _sdc_deleted_at   varchar,
    _sdc_extracted_at timestamp,
    clicks_count      numeric,
    collection_id     numeric,
    updated_at        timestamp
);
insert into raw_data.stats_ttf_clicks (_etl_tstamp, _sdc_batched_at, _sdc_deleted_at, _sdc_extracted_at, clicks_count, collection_id, updated_at)
values  (1670409203198, '2022-12-07 10:33:23.209486', null, null, 3, 210, '2022-12-07 10:32:10.785153'),
        (1670409203207, '2022-12-07 10:33:23.220095', null, null, 31, 83, '2022-12-07 10:32:10.785153'),
        (1670409203207, '2022-12-07 10:33:23.221481', null, null, 23, 107, '2022-12-07 10:32:10.785153'),
        (1670409203214, '2022-12-07 10:33:23.235271', null, null, 6, 59, '2022-12-07 10:32:10.785153'),
        (1670409203218, '2022-12-07 10:33:23.243325', null, null, 3, 276, '2022-12-07 10:32:10.785153'),
        (1670409203218, '2022-12-07 10:33:23.244626', null, null, 7, 207, '2022-12-07 10:32:10.785153'),
        (1670409203220, '2022-12-07 10:33:23.247931', null, null, 3, 272, '2022-12-07 10:32:10.785153'),
        (1670409203226, '2022-12-07 10:33:23.260385', null, null, 11, 116, '2022-12-07 10:32:10.785153'),
        (1670409203227, '2022-12-07 10:33:23.262171', null, null, 23, 277, '2022-12-07 10:32:10.785153'),
        (1670409203229, '2022-12-07 10:33:23.266516', null, null, 14, 245, '2022-12-07 10:32:10.785153'),
        (1670409203233, '2022-12-07 10:33:23.274858', null, null, 9, 45, '2022-12-07 10:32:10.785153'),
        (1670409203236, '2022-12-07 10:33:23.280481', null, null, 20, 17, '2022-12-07 10:32:10.785153'),
        (1670409203238, '2022-12-07 10:33:23.285823', null, null, 21, 154, '2022-12-07 10:32:10.785153');
