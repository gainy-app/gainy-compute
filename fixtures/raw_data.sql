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