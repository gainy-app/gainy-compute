import logging
from decimal import Decimal
from itertools import groupby

import pytest

from gainy.data_access.operators import OperatorIn
from gainy.tests.mocks.repository_mocks import mock_record_calls, mock_persist, mock_find
from gainy.trading.drivewealth.provider.transaction_handler import DriveWealthTransactionHandler
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthPortfolioStatus, \
    DriveWealthRedemption, DriveWealthSpinOffTransaction, DriveWealthDividendTransaction, DriveWealthAccountPositions, \
    DriveWealthTransaction, DriveWealthCorporateActionTransactionLink, DriveWealthFund, \
    DriveWealthPortfolioStatusHolding
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.models import TradingAccount, CorporateActionAdjustment, TradingOrderSource

from gainy.trading.drivewealth.models import DriveWealthPortfolio
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService


def get_test_handle_new_transactions_handle_transactions_pc():
    return [False, True]


def get_test_handle_new_transactions_handle_redemptions_pc():
    return [False, True]


@pytest.mark.parametrize(
    "handle_transactions_pc",
    get_test_handle_new_transactions_handle_transactions_pc())
@pytest.mark.parametrize(
    "handle_redemptions_pc",
    get_test_handle_new_transactions_handle_redemptions_pc())
def test_handle_new_transactions(monkeypatch, handle_transactions_pc,
                                 handle_redemptions_pc):
    portfolio = DriveWealthPortfolio()
    portfolio_status = DriveWealthPortfolioStatus()
    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    handler = DriveWealthTransactionHandler(None, drivewealth_repository, None,
                                            None)
    handle_transactions_calls = []

    def mock_handle_transactions(*args):
        mock_record_calls(handle_transactions_calls)(*args)
        return handle_transactions_pc

    monkeypatch.setattr(handler, "_handle_transactions",
                        mock_handle_transactions)
    handle_redemptions_calls = []

    def mock_handle_redemptions(*args):
        mock_record_calls(handle_redemptions_calls)(*args)
        return handle_redemptions_pc

    monkeypatch.setattr(handler, "_handle_redemptions",
                        mock_handle_redemptions)

    portfolio_changed = handler.handle_new_transactions(
        portfolio, portfolio_status)

    assert portfolio_changed == (handle_transactions_pc
                                 or handle_redemptions_pc)
    assert handle_transactions_calls
    assert handle_redemptions_calls
    if portfolio_changed:
        assert DriveWealthPortfolio in persisted_objects
        assert portfolio in persisted_objects[DriveWealthPortfolio]
    else:
        assert DriveWealthPortfolio not in persisted_objects


def test_handle_transactions(monkeypatch):
    transaction1 = DriveWealthSpinOffTransaction()
    transaction2 = DriveWealthSpinOffTransaction()
    transaction3 = DriveWealthDividendTransaction()
    transaction4 = DriveWealthDividendTransaction()
    transactions = [
        transaction1,
        transaction2,
        transaction3,
        transaction4,
    ]

    portfolio = DriveWealthPortfolio()
    portfolio_status = DriveWealthPortfolioStatus()

    drivewealth_repository = DriveWealthRepository(None)

    def mock_get_new_transactions(*args):
        return transactions

    monkeypatch.setattr(drivewealth_repository, "get_new_transactions",
                        mock_get_new_transactions)

    handle_spinoff_transactions_calls = []
    handle_dividend_transactions_calls = []

    handler = DriveWealthTransactionHandler(None, drivewealth_repository, None,
                                            None)
    monkeypatch.setattr(handler, "_handle_spinoff_transactions",
                        mock_record_calls(handle_spinoff_transactions_calls))
    monkeypatch.setattr(handler, "_handle_dividend_transactions",
                        mock_record_calls(handle_dividend_transactions_calls))

    portfolio_changed = handler._handle_transactions(portfolio,
                                                     portfolio_status)

    assert portfolio_changed
    assert len(handle_spinoff_transactions_calls) == 1
    assert {transaction1,
            transaction2} == set(handle_spinoff_transactions_calls[0][0][0])
    assert len(handle_dividend_transactions_calls) == 1
    assert {transaction3,
            transaction4} == set(handle_dividend_transactions_calls[0][0][0])


def test_handle_spinoff_transactions(monkeypatch):
    position_delta1 = -1
    position_delta2 = 1
    position_delta3 = 5

    symbol1 = "symbol1"
    symbol2 = "symbol2"
    symbol3 = "symbol3"
    symbol4 = "symbol4"

    price = {
        symbol1: Decimal(1),
        symbol2: Decimal(2),
        symbol3: Decimal(3),
        symbol4: Decimal(4),
    }

    comment1 = f"from {symbol1} to {symbol2}"
    comment2 = f"from {symbol1} to {symbol2}"
    comment3 = f"from {symbol3} to {symbol4}"

    tx_data = {"finTranID": None, "finTranTypeID": None, "accountAmount": 0}
    transaction1 = DriveWealthSpinOffTransaction()
    transaction1.set_from_response({
        **tx_data, "positionDelta": position_delta1,
        "comment": comment1,
        "instrument": {
            "symbol": symbol1
        }
    })
    transaction2 = DriveWealthSpinOffTransaction()
    transaction2.set_from_response({
        **tx_data, "positionDelta": position_delta2,
        "comment": comment2,
        "instrument": {
            "symbol": symbol1
        }
    })
    transaction3 = DriveWealthSpinOffTransaction()
    transaction3.set_from_response({
        **tx_data, "positionDelta": position_delta3,
        "comment": comment3,
        "instrument": {
            "symbol": symbol4
        }
    })
    transactions = [
        transaction1,
        transaction2,
        transaction3,
    ]

    collection_id1 = 1
    collection_id2 = 2
    collection_none_weight = Decimal(0.5)
    collection_1_weight = Decimal(0.2)
    collection_2_weight = Decimal(0.3)
    trading_account_id = 3
    profile_id = 4

    symbol_weights = [
        (None, collection_none_weight),
        (collection_id1, collection_1_weight),
        (collection_id2, collection_2_weight),
    ]

    portfolio = DriveWealthPortfolio()
    portfolio.profile_id = profile_id
    portfolio_status = DriveWealthPortfolioStatus()
    account_positions = DriveWealthAccountPositions()

    trading_account = TradingAccount()
    trading_account.id = trading_account_id

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_account_positions(*args, **kwargs):
        return account_positions

    monkeypatch.setattr(provider, "sync_account_positions",
                        mock_sync_account_positions)
    monkeypatch.setattr(provider, "get_trading_account_by_portfolio",
                        lambda x: trading_account)

    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    handler = DriveWealthTransactionHandler(provider, drivewealth_repository,
                                            None, None)

    filter_linked_transactions_calls = []

    def mock_filter_linked_transactions(transactions):
        mock_record_calls(filter_linked_transactions_calls)(transactions)
        return transactions

    monkeypatch.setattr(handler, "_filter_linked_transactions",
                        mock_filter_linked_transactions)

    def mock_get_portfolio_symbol_weights(symbol, portfolio_status):
        return symbol_weights

    monkeypatch.setattr(handler, "_get_portfolio_symbol_weights",
                        mock_get_portfolio_symbol_weights)

    def mock_get_actual_price(symbol, account_positions):
        return price[symbol]

    monkeypatch.setattr(handler, "_get_actual_price", mock_get_actual_price)

    link_caa_calls = []
    monkeypatch.setattr(handler, "_link_caa",
                        mock_record_calls(link_caa_calls))

    handler._handle_spinoff_transactions(transactions, portfolio,
                                         portfolio_status)

    for i in persisted_objects[CorporateActionAdjustment]:
        logging.debug(i.to_dict())

    expected_number_of_caas = 6
    assert all(i.profile_id == profile_id
               for i in persisted_objects[CorporateActionAdjustment])
    assert all(i.trading_account_id == trading_account_id
               for i in persisted_objects[CorporateActionAdjustment])
    assert {symbol1, symbol3} == {
        i.symbol
        for i in persisted_objects[CorporateActionAdjustment]
    }
    assert len(persisted_objects[CorporateActionAdjustment]
               ) == expected_number_of_caas

    def key_func(caa: CorporateActionAdjustment):
        return caa.symbol

    for symbol, caas in groupby(
            sorted(persisted_objects[CorporateActionAdjustment], key=key_func),
            key_func):
        caas: list[CorporateActionAdjustment]

        if symbol == symbol1:
            assert all(abs(i.amount) < Decimal(1e-10) for i in caas)
        elif symbol == symbol3:
            expected_amount_sum = price[symbol4] * position_delta3
            assert abs(sum(i.amount for i in caas) -
                       expected_amount_sum) < Decimal(1e-10)
            for caa in caas:
                w = next(
                    filter(
                        lambda x: x[0] is None and caa.collection_id is None or
                        x[0] == caa.collection_id, symbol_weights))[1]
                assert abs(caa.amount -
                           expected_amount_sum * w) < Decimal(1e-10)

        else:
            raise Exception("Wrong symbol %s" % symbol)

    for i in link_caa_calls:
        logging.debug(i[0])
    assert len(link_caa_calls) == expected_number_of_caas
    assert set(i[0][0] for i in link_caa_calls) == set(
        persisted_objects[CorporateActionAdjustment])
    linked_transactions = set()
    for i in link_caa_calls:
        linked_transactions.update(i[0][1])
    assert linked_transactions == set(transactions)

    filtered_transactions = set()
    for i in filter_linked_transactions_calls:
        filtered_transactions.update(i[0][0])
    assert filtered_transactions == set(transactions)


def test_handle_dividend_transactions(monkeypatch):
    amount_delta1 = -1
    amount_delta2 = 10

    symbol1 = "symbol1"

    tx_data = {"finTranID": None, "finTranTypeID": None, "accountAmount": 0}
    transaction1 = DriveWealthDividendTransaction()
    transaction1.set_from_response({
        **tx_data, "accountAmount": amount_delta1,
        "instrument": {
            "symbol": symbol1
        }
    })
    transaction2 = DriveWealthDividendTransaction()
    transaction2.set_from_response({
        **tx_data, "accountAmount": amount_delta2,
        "instrument": {
            "symbol": symbol1
        }
    })
    transactions = [
        transaction1,
        transaction2,
    ]

    collection_id1 = 1
    collection_id2 = 2
    collection_none_weight = Decimal(0.5)
    collection_1_weight = Decimal(0.2)
    collection_2_weight = Decimal(0.3)
    trading_account_id = 3
    profile_id = 4

    symbol_weights = [
        (None, collection_none_weight),
        (collection_id1, collection_1_weight),
        (collection_id2, collection_2_weight),
    ]

    portfolio = DriveWealthPortfolio()
    portfolio.profile_id = profile_id
    portfolio_status = DriveWealthPortfolioStatus()
    account_positions = DriveWealthAccountPositions()

    trading_account = TradingAccount()
    trading_account.id = trading_account_id

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_account_positions(*args, **kwargs):
        return account_positions

    monkeypatch.setattr(provider, "sync_account_positions",
                        mock_sync_account_positions)
    monkeypatch.setattr(provider, "get_trading_account_by_portfolio",
                        lambda x: trading_account)

    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    caa_id = 0

    def _mock_persist(entity):
        nonlocal caa_id
        mock_persist(persisted_objects)(entity)
        if isinstance(entity, CorporateActionAdjustment):
            caa_id += 1
            entity.id = caa_id

    monkeypatch.setattr(drivewealth_repository, "persist", _mock_persist)

    handler = DriveWealthTransactionHandler(provider, drivewealth_repository,
                                            None, None)

    filter_linked_transactions_calls = []

    def mock_filter_linked_transactions(transactions):
        mock_record_calls(filter_linked_transactions_calls)(transactions)
        return transactions

    monkeypatch.setattr(handler, "_filter_linked_transactions",
                        mock_filter_linked_transactions)

    def mock_get_portfolio_symbol_weights(symbol, portfolio_status):
        return symbol_weights

    monkeypatch.setattr(handler, "_get_portfolio_symbol_weights",
                        mock_get_portfolio_symbol_weights)

    link_caa_calls = []
    monkeypatch.setattr(handler, "_link_caa",
                        mock_record_calls(link_caa_calls))

    create_order_calls = []
    monkeypatch.setattr(handler, "_create_order",
                        mock_record_calls(create_order_calls))

    handler._handle_dividend_transactions(transactions, portfolio,
                                          portfolio_status)

    for i in persisted_objects[CorporateActionAdjustment]:
        logging.debug(i.to_dict())

    expected_number_of_caas = 3
    assert all(i.profile_id == profile_id
               for i in persisted_objects[CorporateActionAdjustment])
    assert all(i.trading_account_id == trading_account_id
               for i in persisted_objects[CorporateActionAdjustment])
    assert {symbol1} == {
        i.symbol
        for i in persisted_objects[CorporateActionAdjustment]
    }
    assert len(persisted_objects[CorporateActionAdjustment]
               ) == expected_number_of_caas

    expected_amount_sum = amount_delta1 + amount_delta2

    def key_func(caa: CorporateActionAdjustment):
        return caa.symbol

    for symbol, caas in groupby(
            sorted(persisted_objects[CorporateActionAdjustment], key=key_func),
            key_func):
        caas: list[CorporateActionAdjustment]

        if symbol == symbol1:
            assert abs(sum(i.amount for i in caas) -
                       expected_amount_sum) < Decimal(1e-10)
            for caa in caas:
                w = next(
                    filter(
                        lambda x: x[0] is None and caa.collection_id is None or
                        x[0] == caa.collection_id, symbol_weights))[1]
                assert abs(caa.amount -
                           expected_amount_sum * w) < Decimal(1e-10)

        else:
            raise Exception("Wrong symbol %s" % symbol)

    for i in link_caa_calls:
        logging.debug(i[0])
    assert len(link_caa_calls) == expected_number_of_caas
    assert set(i[0][0] for i in link_caa_calls) == set(
        persisted_objects[CorporateActionAdjustment])
    linked_transactions = set()
    for i in link_caa_calls:
        linked_transactions.update(i[0][1])
    assert linked_transactions == set(transactions)

    for i in create_order_calls:
        logging.debug(i[0])
    assert len(create_order_calls) == expected_number_of_caas
    assert set(i[0][0] for i in create_order_calls) == set(
        persisted_objects[CorporateActionAdjustment])

    filtered_transactions = set()
    for i in filter_linked_transactions_calls:
        filtered_transactions.update(i[0][0])
    assert filtered_transactions == set(transactions)


def test_filter_linked_transactions(monkeypatch):
    transaction_id1 = 1
    transaction_id2 = 2

    transaction1 = DriveWealthTransaction()
    transaction1.id = transaction_id1
    transaction2 = DriveWealthTransaction()
    transaction2.id = transaction_id2
    transactions = [transaction1, transaction2]

    link = DriveWealthCorporateActionTransactionLink()
    link.drivewealth_transaction_id = transaction_id2
    found_transaction_links = [link]

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "find_all",
        mock_find([
            (DriveWealthCorporateActionTransactionLink, {
                "drivewealth_transaction_id":
                OperatorIn([transaction_id1, transaction_id2])
            }, found_transaction_links),
        ]))

    handler = DriveWealthTransactionHandler(None, None, trading_repository,
                                            None)

    assert handler._filter_linked_transactions(transactions) == [transaction1]


def test_get_portfolio_symbol_weights(monkeypatch):
    symbol1 = "symbol1"
    symbol2 = "symbol2"
    fund_id1 = "fund_id1"
    fund_id2 = "fund_id2"
    value1 = 1
    value2 = 2
    value3 = 3
    collection_id = 4

    fund1 = DriveWealthFund()
    fund1.collection_id = None
    fund2 = DriveWealthFund()
    fund2.collection_id = collection_id

    fund_holding_1 = DriveWealthPortfolioStatusHolding(
        {"holdings": [
            {
                "symbol": symbol1,
                "value": value1
            },
        ]})
    fund_holding_2 = DriveWealthPortfolioStatusHolding({
        "holdings": [
            {
                "symbol": symbol1,
                "value": value2
            },
            {
                "symbol": symbol2,
                "value": value3
            },
        ]
    })

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.holdings = {
        fund_id1: fund_holding_1,
        fund_id2: fund_holding_2,
    }

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([
            (DriveWealthFund, {
                "ref_id": fund_id1
            }, fund1),
            (DriveWealthFund, {
                "ref_id": fund_id2
            }, fund2),
        ]))

    handler = DriveWealthTransactionHandler(None, drivewealth_repository, None,
                                            None)

    weights = handler._get_portfolio_symbol_weights(symbol1, portfolio_status)
    assert len(weights) == 2
    for c_id, w in weights:
        if c_id is None:
            expected_weigth = Decimal(value1 / (value1 + value2))
        elif c_id == collection_id:
            expected_weigth = Decimal(value2 / (value1 + value2))
        else:
            raise Exception("Wrong collection " + c_id)

        assert abs(w - expected_weigth) < Decimal(1e-10)


def test_get_actual_price_from_account_positions(monkeypatch):
    symbol = "symbol"
    price = Decimal(1)

    def mock_get_symbol_market_price(s):
        assert s == symbol
        return price

    account_positions = DriveWealthAccountPositions()
    monkeypatch.setattr(account_positions, "get_symbol_market_price",
                        mock_get_symbol_market_price)

    handler = DriveWealthTransactionHandler(None, None, None, None)

    assert handler._get_actual_price(symbol, account_positions) == price


def test_get_actual_price_from_repository(monkeypatch):
    symbol = "symbol"
    price = Decimal(1)

    def mock_get_ticker_actual_price(s):
        assert s == symbol
        return price

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(trading_repository, "get_ticker_actual_price",
                        mock_get_ticker_actual_price)

    handler = DriveWealthTransactionHandler(None, None, trading_repository,
                                            None)

    assert handler._get_actual_price(symbol, None) == price


def test_link_caa(monkeypatch):
    caa_id = 1
    transaction_id1 = 2
    transaction_id2 = 3

    trading_repository = TradingRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(trading_repository, "persist",
                        mock_persist(persisted_objects))

    caa = CorporateActionAdjustment()
    caa.id = caa_id

    transaction1 = DriveWealthTransaction()
    transaction1.id = transaction_id1
    transaction2 = DriveWealthTransaction()
    transaction2.id = transaction_id2
    transactions = [transaction1, transaction2]

    handler = DriveWealthTransactionHandler(None, None, trading_repository,
                                            None)

    handler._link_caa(caa, transactions)

    assert len(
        persisted_objects[DriveWealthCorporateActionTransactionLink]) == 2
    assert all(
        i.corporate_action_adjustment_id == caa_id
        for i in persisted_objects[DriveWealthCorporateActionTransactionLink])
    assert set(
        i.drivewealth_transaction_id for i in
        persisted_objects[DriveWealthCorporateActionTransactionLink]) == {
            transaction_id1, transaction_id2
        }


def test_create_collection_order(monkeypatch):
    caa_id = 1
    collection_id = 2
    amount = Decimal(3)
    trading_account_id = 4
    profile_id = 5

    caa = CorporateActionAdjustment()
    caa.id = caa_id
    caa.amount = amount
    caa.collection_id = collection_id
    caa.trading_account_id = trading_account_id
    caa.profile_id = profile_id

    trading_service = TradingService(None, None, None)
    create_collection_version_calls = []
    monkeypatch.setattr(trading_service, "create_collection_version",
                        mock_record_calls(create_collection_version_calls))

    handler = DriveWealthTransactionHandler(None, None, None, trading_service)

    handler._create_order(caa)

    assert len(create_collection_version_calls) == 1
    assert ((profile_id, TradingOrderSource.AUTOMATIC, collection_id,
             trading_account_id), {
                 'note': 'caa #%d' % caa_id,
                 'target_amount_delta': amount
             }) in create_collection_version_calls


def test_create_orders(monkeypatch):
    caa_id = 1
    symbol = "symbol"
    amount = Decimal(3)
    trading_account_id = 4
    profile_id = 5

    caa = CorporateActionAdjustment()
    caa.id = caa_id
    caa.amount = amount
    caa.symbol = symbol
    caa.trading_account_id = trading_account_id
    caa.profile_id = profile_id

    trading_service = TradingService(None, None, None)
    create_stock_order_calls = []
    monkeypatch.setattr(trading_service, "create_stock_order",
                        mock_record_calls(create_stock_order_calls))

    handler = DriveWealthTransactionHandler(None, None, None, trading_service)

    handler._create_order(caa)

    assert len(create_stock_order_calls) == 1
    assert ((profile_id, TradingOrderSource.AUTOMATIC, symbol,
             trading_account_id), {
                 'note': 'caa #%d' % caa_id,
                 'target_amount_delta': amount
             }) in create_stock_order_calls


def get_test_handle_redemptions_pending_redemption():
    return [False, True]


def get_test_handle_redemptions_expected_result():
    return [False, True]


@pytest.mark.parametrize("pending_redemption",
                         get_test_handle_redemptions_pending_redemption())
@pytest.mark.parametrize(
    "expected_result",
    get_test_handle_new_transactions_handle_redemptions_pc())
def test_handle_redemptions(monkeypatch, expected_result, pending_redemption):
    redemption = DriveWealthRedemption()
    redemption.data = {"amount": 10}

    pending_redemptions_amount_sum = redemption.amount if pending_redemption else Decimal(
        0)

    portfolio = DriveWealthPortfolio()
    portfolio.pending_redemptions_amount_sum = Decimal(
        pending_redemptions_amount_sum)
    if expected_result:
        portfolio.pending_redemptions_amount_sum += Decimal(1)

    drivewealth_repository = DriveWealthRepository(None)

    def mock_get_pending_redemptions(*args):
        return [redemption] if pending_redemption else []

    monkeypatch.setattr(drivewealth_repository, "get_pending_redemptions",
                        mock_get_pending_redemptions)

    handler = DriveWealthTransactionHandler(None, drivewealth_repository, None,
                                            None)

    portfolio_changed = handler._handle_redemptions(portfolio)

    assert portfolio_changed == expected_result
    assert portfolio.pending_redemptions_amount_sum == pending_redemptions_amount_sum
