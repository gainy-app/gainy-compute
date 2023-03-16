from _decimal import Decimal

from gainy.billing.models import Invoice, PaymentMethod, PaymentTransaction, PaymentMethodProvider
from gainy.billing.provider import AbstractPaymentProvider
from gainy.trading.drivewealth import DriveWealthApi, DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.config import DRIVEWEALTH_HOUSE_ACCOUNT_NO
from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.trading.exceptions import InsufficientFundsException


class DriveWealthPaymentProvider(AbstractPaymentProvider):

    @property
    def provider_id(self):
        return PaymentMethodProvider.DRIVEWEALTH

    def __init__(self, provider: DriveWealthProvider,
                 repo: DriveWealthRepository, api: DriveWealthApi):
        self.provider = provider
        self.repo = repo
        self.api = api

    def supports(self, payment_method: PaymentMethod) -> bool:
        return payment_method.provider == self.provider_id

    def charge(self, invoice: Invoice,
               payment_method: PaymentMethod) -> PaymentTransaction:
        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"payment_method_id": payment_method.id})
        if not account:
            raise Exception('DriveWealthAccount not found')

        account_money = self.provider.sync_account_money(account.ref_id)
        if Decimal(account_money.cash_balance) < invoice.amount:
            raise InsufficientFundsException()

        transaction = self._create_transaction(invoice, payment_method)
        self.repo.persist(transaction)

        redemption = self.api.create_redemption(
            invoice.amount,
            account,
            _type="CASH_TRANSFER",
            transaction_code="FEE_AUM",
            partner_account_no=DRIVEWEALTH_HOUSE_ACCOUNT_NO,
            note=f"AUM FEE {invoice.description} #{invoice.id}")
        redemption.payment_transaction_id = transaction.id
        self.repo.persist(redemption)

        return transaction
