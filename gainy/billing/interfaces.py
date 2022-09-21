from abc import ABC, abstractmethod

from gainy.billing.models import Invoice


class BillingServiceInterface(ABC):

    @abstractmethod
    def charge(self, invoice: Invoice):
        pass
