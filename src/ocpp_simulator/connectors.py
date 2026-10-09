"""Per-connector task, stop signal and transaction ID bookkeeping."""
import asyncio

class Connectors:
    def __init__(self, count):
        if count < 1:
            raise ValueError("--connectors must be positive")
        self.count = count
        self.tasks = {}
        self.stops = {}
        self.transactions = {}

    def free(self, connector):
        return 1 <= connector <= self.count and connector not in self.tasks

    def reserve(self, connector):
        if not self.free(connector):
            return None
        stop = asyncio.Event()
        self.stops[connector] = stop
        return stop

    def active_transaction(self, connector, transaction_id):
        self.transactions[transaction_id] = connector

    def stop_transaction(self, transaction_id):
        connector = self.transactions.get(transaction_id)
        if connector is None:
            return False
        self.stops[connector].set()
        return True

    def release(self, connector):
        self.tasks.pop(connector, None)
        self.stops.pop(connector, None)
        for tx, owner in list(self.transactions.items()):
            if owner == connector:
                del self.transactions[tx]
