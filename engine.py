import datetime
from datetime import date
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("yfinance not installed. 'yahoo' source will not work.")
    yf = None


class MarketDataQuery:
    def __init__(self,
                 symbol: str,  # symbol means permno
                 time_frame: str,
                 start_date: str,
                 end_date: str,
                 frequency: str = '1d', # '1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo'
                 source: str = 'yahoo'): # default source is yahoo, it will ask yfinance to provide the data
        self.symbol = symbol
        self.time_frame = time_frame
        self.start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')  # convert string to datetime, opposite to strftime
        self.end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        self.frequency = frequency
        self.source = source

        self._validate()

    # you have to validate your query condition each time
    def _validate(self):
        if self.start_date > self.end_date:
            raise ValueError('Start date must be before end date')
        valid_frequencies = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']

        if self.frequency not in valid_frequencies:
            raise ValueError('Frequency must be one of {}'.format(valid_frequencies))

    def fetch(self):
        if self.source == 'test':

            freq_map = {'1m': 'T', # these are pandas alias
                        '2m': '2T',
                        '5m': '5T',
                        '15m': '15T',
                        '30m': '30T',
                        '60m': '60T',
                        '90m': '90T',
                        '1h': 'H',
                        '1d': 'D',
                        '5d': '5D',
                        '1wk': 'W',
                        '1mo': 'MS',
                        '3mo': '3MS'} # 'D'
            pd_freq = freq_map.get(self.frequency)
            if not pd_freq:
                raise ValueError('Test source does not support frequency: {self.frequency}')

            dates = pd.date_range(start=self.start_date, end=self.end_date, freq=pd_freq) # get() looks up value of self.frequency in freq_map
            if len(dates) == 0 and self.start_date <= self.end_date: # Handle case where start/end are same day
                dates = pd.to_datetime([self.start_date]) # set the start_date as dates

            prices = [100 + i*0.5 for i in range(len(dates))]  # add 0.5 each day

            df = pd.DataFrame({'date': dates, 'price': prices})
            return df

        elif self.source == 'yahoo':
            if yf is None:
                raise ImportError("yfinance is not installed. Cannot use 'yahoo' source.")

            df = yf.download( # input your parameter
                self.symbol,
                start=self.start_date.strftime('%Y-%m-%d'), # convert the datetime object to string, then send it to API
                end=self.end_date.strftime('%Y-%m-%d'),
                interval=self.frequency,
            )
            df.reset_index(inplace=True)
            return df

        else:
            raise ValueError(f'Unknown source: {self.source}')


# PriceBar is a container after getting the data
class PriceBar:
    def __init__(self, date, open_price, close_price, high_price, low_price, volume):
        self.date = date
        self.open = open_price
        self.close = close_price
        self.high = high_price
        self.low = low_price
        self.volume = volume

    def mid_price(self):
        return (self.high + self.low) / 2

    def is_bullish(self):
        return self.open < self.close

    def is_bearish(self):
        return self.open > self.close

    def __repr__(self):
        return (f'PriceBar(date={self.date}, open={self.open}, close={self.close}, '
                f'high={self.high}, low={self.low}, volume={self.volume})')


class TradeOrder:
    def __init__(self, symbol, side, quantity, order_type='market', price=None):
        self.symbol = symbol.upper()
        self.side = side.upper()  # 'BUY' or 'SELL'
        self.quantity = quantity
        self.order_type = order_type.lower()
        self.price = price
        # These are now set internally, not passed as args
        self.timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.status = 'pending'

    def cancel(self):
        if self.status == 'pending':
            self.status = 'cancelled'

    def __repr__(self):
        return (f'TradeOrder(symbol={self.symbol}, side={self.side}, quantity={self.quantity}, '
                f'type={self.order_type}, price={self.price}, status={self.status})')


class MarketOrder(TradeOrder):
    def __init__(self, symbol, side, quantity):
        super().__init__(symbol, side, quantity, order_type='market')

    def execute(self, market_price):
        self.price = market_price
        self.status = 'filled'


class LimitOrder(TradeOrder):
    def __init__(self, symbol, side, quantity, limit_price):
        super().__init__(symbol, side, quantity, order_type='limit', price=limit_price)

    def execute(self, market_price):
        if self.side == 'BUY' and market_price <= self.price:
            self.status = 'filled'
        elif self.side == 'SELL' and market_price >= self.price:
            self.status = 'filled'


class OrderReceipt:
    def __init__(self, symbol, side, order, timestamp, executed_price=None, executed_quantity=0, status='pending'):
        self.order_id = id(order)
        self.symbol = symbol.upper()
        self.side = side.upper()
        self.original_quantity = order.quantity  # order here is an object from TradeOrder
        self.executed_quantity = executed_quantity
        self.executed_price = executed_price
        self.timestamp = timestamp  # Use passed timestamp
        self.status = status

    def __repr__(self):
        return (f'OrderReceipt(symbol={self.symbol.upper()}, side={self.side.upper()}, '
                f'executed_qty={self.executed_quantity}/{self.original_quantity}, '
                f'executed_price={self.executed_price}, status={self.status}, '
                f'timestamp={self.timestamp})')


class IConnector:  # defines a contract for how your system talks to any broker.
    def getMarketData(self, symbol, start_date, end_date):
        raise NotImplementedError('Subclasses must implement getMarketData')

    def submitOrder(self, order):
        raise NotImplementedError('Subclasses must implement submitOrder')

    def getAccountInfo(self):
        raise NotImplementedError('Subclasses must implement getAccountInfo')


class MockBrokerConnector(IConnector):  # provides a dummybroker
    def __init__(self):
        self.cash_balance = 100000.0
        self.positions = {}  # e.g., {'AAPL': 100}
        self.order_history = []
        self.current_market_price = 100.0  # Added for predictable testing

    def getMarketData(self, symbol, start_date, end_date):
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        prices = [100 + i * 0.5 for i in range(len(dates))]
        df = pd.DataFrame({'date': dates, 'symbol': symbol, 'price': prices})
        return df

    def submitOrder(self, order):
        # Use the broker's current market price
        order.execute(self.current_market_price)
        self.order_history.append(order)

        if order.status == 'filled':
            if order.side == 'BUY':
                cost = order.price * order.quantity
                self.cash_balance -= cost
                self.positions[order.symbol] = self.positions.get(order.symbol, 0) + order.quantity
            elif order.side == 'SELL':
                revenue = order.price * order.quantity
                self.cash_balance += revenue
                self.positions[order.symbol] = self.positions.get(order.symbol, 0) - order.quantity

        # Return a receipt
        receipt = OrderReceipt(
            symbol=order.symbol,
            side=order.side,
            order=order,
            timestamp=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            executed_price=order.price if order.status == 'filled' else None,
            executed_quantity=order.quantity if order.status == 'filled' else 0,
            status=order.status
        )
        return receipt

    def getAccountInfo(self):
        return {
            'cash_balance': self.cash_balance,
            'positions': self.positions,
            'order_history': self.order_history}
