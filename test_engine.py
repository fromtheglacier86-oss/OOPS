import pytest
import pandas as pd
import datetime
from trading_system import (
    MarketDataQuery,
    PriceBar,
    TradeOrder,
    MarketOrder,
    LimitOrder,
    OrderReceipt,
    MockBrokerConnector
)


# --- Tests ---

def test_market_data_query_test_source_daily():
    """Tests the MarketDataQuery with the 'test' source."""
    query = MarketDataQuery(  # input the query condition, six parameters here totally
        symbol='TEST',
        time_frame='D1',
        start_date='2023-01-01',
        end_date='2023-01-05',
        frequency='1d',
        source='test'
    )
    df = query.fetch()

    # assert will raise an error if the statement return false
    assert isinstance(df, pd.DataFrame)
    assert 'date' in df.columns
    assert 'price' in df.columns
    assert len(df) == 5
    assert df['price'].iloc[0] == 100.0
    assert df['price'].iloc[-1] == 102.0  # 100 + 4 * 0.5


def test_market_data_query_test_source_weekly():
    """Tests the MarketDataQuery with the 'test' source for weekly freq."""
    query = MarketDataQuery(
        symbol='TEST',
        time_frame='W1',
        start_date='2023-01-01',
        end_date='2023-01-31',
        frequency='1wk',  # Test a different valid freq
        source='test'
    )
    df = query.fetch()

    # isinstance check if df is a DataFrame object from pd
    assert isinstance(df, pd.DataFrame)
    assert 'date' in df.columns
    assert len(df) == 5  # 5 weeks in Jan 2023
    assert df['price'].iloc[0] == 100.0
    assert df['price'].iloc[-1] == 102.0  # 100 + 4 * 0.5


def test_market_data_query_validation():
    """Tests the validation logic in MarketDataQuery."""
    # Test invalid date range
    with pytest.raises(ValueError, match='Start date must be before end date'):
        MarketDataQuery('TEST', 'D1', '2023-01-05', '2023-01-01')

    # Test invalid frequency
    with pytest.raises(ValueError, match="Frequency must be one of .* '3mo'"):
        MarketDataQuery('TEST', 'D1', '2023-01-01', '2023-01-05', frequency='yearly')

    # Test invalid source
    with pytest.raises(ValueError, match='Unknown source: bloomberg'):
        query = MarketDataQuery('TEST', 'D1', '2023-01-01', '2023-01-05', source='bloomberg')
        query.fetch()


def test_price_bar():
    """Tests the PriceBar class methods."""
    # Bullish bar
    bull_bar = PriceBar('2023-01-01', 100, 110, 115, 95, 1000)
    assert bull_bar.is_bullish()
    assert not bull_bar.is_bearish()
    assert bull_bar.mid_price() == (115 + 95) / 2

    # Bearish bar
    bear_bar = PriceBar('2023-01-02', 110, 100, 115, 95, 1000)
    assert not bear_bar.is_bullish()
    assert bear_bar.is_bearish()

    # Doji (neutral)
    doji_bar = PriceBar('2023-01-03', 100, 100, 105, 95, 1000)
    assert not doji_bar.is_bullish()
    assert not doji_bar.is_bearish()


def test_trade_order():
    """Tests the base TradeOrder class."""
    order = TradeOrder('AAPL', 'buy', 100)

    # Test init
    assert order.symbol == 'AAPL'
    assert order.side == 'BUY'
    assert order.status == 'pending'
    assert order.order_type == 'market'
    assert isinstance(order.timestamp, str)

    # Test cancel pending
    order.cancel()
    assert order.status == 'cancelled'

    # Test cancel filled (should not change)
    order.status = 'filled'
    order.cancel()
    assert order.status == 'filled'

    # Test repr
    assert 'TradeOrder' in repr(order)
    assert 'AAPL' in repr(order)


def test_market_order():
    """Tests the MarketOrder class."""
    order = MarketOrder('MSFT', 'sell', 50)

    # Test init
    assert order.symbol == 'MSFT'
    assert order.side == 'SELL'
    assert order.quantity == 50
    assert order.order_type == 'market'
    assert order.status == 'pending'

    # Test execute
    order.execute(market_price=123.45)
    assert order.status == 'filled'
    assert order.price == 123.45


def test_limit_order():
    """Tests the LimitOrder class execution logic."""
    # Test BUY limit
    buy_order = LimitOrder('GOOG', 'buy', 10, 150.00)

    # Case 1: Market price is
    buy_order.execute(market_price=151.00)
    assert buy_order.status == 'pending'

    # Case 2: Market price is at limit
    buy_order.execute(market_price=150.00)
    assert buy_order.status == 'filled'

    # Case 3: Market price is below limit
    buy_order.status = 'pending'  # Reset
    buy_order.execute(market_price=149.00)
    assert buy_order.status == 'filled'

    # Test SELL limit
    sell_order = LimitOrder('TSLA', 'sell', 5, 200.00)

    # Case 1: Market price is below limit
    sell_order.execute(market_price=199.00)
    assert sell_order.status == 'pending'

    # Case 2: Market price is at limit
    sell_order.execute(market_price=200.00)
    assert sell_order.status == 'filled'

    # Case 3: Market price is above limit
    sell_order.status = 'pending'  # Reset
    sell_order.execute(market_price=201.00)
    assert sell_order.status == 'filled'


def test_order_receipt():
    """Tests the OrderReceipt data container."""
    order = MarketOrder('AMD', 'buy', 20)
    order.execute(99.0)

    receipt = OrderReceipt(
        symbol=order.symbol,
        side=order.side,
        order=order,
        # It's only checking if the receipt.timestamp attribute correctly hold that value
        timestamp='2023-01-01 12:00:00',
        executed_price=order.price,
        executed_quantity=order.quantity,
        status=order.status
    )

    assert receipt.order_id == id(order)
    assert receipt.symbol == 'AMD'
    assert receipt.original_quantity == 20
    assert receipt.executed_quantity == 20
    assert receipt.executed_price == 99.0
    assert receipt.status == 'filled'
    assert 'OrderReceipt' in repr(receipt)


# --- Test Class for MockBroker ---

class TestMockBroker:

    def setup_method(self, method):
        """Provides a fresh MockBrokerConnector for each test method."""
        self.broker = MockBrokerConnector()

    def test_mock_broker_init(self):
        """Tests the initial state of the MockBrokerConnector."""
        assert self.broker.cash_balance == 100000.0
        assert self.broker.positions == {}
        assert self.broker.order_history == []

    def test_mock_broker_market_data(self):
        """Tests the mock market data generation."""
        df = self.broker.getMarketData('TEST', '2023-01-01', '2023-01-05')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert 'price' in df.columns

    def test_mock_broker_submit_market_buy(self):
        """Tests submitting a successful market BUY order."""
        order = MarketOrder('AAPL', 'buy', 10)
        receipt = self.broker.submitOrder(order)

        assert order.status == 'filled'
        assert order.price == 100.0  # Mock broker's price
        assert self.broker.cash_balance == 100000.0 - (100.0 * 10)
        # --- FIX: Test for QUANTITY (10 shares), not cost (1000.0) ---
        assert self.broker.positions['AAPL'] == 10
        assert order in self.broker.order_history
        assert receipt.status == 'filled'
        assert receipt.executed_quantity == 10

    def test_mock_broker_submit_market_sell(self):
        """Tests submitting a successful market SELL order."""
        order = MarketOrder('MSFT', 'sell', 5)
        receipt = self.broker.submitOrder(order)

        assert order.status == 'filled'
        assert order.price == 100.0
        assert self.broker.cash_balance == 100000.0 + (100.0 * 5)
        # --- FIX: Test for QUANTITY (-5 shares), not cost (-500.0) ---
        assert self.broker.positions['MSFT'] == -5
        assert order in self.broker.order_history
        assert receipt.status == 'filled'
        assert receipt.executed_quantity == 5

    def test_mock_broker_submit_limit_pending(self):
        """Tests a limit order that does NOT fill."""
        # BUY order with limit 95, market is 100
        order = LimitOrder('TSLA', 'buy', 10, 95.0)
        receipt = self.broker.submitOrder(order)

        assert order.status == 'pending'
        assert self.broker.cash_balance == 100000.0  # No change
        assert 'TSLA' not in self.broker.positions
        assert order in self.broker.order_history
        assert receipt.status == 'pending'
        assert receipt.executed_quantity == 0

    def test_mock_broker_account_info(self):
        """Tests the getAccountInfo method."""
        self.broker.cash_balance = 5000.0
        self.broker.positions = {'XYZ': 1234.5}

        info = self.broker.getAccountInfo()

        assert info['cash_balance'] == 5000.0
        assert info['positions']['XYZ'] == 1234.5
        assert info['order_history'] == []