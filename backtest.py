import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt

# ---------------------------------------------------------
# Load the HQM Strategy produced by your original code
# ---------------------------------------------------------
file_path = "momentum_strategy.xlsx"   # output from your strategy code
strategy = pd.read_excel(file_path)

tickers = strategy["Ticker"].tolist()
shares = strategy["Number of Shares to Buy"].tolist()

print(f"Loaded {len(tickers)} tickers from momentum_strategy.xlsx")

# ---------------------------------------------------------
# Download future price data for backtest
# ---------------------------------------------------------

# Detect the last date in your strategy (dynamic)
start_date = pd.Timestamp.today().normalize() - pd.DateOffset(days=1)
start_date = start_date.strftime("%Y-%m-%d")

print("Backtest start date:", start_date)

# Download 1 year of future data (you may expand)
prices = yf.download(tickers, start=start_date, period="1y", progress=False)

# If MultiIndex, flatten
if isinstance(prices.columns, pd.MultiIndex):
    prices.columns = prices.columns.get_level_values(0)

# Keep only close prices
prices = prices["Close"].ffill()

# ---------------------------------------------------------
# Compute Portfolio Value Over Time
# ---------------------------------------------------------

portfolio_value = pd.DataFrame(index=prices.index)
portfolio_value["Portfolio"] = 0

for i, t in enumerate(tickers):
    try:
        portfolio_value["Portfolio"] += prices[t] * shares[i]
    except KeyError:
        print(f"Warning: missing data for {t}")

# Normalize for comparison
portfolio_norm = portfolio_value["Portfolio"] / portfolio_value["Portfolio"].iloc[0]

# ---------------------------------------------------------
# Benchmark: SPY
# ---------------------------------------------------------
spy = yf.download("SPY", start=portfolio_value.index[0], end=portfolio_value.index[-1], progress=False)["Close"]
spy_norm = spy / spy.iloc[0]

# ---------------------------------------------------------
# Calculate Performance Statistics
# ---------------------------------------------------------

returns = portfolio_value["Portfolio"].pct_change().dropna()

# Annualized return (CAGR)
days = (portfolio_value.index[-1] - portfolio_value.index[0]).days
years = days / 365
cagr = (portfolio_value["Portfolio"].iloc[-1] / portfolio_value["Portfolio"].iloc[0]) ** (1 / years) - 1

# Volatility (annualized)
vol = returns.std() * np.sqrt(252)

# Sharpe ratio (risk-free = 0)
sharpe = cagr / vol

print("\n---------------- Performance ----------------")
print(f"CAGR: {cagr:.2%}")
print(f"Annual Volatility: {vol:.2%}")
print(f"Sharpe Ratio: {sharpe:.2f}")
print("------------------------------------------------\n")

# ---------------------------------------------------------
# Drawdown calculation
# ---------------------------------------------------------
portfolio_value["Peak"] = portfolio_value["Portfolio"].cummax()
portfolio_value["Drawdown"] = (portfolio_value["Portfolio"] - portfolio_value["Peak"]) / portfolio_value["Peak"]

# ---------------------------------------------------------
# Plot: Portfolio vs SPY
# ---------------------------------------------------------
plt.figure(figsize=(12,6))
plt.plot(portfolio_norm, label="HQM Strategy")
plt.plot(spy_norm, label="SPY Benchmark", alpha=0.8)
plt.title("HQM Strategy vs SPY Benchmark")
plt.ylabel("Normalized Return")
plt.legend()
plt.grid(True)
plt.show()

# ---------------------------------------------------------
# Plot: Equity Curve
# ---------------------------------------------------------
plt.figure(figsize=(12,6))
plt.plot(portfolio_value["Portfolio"], label="Portfolio Value")
plt.title("HQM Strategy Equity Curve")
plt.ylabel("Equity ($)")
plt.grid(True)
plt.show()

# ---------------------------------------------------------
# Plot: Drawdown
# ---------------------------------------------------------
plt.figure(figsize=(12,4))
plt.plot(portfolio_value["Drawdown"], color='red')
plt.title("Drawdown (%)")
plt.grid(True)
plt.show()
