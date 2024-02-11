import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas import DataFrame


def calculate_portfolio_asset_returns(market_prices: DataFrame, price_type: str) -> DataFrame:
    if "SecurityName" not in market_prices.columns or price_type not in market_prices.columns:
        raise ValueError(f"DataFrame must contain 'SecurityName' and {price_type} columns for calculating returns.")

    returns_df = pd.DataFrame()
    grouped = market_prices.groupby("SecurityName")
    for asset_name, asset_data in grouped:
        asset_data = asset_data.sort_values(by="AsOfDate")
        asset_data["Returns"] = asset_data["Close"].pct_change()
        asset_data["LogReturns"] = np.log(asset_data["Close"] / asset_data["Close"].shift(1))
        returns_df = pd.concat([returns_df, asset_data])

    asset_returns_pivot = pd.pivot(
        data=returns_df.dropna(),
        index="AsOfDate",
        columns=["PortfolioShortName", "SecurityName"],
        values=["Returns", "LogReturns"],
    )

    return asset_returns_pivot.sort_index(axis=1, level=[0, 1])


def plot_cumulative_returns(asset_returns: DataFrame) -> None:
    # Create a figure and axis for the plot
    fig, ax = plt.subplots()

    # Plot cumulative returns for each portfolio
    for portfolio_name in asset_returns.columns:
        cumulative_returns = np.exp(asset_returns[portfolio_name].cumsum()) * 100
        ax.plot(
            asset_returns.index,
            cumulative_returns,
            label=portfolio_name,
            linewidth=2,
        )

    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Returns")
    ax.set_title("Cumulative Returns for Each Portfolio")
    ax.legend(loc="upper left")

    # Set the initial value of cumulative returns to 100
    for line in ax.lines:
        ydata = line.get_ydata()
        ydata[0] = 100
        line.set_ydata(ydata)

    plt.show()


def calculate_weights(market_data: DataFrame, price_type: str, weightage_type: str) -> DataFrame:
    pivot_values = ["HeldShares", "Close"]

    market_data_pivot = pd.pivot(
        data=market_data,
        index="AsOfDate",
        columns=["PortfolioShortName", "SecurityName"],
        values=pivot_values,
    )

    weights_by_portfolio = 0
    market_data_pivot = market_data_pivot.fillna(0)

    security_count_by_portfolio = market_data_pivot["HeldShares"].apply(lambda x: x[x != 0].groupby(level=0).count(), axis=1)
    weights_by_portfolio = security_count_by_portfolio

    if weightage_type == "market_weighted":
        market_value = market_data_pivot["HeldShares"].mul(market_data_pivot["Close"])
        market_value = market_value.reorder_levels(["PortfolioShortName", "SecurityName"], axis=1)
        market_values_sum_by_portfolio = market_value.groupby(level="PortfolioShortName", axis=1).sum()
        weights_by_portfolio = market_value.div(market_values_sum_by_portfolio, axis=1)

    elif weightage_type == "price_weighted":
        market_value = market_data_pivot["Close"]
        market_value = market_value.reorder_levels(["PortfolioShortName", "SecurityName"], axis=1)
        market_value = market_value.sort_index(axis=1)
        market_values_sum_by_portfolio = market_value.groupby(level="PortfolioShortName", axis=1).sum()
        weights_by_portfolio = market_value.div(market_values_sum_by_portfolio, axis=1)

    else:
        portfolio_sum = market_data_pivot["HeldShares"].groupby(level=0, axis=1).sum()
        weights_by_portfolio = market_data_pivot["HeldShares"].div(portfolio_sum, axis=0)

    return weights_by_portfolio
