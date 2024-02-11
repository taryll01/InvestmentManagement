from itertools import chain

import numpy as np
import pandas as pd
import scipy.stats

webData = pd.read_csv("calculation_engines\\risk_engines\\MSCIRiskMetricsPrices.csv", header=0)
dailyPrices = webData.iloc[:, 1:6]
units_held = np.array([9, 5, 18, 7, 3])


logReturns = np.log(dailyPrices / dailyPrices.shift(1)).dropna()
previousDayPrice = dailyPrices.tail(1).values
previousDayPortfolio = list(chain.from_iterable(previousDayPrice * units_held))
weights = previousDayPortfolio / np.sum(previousDayPortfolio)

print(weights)


# Historic Simulation
historicSimulatedPrice = logReturns.mul(weights, axis=1)
historicSimulatedPortfolio = historicSimulatedPrice.sum(axis=1)
historicSimulationVaR95 = historicSimulatedPortfolio.quantile(0.05)
historicSimulationVaR99 = historicSimulatedPortfolio.quantile(0.01)

# Delta Normal Parametric
portfolioMean = (logReturns.mean() * weights).sum()
portfolioVariance = np.dot(weights.T, np.dot(logReturns.cov(), weights))
portfolioStdDeviation = np.sqrt(portfolioVariance)
parametricVaR95 = scipy.stats.norm.ppf(0.05, portfolioMean, portfolioStdDeviation)
parametricVaR99 = scipy.stats.norm.ppf(0.01, portfolioMean, portfolioStdDeviation)


print(
    [
        "1 Day 95% Historical VaR =",
        historicSimulationVaR95 * np.sum(previousDayPortfolio),
    ],
    [
        "1 Day 99% Historical VaR = ",
        historicSimulationVaR99 * np.sum(previousDayPortfolio),
    ],
)
print(
    ["1 Day 95% Parametric VaR = ", parametricVaR95 * np.sum(previousDayPortfolio)],
    ["1 Day 99% Parametric VaR ", parametricVaR99 * np.sum(previousDayPortfolio)],
)
