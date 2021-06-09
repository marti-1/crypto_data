import numpy as np
import matplotlib.pyplot as plt


def annualize_rets(r, periods_per_year, n_periods=None):
    compounded_growth = (1 + r).prod()
    if n_periods is None:
        n_periods = r.shape[0]
    return compounded_growth ** (periods_per_year / n_periods) - 1


def sharpe_ratio(r, riskfree_rate, periods_per_year):
    """
    riskfree_rate -- annual interest rate of risk free asset
    """
    rf_per_period = (1 + riskfree_rate) ** (1 / periods_per_year) - 1
    excess_ret = r - rf_per_period

    return (periods_per_year ** .5) * np.nanmean(excess_ret) / np.nanstd(excess_ret)


def lag1(x):
    y = np.empty(len(x))
    y.fill(np.nan)
    y[1:] = x[:-1]
    return y


def pct_change(x):
    y = x / lag1(x) - 1
    y[0] = 0
    return y


def sma(cl, w):
    cl = np.array(cl, dtype=np.float)
    y = cl.copy()
    y[w - 1:] = np.convolve(cl, np.ones(w) / w, 'valid')
    return y


def cppi_strategy(price, drawdown, m):
    T = len(price)
    b = 1
    floor = 1 - drawdown
    risky_w = np.zeros(T)
    cushion = np.zeros(T)
    peak = b
    r = np.zeros(T)
    for t in range(1, T):
        risky_r = price[t] / price[t - 1] - 1
        risky_alloc = b * risky_w[t - 1]
        safe_alloc = b * (1 - risky_w[t - 1])
        b_new = risky_alloc * (1 + risky_r) + safe_alloc
        b = b_new
        if b > peak:
            peak = b
            floor = peak * (1 - drawdown)

        cushion[t] = (b - floor) / b
        risky_w[t] = max(min(cushion[t] * m, 1), 0)

    return risky_w, cushion


def cppi_r(price, risky_w, slip):
    usd = 1
    eth = 0
    T = len(risky_w)
    b = usd + eth * price[0]
    r = np.zeros(T)
    for t in range(1, T):
        b_new = usd + eth * price[t]
        r[t] = b_new / b - 1
        deth = (risky_w[t] * b_new - eth * price[t]) / price[t]
        eth = eth + deth
        if deth > 0:
            usd = usd - deth * price[t] * (1 + slip)
        elif deth < 0:
            usd = usd + abs(deth) * price[t] * (1 - slip)
        b = b_new
    return r


def sma_strategy(price, w, slip=0, threshold=0, start=(1, 0)):
    usd, eth = start
    ma = sma(price, w)
    b = usd + eth * price[0]
    T = len(price)
    r = np.zeros(T)
    entries = np.zeros(T, dtype=bool)
    exits = np.zeros(T, dtype=bool)
    for t in range(1, T):
        b_new = usd + eth * price[t]
        r[t] = b_new / b - 1
        if usd > 0 and price[t] > ma[t] * (1 + threshold):
            entries[t] = True
            eth = usd / (price[t] * (1 + slip))
            usd = 0
        elif eth > 0 and price[t] < ma[t] * (1 - threshold):
            exits[t] = True
            usd = eth * price[t] * (1 - slip)
            eth = 0
        b = b_new
    return r, entries, exits


def sma_buy_dip_strategy(price, w, slip=0, threshold=0, start=(1, 0), alpha_th=1):
    usd, eth = start
    ma = sma(price, w)
    alpha = price / ma
    b = usd + eth * price[0]
    T = len(price)
    r = np.zeros(T)
    entries = np.zeros(T, dtype=bool)
    exits = np.zeros(T, dtype=bool)
    bd = False
    for t in range(1, T):
        b_new = usd + eth * price[t]
        r[t] = b_new / b - 1

        if bd and price[t] > ma[t] * (1 + threshold):
            bd = False

        if alpha[t] <= alpha_th and usd > 0:
            entries[t] = True
            eth = usd / (price[t] * (1 + slip))
            usd = 0
            bd = True
        elif usd > 0 and price[t] > ma[t] * (1 + threshold):
            entries[t] = True
            eth = usd / (price[t] * (1 + slip))
            usd = 0
        elif eth > 0 and price[t] < ma[t] * (1 - threshold) and not bd:
            exits[t] = True
            usd = eth * price[t] * (1 - slip)
            eth = 0
        b = b_new
    return r, entries, exits


def topbottom(price):
    r = pct_change(price)
    tops = []
    bots = []

    max_top = 1
    min_bot = 1

    top_cr = 1
    bot_cr = 1

    bot_t = 1
    top_t = 1

    for t in range(1, len(r)):
        top_cr = top_cr * (1 + r[t])
        bot_cr = bot_cr * (1 + r[t])

        if top_cr >= 2 and top_cr > max_top:
            top_t = t
            max_top = top_cr
            if min_bot <= .5:
                bots.append(bot_t)
            bot_cr = 1
            min_bot = 1
        elif bot_cr <= .5 and bot_cr < min_bot:
            bot_t = t
            min_bot = bot_cr
            if max_top >= 2:
                tops.append(top_t)
            top_cr = 1
            max_top = 1
    if max_top >= 2:
        tops.append(top_t)
    if min_bot <= .5:
        bots.append(bot_t)

    return tops, bots


def selloff_sizes(price, tops, bots):
    if len(tops) == 0 or len(bots) == 0:
        return []

    if bots[0] < tops[0]:
        bots = bots[1:]

    acc = []
    for i in range(len(bots)):
        acc.append(price[bots[i]] / price[tops[i]] - 1)

    return acc
