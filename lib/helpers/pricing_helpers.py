from numba import jit
import math

NUM_DAYS_PER_YEAR = 365


# @jit(nopython=True)
def pdf(x):
    return math.exp(-0.5 * x ** 2) / math.sqrt(2 * math.pi)


# @jit(nopython=True)
def cdf(x):
    # Constants
    a1 = 0.254829592
    a2 = -0.284496736
    a3 = 1.421413741
    a4 = -1.453152027
    a5 = 1.061405429
    p = 0.3275911

    # Save the sign of x
    sign = 1 if x >= 0 else -1
    x = abs(x) / 2 ** 0.5  # Use constant instead of math.sqrt(2.0)

    # A constant used in the CDF calculation
    t = 1.0 / (1.0 + p * x)

    # Calculate the CDF using the approximation formula
    y = 1.0 - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * math.exp(-x * x))

    # Adjust for the sign
    return 0.5 * (1.0 + sign * y)


# @jit(nopython=True)
def d1_d2(spot, strike, maturity, rate, div, vol):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1 = (math.log(spot / strike) + (rate - div + 0.5 * vol ** 2) * maturity) / (vol * math.sqrt(maturity))
    d2 = d1 - vol * math.sqrt(maturity)
    return d1, d2


# @jit(nopython=True)
def price_bs(spot, strike, maturity, rate, div, vol, op_type):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1, d2 = d1_d2(spot, strike, maturity, rate, div, vol)
    typefac = 2.0 * op_type - 1.0
    _price = typefac * spot * math.exp(-div * maturity) * cdf(typefac * d1) - typefac * strike * math.exp(-rate * maturity) * cdf(typefac * d2)
    return _price


# @jit(nopython=True)
def delta(spot, strike, maturity, rate, div, vol, op_type):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1, d2 = d1_d2(spot, strike, maturity, rate, div, vol)
    typefac = 2.0 * op_type - 1.0
    _delta = typefac * cdf(typefac * d1) * math.exp(-div * maturity)
    return _delta


# @jit(nopython=True)
def gamma(spot, strike, maturity, rate, div, vol, op_type):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1, d2 = d1_d2(spot, strike, maturity, rate, div, vol)
    _gamma = (pdf(d1) * math.exp(-div * maturity) / spot / math.sqrt(maturity) / vol) * (vol > 1e-10)
    return _gamma


# @jit(nopython=True)
def vega(spot, strike, maturity, rate, div, vol, op_type):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1, d2 = d1_d2(spot, strike, maturity, rate, div, vol)
    _vega = pdf(d1) * spot * math.exp(-div * maturity) * math.sqrt(maturity)
    return _vega


# @jit(nopython=True)
def theta(spot, strike, maturity, rate, div, vol, op_type):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1, d2 = d1_d2(spot, strike, maturity, rate, div, vol)
    typefac = 2.0 * op_type - 1.0
    _theta = -math.exp(-div * maturity) * spot * pdf(d1) * vol / (2 * math.sqrt(maturity)) - \
             typefac * rate * strike * math.exp(-rate * maturity) * cdf(typefac * d2) + \
             typefac * div * spot * math.exp(-div * maturity) * cdf(typefac * d1)
    return _theta / NUM_DAYS_PER_YEAR


# @jit(nopython=True)
def rho(spot, strike, maturity, rate, div, vol, op_type):
    maturity = maturity / NUM_DAYS_PER_YEAR
    d1, d2 = d1_d2(spot, strike, maturity, rate, div, vol)
    typefac = 2.0 * op_type - 1.0
    _rho = typefac * strike * maturity * math.exp(-rate * maturity) * cdf(typefac * d2)
    return _rho


# @jit(nopython=True)
def implied_vol(spot, strike, maturity, rate, div, price, op_type, low_vol=0.0, high_vol=2.0, max_iter=20, max_error=0.01):
    maturity = maturity / NUM_DAYS_PER_YEAR
    n = 1
    mid_vol = (low_vol + high_vol) / 2
    while n < max_iter:
        mid_vol = (low_vol + high_vol) / 2
        mid_price = price_bs(spot, strike, maturity, rate, div, mid_vol, op_type)
        if abs(price - mid_price) < max_error:
            break
        elif mid_price > price:
            high_vol = mid_vol
        else:
            low_vol = mid_vol
        n += 1
    return mid_vol