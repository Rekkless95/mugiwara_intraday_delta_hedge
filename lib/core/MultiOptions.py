import pandas as pd

import numpy as np

from lib.helpers.pricing_helpers import price_bs, delta, gamma, vega, theta, rho
from lib.helpers.rrule_helper import (get_roll_dates, get_eligible_maturity, weekdays_rule, mondays_wednesdays_fridays_rule, fridays_rule, third_fridays_rule, third_fridays_quarterly_rule)
import os
import pytz
import copy
import yfinance as yf
from datetime import datetime

ny_tz = pytz.timezone('America/New_York')
PATH_SPOT = r'X:\Main Folder\Data\Spot'
DATAPATH = r"X:\Main Folder\Data"


class MultiOptionsRoll():

    def __init__(self, params):
        self.params = params

        self.start_date = pd.to_datetime(self.params['start_date']).tz_localize(ny_tz) + pd.Timedelta(hours=16)
        self.end_date = pd.to_datetime(self.params['end_date']).tz_localize(ny_tz) + pd.Timedelta(hours=16)
        self.calendar = None

        self.underlying_ticker = self.params.get("underlying_ticker", None)
        self.vol_ticker = self.params.get("vol_ticker", 'VIX')
        self.rf_ticker = self.params.get("rf_ticker", '^IRX')
        self.notional = self.params.get('notional', None)
        self.pnl_explanation = self.params.get('pnl_explanation', False)
        self.close_time = self.params.get('close_time', '16:00')

        self.spot = None
        self.div = None
        self.rf = None
        self.div = None

        self.legs = self.params.get('legs', [])

        self.roll_days = self.params.get("roll_days", [])
        self.eligible_maturities = self.params.get("eligible_maturities", [])
        self.maturity_number = self.params.get("maturity_number", [])
        self.unwind_dates = self.params.get("unwind_dates", [])
        self.holding_period = self.params.get("holding_period", [])

        self.delta_strike = self.params.get("delta_strike", [])
        self.moneyness = self.params.get("moneyness", [])
        self.type = self.params.get("type", [])
        self.leverage = self.params.get("leverage", [])

        self.delta_fees_bps = self.params.get("delta_fees_bps", 0) / 10000
        self.delta_hedging_time = self.params.get("delta_hedging_time", [])

        self.roll_calendar = []

        self.data_folder = os.path.join(DATAPATH, 'Options', self.underlying_ticker, 'Pre Processed')
        self.opened_file = ''
        self.opened_data_file = None

        self.live_portfolio = {}
        self.expired_portfolio = {}
        self.unwind_portfolio = {}
        self.new_options = {}

        self.results = {}

        self.mvop = {}
        self.cash = {}
        self.fees = {}
        self.il = {}
        self.premiums = {}
        self.payoffs = {}
        self.unwinds = {}

        self.hedging_delta = {}
        self.delta_hedge_pnl = {}
        self.spot_quantity = {}
        self.delta_fees = {}

        self.pnl = {}
        self.pnl_unexplained = {}

        self.portfolio_delta = {}
        self.portfolio_gamma = {}
        self.portfolio_vega = {}
        self.portfolio_theta = {}
        self.portfolio_rho = {}

        self.pnl_delta = {}
        self.pnl_gamma = {}
        self.pnl_vega = {}
        self.pnl_theta = {}
        self.pnl_rho = {}

    def get_calendar(self):
        observation_times = np.hstack(self.delta_hedging_time)
        observation_times = sorted(set(observation_times))

        if self.close_time not in observation_times:
            if len(observation_times) == 0:
                observation_times.append(self.close_time)
            else:
                observation_times.extend(self.close_time)

        calendar = pd.read_csv(os.path.join(DATAPATH, 'Options', self.underlying_ticker, f"calendar.csv"))['Index'].values
        calendar = pd.to_datetime(calendar, utc=False)
        calendar = calendar[(calendar >= self.start_date) & (calendar <= self.end_date)]

        calendar = [[date.replace(hour=int(time.split(':')[0]), minute=int(time.split(':')[1])) for time in observation_times] for date in calendar]

        calendar = sorted(set([item for sublist in calendar for item in sublist]))

        return pd.DatetimeIndex(calendar, tz=str(ny_tz))

    def get_roll_calendar(self, ix=None):
        if ix is None:
            roll_calendar = get_roll_dates(self.roll_days, self.calendar)
        else:
            roll_calendar = get_roll_dates(self.roll_days[ix], self.calendar)

        roll_calendar = [date.replace(hour=int(self.close_time.split(':')[0]), minute=int(self.close_time.split(':')[1])) for date in roll_calendar]

        return roll_calendar

    def get_historical_rf_q(self):
        start_date = pd.Timestamp('2000-01-01')
        end_date = pd.Timestamp('2024-05-01')
        interval = '1d'

        self.rf = yf.Ticker(self.rf_ticker).history(start=start_date, end=end_date, interval=interval)
        self.rf.index = self.rf.index.map(lambda x: x.tz_convert('America/New_York').replace(hour=16))
        self.rf = self.rf['Close']
        self.rf = self.rf.reindex(self.calendar, method='ffill')

        self.div = yf.Ticker(self.underlying_ticker).dividends
        self.div.index = self.div.index.map(lambda x: x.tz_convert('America/New_York').replace(hour=16))
        self.div = self.div.reindex(self.calendar, method='ffill')
        self.div = self.div.rolling(window='365D').sum()
        self.div = pd.DataFrame(self.div)
        self.div = self.div.loc[(self.spot.index[0] <= self.div.index) & (self.div.index <= self.spot.index[-1])].apply(lambda x: x / self.spot.loc[x.name], axis=1).reindex(self.spot.index, method='ffill').dropna()['Dividends']

    def initialize(self):
        # Get each leg's info
        for ix, leg in enumerate(self.legs):
            # dates and calendars
            self.roll_days.append(leg.get('roll_days', None))
            self.eligible_maturities.append(leg.get('eligible_maturities', None))
            self.maturity_number.append(leg.get('maturity_number', None))
            self.unwind_dates.append(leg.get('unwind_dates', None))
            self.holding_period.append(leg.get('holding_period', None))
            # option description
            self.moneyness.append(leg.get('moneyness', None))
            self.delta_strike.append(leg.get('delta_strike', None))
            self.type.append(leg.get('type', None))
            self.leverage.append(leg.get('leverage', None))
            # delta hedging
            self.delta_hedging_time.append(leg.get('delta_hedging_time', []))

        # Initialize Calendar
        self.calendar = self.get_calendar()

        # Set Roll Schedule
        for ix, leg in enumerate(self.legs):
            self.roll_calendar.append(self.get_roll_calendar(ix))

        # Get Intraday Spot Data
        spot_file_name = f"{self.underlying_ticker} Intraday.csv"
        self.spot = pd.read_csv(os.path.join(DATAPATH, 'Spot', spot_file_name), index_col=0)['Close']
        self.spot.index = pd.to_datetime(self.spot.index, utc=False)
        self.spot = self.spot.reindex(self.calendar, method='ffill')

        # Get VIX Data
        vix_file_name = f"{self.vol_ticker} Daily.csv"
        self.vix = pd.read_csv(os.path.join(DATAPATH, 'Spot', vix_file_name), index_col=0)['Close']
        self.vix.index = pd.to_datetime(self.vix.index, utc=False)

        # Get Risk Free Rate and Dividend Yield
        self.get_historical_rf_q()

    def get_target_maturity(self, date, ix=None):
        if ix is None:
            return get_eligible_maturity(self.eligible_maturities, date, self.maturity_number).replace(hour=int(self.close_time.split(":")[0]), minute=int(self.close_time.split(":")[1]))
        else:
            return get_eligible_maturity(self.eligible_maturities[ix], date, self.maturity_number[ix]).replace(hour=int(self.close_time.split(":")[0]), minute=int(self.close_time.split(":")[1]))

    def get_unwind_date(self, date, ix=None):
        if ix is None:
            return pd.Timestamp(get_eligible_maturity(self.unwind_dates, date, self.holding_period)).replace(hour=int(self.close_time.split(":")[0]), minute=int(self.close_time.split(":")[1]))
        else:
            return pd.Timestamp(get_eligible_maturity(self.unwind_dates[ix], date, self.holding_period[ix])).replace(hour=int(self.close_time.split(":")[0]), minute=int(self.close_time.split(":")[1]))

    def get_quantity(self, date, ix=None):
        if ix is None:
            return self.leverage * self.notional / self.spot.loc[date]
        else:
            return self.leverage[ix] * self.notional / self.spot.loc[date]

    def get_new_options(self, date, ix=None):
        # get today's option data and spot close
        spot = self.spot.loc[date]
        data = self.get_option_data(date)

        # List of options to return
        new_options = []

        # Set options characteristics
        if ix is None:
            type = self.type
            target_maturity = self.get_target_maturity(date)
            moneyness = self.moneyness
            delta_strike = self.delta_strike
            unwind = self.get_unwind_date(date)
            quantity = self.get_quantity(date)
        else:
            type = self.type[ix]
            target_maturity = self.get_target_maturity(date, ix)
            moneyness = self.moneyness[ix]
            delta_strike = self.delta_strike[ix]
            unwind = self.get_unwind_date(date, ix)
            quantity = self.get_quantity(date, ix)

        # select closest higher or equal maturity available
        maturity = data['Maturity'].loc[(data['Maturity'].apply(lambda x: x.date()) >= target_maturity.date()) & (data['Type'] == type)].min()
        data = data.loc[(data['Maturity'] == maturity) & (data['Type'] == type)]
        if type == 0:
            if moneyness is not None:
                strike = data['Strike'].loc[(data['Strike'] <= spot * moneyness) & (data['Type'] == type)].max()
            if delta_strike is not None:
                delta = data['Delta'].loc[(data['Delta'].apply(abs) <= delta_strike) & (data['Type'] == type)].min()
                strike = data['Strike'].loc[data['Delta'] == delta].iloc[0]
        else:
            if moneyness is not None:
                strike = data['Strike'].loc[(data['Strike'] >= spot * moneyness) & (data['Type'] == type)].min()
            if delta_strike is not None:
                delta = data['Delta'].loc[(data['Delta'].apply(abs) <= delta_strike) & (data['Type'] == type)].max()
                strike = data['Strike'].loc[data['Delta'] == delta].iloc[0]

        # Select Option
        option = {'Strike_date': date,
                  'Maturity': maturity,
                  'Unwind': unwind,
                  'Strike': strike,
                  'Type': type,
                  'Quantity': quantity,
                  'index': ix}

        new_options.append(option)
        return new_options

    def build_rolling_portfolios(self):
        for t, date in enumerate(self.calendar):
            # select the correct previous date and previous portfolio
            if t == 0:
                prev_portfolio = {}
            else:
                prev_date = self.calendar[t - 1]
                prev_portfolio = self.live_portfolio[prev_date]

            # Keep only live options today
            live_portfolio = [option for option in copy.deepcopy(prev_portfolio) if (option['Maturity'] > date and option['Unwind'] > date)]

            # Get new opened positions
            self.new_options[date] = []
            for ix in range(len(self.legs)):
                if date in self.roll_calendar[ix]:
                    self.new_options[date].extend(self.get_new_options(date, ix))

            # Construct live portfolio for today
            self.live_portfolio[date] = live_portfolio + self.new_options[date]
            # Add expired options portfolio for today
            self.expired_portfolio[date] = [option for option in copy.deepcopy(prev_portfolio) if (option['Maturity'] == date)]
            # Add unwind options portfolio for today
            self.unwind_portfolio[date] = [option for option in copy.deepcopy(prev_portfolio) if ((option['Maturity'] > date) and (date == option['Unwind']))]

    def get_option_data(self, date):
        file_name = f'{self.underlying_ticker.lower()}_eod_{date.strftime('%Y%m')}.csv'
        file_path = os.path.join(self.data_folder, file_name)

        if file_path != self.opened_file:
            # Use new file
            self.opened_file = file_path
            self.opened_data_file = pd.read_csv(self.opened_file, index_col=0)
            # Reformat index to close NY
            self.opened_data_file.index = pd.to_datetime(self.opened_data_file.index)
            self.opened_data_file.index = self.opened_data_file.index.normalize() + pd.Timedelta(hours=16)
            self.opened_data_file.index = self.opened_data_file.index.tz_localize(ny_tz)
            # Reformat Maturity to close NY
            self.opened_data_file.loc[:, 'Maturity'] = pd.to_datetime(self.opened_data_file['Maturity']).apply(lambda x: x.normalize().tz_localize(ny_tz) + pd.Timedelta(hours=16))
            # format the rest
            self.opened_data_file.loc[:, 'Strike'] = pd.to_numeric(self.opened_data_file['Strike'], errors='coerce')
            self.opened_data_file.loc[:, 'Bid'] = pd.to_numeric(self.opened_data_file['Bid'], errors='coerce')
            self.opened_data_file.loc[:, 'Ask'] = pd.to_numeric(self.opened_data_file['Ask'], errors='coerce')
            self.opened_data_file.loc[:, 'Spot'] = pd.to_numeric(self.opened_data_file['Spot'], errors='coerce')
            self.opened_data_file.loc[:, 'Volume'] = pd.to_numeric(self.opened_data_file['Volume'], errors='coerce')
            self.opened_data_file.loc[:, 'Implied Vol'] = pd.to_numeric(self.opened_data_file['Implied Vol'], errors='coerce')
            self.opened_data_file.loc[:, 'Delta'] = pd.to_numeric(self.opened_data_file['Delta'], errors='coerce')
            self.opened_data_file.loc[:, 'Gamma'] = pd.to_numeric(self.opened_data_file['Gamma'], errors='coerce')
            self.opened_data_file.loc[:, 'Vega'] = pd.to_numeric(self.opened_data_file['Vega'], errors='coerce')
            self.opened_data_file.loc[:, 'Theta'] = pd.to_numeric(self.opened_data_file['Theta'], errors='coerce')
            self.opened_data_file.loc[:, 'Rho'] = pd.to_numeric(self.opened_data_file['Rho'], errors='coerce')
            self.opened_data_file.loc[:, 'Type'] = pd.to_numeric(self.opened_data_file['Type'], errors='coerce')
            self.opened_data_file.loc[:, 'Mid'] = (self.opened_data_file.loc[:, 'Bid'] + self.opened_data_file.loc[:, 'Ask']) / 2
            self.opened_data_file.loc[:, 'Spread'] = self.opened_data_file.loc[:, 'Ask'] - self.opened_data_file.loc[:, 'Bid']

        return self.opened_data_file.loc[date].dropna(subset=['Mid'])

    def attach_market_data(self):
        for date in self.calendar:
            print(date)
            # if date is in the close use option metrics
            if date.hour == 16 and date.minute == 0:
                data = self.get_option_data(date)
                # Get live portfolio data
                options = []
                for option in self.live_portfolio[date]:
                    option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                    option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = \
                        option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
                    options.append(option)
                self.live_portfolio[date] = options
                # Get expired portfolio data
                options = []
                for option in self.expired_portfolio[date]:
                    option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                    if len(option_data) == 0:
                        option['Mid'] = option["Type"] * max(self.spot[date] - option['Strike'], 0) + (1 - option["Type"]) * max(option['Strike'] - self.spot[date], 0)
                        option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = 0, 0, (self.spot[date] > option['Strike']) * (option['Type'] - 0.5) * 2, 0, 0, 0, 0
                    else:
                        option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
                    options.append(option)
                self.expired_portfolio[date] = options
                # Get unwind portfolio data
                options = []
                for option in self.unwind_portfolio[date]:
                    option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                    if len(option_data) == 0:
                        option['Mid'] = option["Type"] * max(spot - option['Strike'], 0) + (1 - option["Type"]) * max(option['Strike'] - spot, 0)
                        option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = 0, 0, (spot > option['Strike']) * (option['Type'] - 0.5) * 2, 0, 0, 0, 0
                    else:
                        option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
                    options.append(option)
                self.unwind_portfolio[date] = options
            # if time is not close : intraday delta hedging : no expiration and unwind, compute metrics for intraday delta hedging
            else:
                if date.replace(hour=0, minute=0) < min(self.calendar):
                    options = []
                    self.live_portfolio[date] = options
                    self.expired_portfolio[date] = options
                    self.unwind_portfolio[date] = options
                    continue
                # Get previous day's Close data
                dt = max([dt for dt in self.calendar if dt < date.replace(hour=0, minute=0)])
                data = self.get_option_data(dt.replace(hour=16, minute=0))

                options = []
                for option in self.live_portfolio[date]:
                    option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                    # Use Previous day's Implied Vol to compute greeks now using current spot
                    option['Implied Vol'] = option_data['Implied Vol'].iloc[0]
                    spot, strike, maturity, rate, div, vol, op_type = self.spot[date], option['Strike'], (option['Maturity'] - date).total_seconds() / 3600 / 24, self.rf.loc[dt.replace(hour=16, minute=0)] / 100, self.div.loc[dt.replace(hour=16, minute=0)], option['Implied Vol'], option['Type']

                    option['Mid'] = price_bs(spot, strike, maturity, rate, div, vol, op_type)
                    option['Delta'] = delta(spot, strike, maturity, rate, div, vol, op_type)
                    option['Gamma'] = gamma(spot, strike, maturity, rate, div, vol, op_type)
                    option['Vega'] = vega(spot, strike, maturity, rate, div, vol, op_type)
                    option['Theta'] = theta(spot, strike, maturity, rate, div, vol, op_type)
                    option['Rho'] = rho(spot, strike, maturity, rate, div, vol, op_type)

                    options.append(option)

                self.live_portfolio[date] = options

    def compute_pnl(self):
        hedging_delta = {}
        spot_quantity = {}
        delta_hedge_pnl = {}
        delta_fees = {}
        premiums = {}
        payoffs = {}
        unwinds = {}
        mvop = {}
        cash = {}
        fees = {}
        il = {}

        for t, date in enumerate(self.calendar):
            # MVOP is equal to the new valuation at mid of current live options
            mvop[date] = sum(option['Mid'] * option['Quantity'] for option in self.live_portfolio[date])

            # Initialize cash = Notional or previous cash level
            if date == self.calendar[0]:
                prev_date = date
                cash[date] = self.notional
                spot_quantity[date] = 0
            else:
                prev_date = self.calendar[t - 1]
                cash[date] = cash[prev_date]

            # Adjust cash balance to latest options added removed at Mid
            premiums[date] = - sum(option['Mid'] * option['Quantity'] for option in self.new_options[date])
            payoffs[date] = sum(option['Mid'] * option['Quantity'] for option in self.expired_portfolio[date])
            unwinds[date] = sum(option['Mid'] * option['Quantity'] for option in self.unwind_portfolio[date])

            cash[date] = (cash[date] + premiums[date] + payoffs[date] + unwinds[date])
            # Compute Daily Execution fees
            fees[date] = (sum(0.5 * option['Spread'] * abs(option['Quantity']) for option in self.new_options[date])
                          + sum(0.5 * option['Spread'] * abs(option['Quantity']) for option in self.unwind_portfolio[date])
                          + sum(0.5 * option['Spread'] * abs(option['Quantity']) for option in self.expired_portfolio[date]))

            # Delta Hedging
            # Compute PnL from previous delta hedge
            delta_hedge_pnl[date] = spot_quantity[prev_date] * (self.spot[date] - self.spot[prev_date])  # Delta hedging PnL from the spot portfolio
            # Compute current delta to be hedged and number of underlying
            hedging_delta[date] = sum(option['Delta'] * option['Quantity'] for option in self.live_portfolio[date] if (date.strftime('%H:%M') in self.delta_hedging_time[option['index']]))
            spot_quantity[date] = hedging_delta[date]
            # Compute delta hedge fees
            delta_fees[date] = abs(spot_quantity[date] - spot_quantity[prev_date]) * self.spot[date] * self.delta_fees_bps

            # Remove execution fees and delta hedging fees and adding hedging pnl to cash balance
            cash[date] = cash[date] - fees[date] - delta_fees[date] + delta_hedge_pnl[date]
            # Compute Index Level
            il[date] = cash[date] + mvop[date]

        self.hedging_delta = hedging_delta
        self.delta_hedge_pnl = delta_hedge_pnl
        self.spot_quantity = spot_quantity
        self.delta_fees = delta_fees
        self.payoffs = payoffs
        self.premiums = premiums
        self.unwinds = unwinds
        self.mvop = mvop
        self.cash = cash
        self.fees = fees
        self.il = il

    def explain_pnl(self):
        pnl = {}
        pnl_unexplained = {}

        portfolio_delta = {}
        portfolio_gamma = {}
        portfolio_vega = {}
        portfolio_theta = {}
        portfolio_rho = {}

        pnl_delta = {}
        pnl_gamma = {}
        pnl_vega = {}
        pnl_theta = {}
        pnl_rho = {}

        for ix, date in enumerate(self.calendar):
            if ix == 0:
                pnl[date] = 0
                pnl_unexplained[date] = 0
                portfolio_delta[date] = 0
                portfolio_gamma[date] = 0
                portfolio_vega[date] = 0

                portfolio_theta[date] = 0
                portfolio_rho[date] = 0
                pnl_delta[date] = 0
                pnl_gamma[date] = 0
                pnl_vega[date] = 0
                pnl_theta[date] = 0
                pnl_rho[date] = 0

            else:
                prev_date = self.calendar[ix - 1]
                pnl[date] = self.il[date] - self.il[prev_date]

                # Compute Greeks for the portfolio
                portfolio_delta[date] = sum(option['Delta'] * option['Quantity'] for option in self.live_portfolio[prev_date])
                portfolio_gamma[date] = sum(option['Gamma'] * option['Quantity'] for option in self.live_portfolio[prev_date])
                portfolio_vega[date] = sum(option['Vega'] * option['Quantity'] for option in self.live_portfolio[prev_date])
                portfolio_theta[date] = sum(option['Theta'] * option['Quantity'] for option in self.live_portfolio[prev_date])
                portfolio_rho[date] = sum(option['Rho'] * option['Quantity'] for option in self.live_portfolio[prev_date])

                # PnL Attribution
                pnl_delta[date] = (portfolio_delta[prev_date] - self.hedging_delta[prev_date]) * (self.spot.loc[date] - self.spot.loc[prev_date])

                pnl_gamma[date] = 0.5 * portfolio_gamma[prev_date] * ((self.spot.loc[date] - self.spot.loc[prev_date]) ** 2)

                pnl_vega[date] = 0

                for option in self.live_portfolio[prev_date]:
                    if option['Maturity'] > date and option['Unwind'] > date:
                        for option_ in self.live_portfolio[date]:
                            if option_['Strike'] == option['Strike'] and option_['Type'] == option['Type'] and option_['Maturity'] == option['Maturity'] and option_['Unwind'] == option['Unwind']:
                                pnl_vega[date] += option['Quantity'] * option['Vega'] * (option_['Implied Vol'] - option['Implied Vol'])

                pnl_theta[date] = portfolio_theta[prev_date] * (date - prev_date).total_seconds() / 3600 / 24 / 365

                pnl_rho[date] = portfolio_rho[prev_date] * (self.rf[date] - self.rf[prev_date]) / 100

                pnl_unexplained[date] = pnl[date] - pnl_delta[date] - pnl_gamma[date] - pnl_vega[date] - pnl_theta[date] - pnl_rho[date]

                pnl_unexplained[date] = pnl_unexplained[date] - self.payoffs[date] - self.premiums[date] - self.unwinds[date] - self.delta_hedge_pnl[date]

        self.pnl = pnl
        self.pnl_unexplained = pnl_unexplained

        self.portfolio_delta = portfolio_delta
        self.portfolio_gamma = portfolio_gamma
        self.portfolio_vega = portfolio_vega
        self.portfolio_theta = portfolio_theta
        self.portfolio_rho = portfolio_rho

        self.pnl_delta = pnl_delta
        self.pnl_gamma = pnl_gamma
        self.pnl_vega = pnl_vega
        self.pnl_theta = pnl_theta
        self.pnl_rho = pnl_rho

    def create_report(self):
        self.results = pd.DataFrame(index=self.calendar)
        self.results.loc[:, "il"] = self.il

        self.results.loc[:, "mvop"] = self.mvop
        self.results.loc[:, "cash"] = self.cash
        self.results.loc[:, "premiums"] = self.premiums
        self.results.loc[:, "payoffs"] = self.payoffs
        self.results.loc[:, "unwinds"] = self.unwinds
        self.results.loc[:, "fees"] = self.fees

        self.results.loc[:, "hedging_delta"] = self.hedging_delta
        self.results.loc[:, "spot_quantity"] = self.spot_quantity
        self.results.loc[:, "delta_hedge_pnl"] = self.delta_hedge_pnl
        self.results.loc[:, "delta_fees"] = self.delta_fees

        self.results.loc[:, "spot"] = self.spot.loc[self.calendar]
        self.results.loc[:, "vix"] = self.vix.reindex(self.calendar, method='ffill')

        if self.pnl_explanation:
            self.results.loc[:, 'pnl'] = self.pnl
            self.results.loc[:, 'pnl_unexplained'] = self.pnl_unexplained

            self.results.loc[:, 'portfolio_delta'] = self.portfolio_delta
            self.results.loc[:, 'portfolio_gamma'] = self.portfolio_gamma
            self.results.loc[:, 'portfolio_vega'] = self.portfolio_vega
            self.results.loc[:, 'portfolio_theta'] = self.portfolio_theta
            self.results.loc[:, 'portfolio_rho'] = self.portfolio_rho

            self.results.loc[:, 'pnl_delta'] = self.pnl_delta
            self.results.loc[:, 'pnl_gamma'] = self.pnl_gamma
            self.results.loc[:, 'pnl_vega'] = self.pnl_vega
            self.results.loc[:, 'pnl_theta'] = self.pnl_theta
            self.results.loc[:, 'pnl_rho'] = self.pnl_rho

    def dump_results(self):
        file_path = os.path.join(r"X:\Main Folder\Pycharm Projects\mugiwara_intraday_delta_hedge\Dumps", f"Backtest Multi {datetime.now().strftime('%Y-%m-%d %H%M%S')}.xlsx")

        live_strikes = []
        live_types = []
        live_maturities = []
        live_unwinds = []
        live_quantities = []
        live_mids = []
        live_spreads = []
        index = []

        for key, options in self.live_portfolio.items():
            index.append(key)

            strikes = []
            types = []
            maturities = []
            unwinds = []
            quantities = []
            mids = []
            spreads = []

            for option in options:
                strikes.append(option["Strike"])
                types.append(option["Type"])
                maturities.append(option["Maturity"])
                unwinds.append(option["Unwind"])
                quantities.append(option["Quantity"])
                mids.append(option["Mid"])
                # spreads.append(option["Spread"])

            live_strikes.append(strikes)
            live_types.append(types)
            live_maturities.append(maturities)
            live_unwinds.append(unwinds)
            live_quantities.append(quantities)
            live_mids.append(mids)
            # live_spreads.append(spreads)

        live_strikes = pd.DataFrame(live_strikes, index=index)
        live_types = pd.DataFrame(live_types, index=index)
        live_maturities = pd.DataFrame(live_maturities, index=index)
        live_unwinds = pd.DataFrame(live_unwinds, index=index)
        live_quantities = pd.DataFrame(live_quantities, index=index)
        live_mids = pd.DataFrame(live_mids, index=index)
        # live_spreads = pd.DataFrame(live_spreads, index=index)

        def force_str(x):
            try:
                x = x.replace(tzinfo=None)
                return x.strftime('%Y-%m-%d %H:%M')
            except:
                return x

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:

            self.results = self.results.map(force_str)
            self.results.index = self.results.index.map(force_str)
            self.results.to_excel(writer, sheet_name='results', index=True)

            live_strikes = live_strikes.map(force_str)
            live_strikes.index = live_strikes.index.map(force_str)
            live_strikes.to_excel(writer, sheet_name='live_strikes', index=True)

            live_types = live_types.map(force_str)
            live_types.index = live_types.index.map(force_str)
            live_types.to_excel(writer, sheet_name='live_types', index=True)

            live_maturities = live_maturities.map(force_str)
            live_maturities.index = live_maturities.index.map(force_str)
            live_maturities.to_excel(writer, sheet_name='live_maturities', index=True)

            live_unwinds = live_unwinds.map(force_str)
            live_unwinds.index = live_unwinds.index.map(force_str)
            live_unwinds.to_excel(writer, sheet_name='live_unwinds', index=True)

            live_quantities = live_quantities.map(force_str)
            live_quantities.index = live_quantities.index.map(force_str)
            live_quantities.to_excel(writer, sheet_name='live_quantities', index=True)

            live_mids = live_mids.map(force_str)
            live_mids.index = live_mids.index.map(force_str)
            live_mids.to_excel(writer, sheet_name='live_mids', index=True)

            # live_spreads = live_spreads.map(force_str)
            # live_spreads.index = live_spreads.index.map(force_str)
            # live_spreads.to_excel(writer, sheet_name='live_spreads', index=True)

        print(f"DataFrames written to {file_path}")

    def run_backtest(self):
        self.initialize()
        self.get_historical_rf_q()
        self.build_rolling_portfolios()
        self.attach_market_data()
        self.compute_pnl()
        if self.pnl_explanation:
            self.explain_pnl()
        self.create_report()
        self.dump_results()

