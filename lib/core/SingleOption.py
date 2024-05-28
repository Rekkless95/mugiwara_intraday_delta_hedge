import pandas as pd
from dateutil import rrule
from lib.helpers.rrule_helper import (get_roll_dates, get_eligible_maturity, weekdays_rule, mondays_wednesdays_fridays_rule, fridays_rule, third_fridays_rule, third_fridays_quarterly_rule)
import os
import pytz
import copy

ny_timezone = pytz.timezone('America/New_York')
PATH_SPOT = r'X:\Main Folder\Data\Spot'
DATAPATH = r"X:\Main Folder\Data"


# ny_timezone.localize(date)

class SingleOptionRoll():
    def __init__(self, params):
        self.params = params
        self.underlying_ticker = self.notional = self.params['underlying_ticker']

        self.start_date = pd.to_datetime(self.params['start_date']).tz_localize(ny_timezone) + pd.Timedelta(hours=16)
        self.end_date = pd.to_datetime(self.params['end_date']).tz_localize(ny_timezone) + pd.Timedelta(hours=16)
        self.data_folder = os.path.join(DATAPATH, 'Options', self.underlying_ticker, 'Pre Processed')

        # Get spot close prices
        self.spot = pd.read_csv(os.path.join(DATAPATH, 'Spot', f"{self.underlying_ticker} Daily.csv"), index_col=0)['Close']
        self.spot.index = pd.to_datetime(self.spot.index)

        # Get VIX close level
        self.vol_ticker = params.get("vol_ticker", 'VIX')
        self.vix = pd.read_csv(os.path.join(DATAPATH, 'Spot', f"{self.vol_ticker} Daily.csv"), index_col=0)['Close']
        self.vix.index = pd.to_datetime(self.vix.index)

        self.calendar = self.get_calendar()
        self.roll_calendar = self.get_roll_calendar()

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

        self.type = self.params['type']
        self.moneyness = self.params['moneyness']

        self.notional = self.params['notional']
        self.leverage = self.params['leverage']

        self.delta_hedge = self.params.get('delta_hedge', False)
        self.delta_fees_bps = self.params.get('delta_fees_bps', False) / 10000

        self.pnl_explanation = self.params.get('pnl_explanation', False)
        self.implied_vol_for_vega = self.params.get('implied_vol_for_vega', False)

    def get_calendar(self):
        calendar = pd.read_csv(os.path.join(DATAPATH, 'Options', self.underlying_ticker, f"calendar.csv"))['Index'].values
        calendar = pd.to_datetime(calendar)
        calendar = calendar[(calendar >= self.start_date) & (calendar <= self.end_date)]
        return calendar.to_list()

    def get_roll_calendar(self):
        return get_roll_dates(self.params['roll_days'], self.calendar)

    def get_target_maturity(self, date):
        return get_eligible_maturity(self.params['eligible_maturities'], date, self.params['maturity_number'])

    def get_unwind_date(self, date):
        return get_eligible_maturity(self.params['unwind_dates'], date, self.params['holding_period'])

    def get_quantity(self, date):
        return self.leverage * self.notional / self.spot.loc[date]

    def get_new_options(self, date):
        new_options = []
        # get today's option data and spot close
        spot = self.spot.loc[date]
        data = self.get_option_data(date)
        # select closest higher or equal maturity available
        maturity = data['Maturity'].loc[(data['Maturity'].apply(lambda x: x.date()) >= self.get_target_maturity(date).date()) & (data['Type'] == self.type)].min()
        data = data.loc[(data['Maturity'] == maturity) & (data['Type'] == self.type)]
        if self.type == 0:
            strike = data['Strike'].loc[data['Strike'] <= spot * self.moneyness].max()
        else:
            strike = data['Strike'].loc[data['Strike'] >= spot * self.moneyness].min()
        # Select Option
        option = {'Maturity': maturity,
                  'Unwind': self.get_unwind_date(date),
                  'Strike': strike,
                  'Type': self.type,
                  'Quantity': self.get_quantity(date)}

        new_options.append(option)
        return new_options

    def build_rolling_portfolios(self):
        for ix, date in enumerate(self.calendar):
            # select the correct previous date and previous portfolio
            if ix == 0:
                prev_date = date
                prev_portfolio = {}
            else:
                prev_date = self.calendar[ix - 1]
                prev_portfolio = self.live_portfolio[prev_date]

            # Keep only live options today
            live_portfolio = [option for option in copy.deepcopy(prev_portfolio) if (option['Maturity'] > date and option['Unwind'] > date)]
            # Update with added options
            self.new_options[date] = []
            if date in self.roll_calendar:
                new_options = self.get_new_options(date)
                self.new_options[date] = new_options
                live_portfolio.extend(new_options)
            # Add live portfolio for today
            self.live_portfolio[date] = live_portfolio
            # Add expired options portfolio for today
            self.expired_portfolio[date] = [option for option in copy.deepcopy(prev_portfolio) if (option['Maturity'] <= date)]
            # Add unwind options portfolio for today
            self.unwind_portfolio[date] = [option for option in copy.deepcopy(prev_portfolio) if (option['Maturity'] > date >= option['Unwind'])]

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
            self.opened_data_file.index = self.opened_data_file.index.tz_localize(ny_timezone)
            # Reformat Maturity to close NY
            self.opened_data_file.loc[:, 'Maturity'] = pd.to_datetime(self.opened_data_file['Maturity']).apply(lambda x: x.normalize().tz_localize(ny_timezone) + pd.Timedelta(hours=16))
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

        return self.opened_data_file.loc[date]

    def attach_market_data(self):
        for date in self.calendar:
            data = self.get_option_data(date)

            options = []
            for option in self.live_portfolio[date]:
                option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = \
                    option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
                options.append(option)
            self.live_portfolio[date] = options

            options = []
            for option in self.expired_portfolio[date]:
                option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = \
                    option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
                options.append(option)
            self.expired_portfolio[date] = options

            options = []
            for option in self.unwind_portfolio[date]:
                option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = \
                    option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
                options.append(option)
            self.unwind_portfolio[date] = options

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

        for ix, date in enumerate(self.calendar):
            # MVOP is equal to the new valuation at mid of current live options
            mvop[date] = sum([option['Mid'] * option['Quantity'] for option in self.live_portfolio[date]])

            # Initialize cash = Notional or previous cash level
            if date == self.calendar[0]:
                prev_date = date
                cash[date] = self.notional
                spot_quantity[date] = 0
            else:
                prev_date = self.calendar[ix - 1]
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
            hedging_delta[date] = sum([option['Delta'] * option['Quantity'] for option in self.live_portfolio[date]])  # Current delta of the options portfolio
            delta_hedge_pnl[date] = spot_quantity[prev_date] * (self.spot[date] - self.spot[prev_date])  # Delta hedging PnL from the spot portfolio
            if self.delta_hedge:
                spot_quantity[date] = hedging_delta[date] - spot_quantity[prev_date]
            else:
                spot_quantity[date] = 0

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

        weighted_implied_vol = {}

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

                weighted_implied_vol[date] = 0

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

                portfolio_delta[date] = sum([option['Delta'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_gamma[date] = sum([option['Gamma'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_vega[date] = sum([option['Vega'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_theta[date] = sum([option['Theta'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_rho[date] = sum([option['Rho'] * option['Quantity'] for option in self.live_portfolio[prev_date]])

                if sum(option['Quantity'] for option in self.live_portfolio[prev_date]) != 0:
                    weighted_implied_vol[date] = sum(option['Implied Vol'] * option['Quantity'] for option in self.live_portfolio[prev_date]) / sum(option['Quantity'] for option in self.live_portfolio[prev_date])
                else:
                    weighted_implied_vol[date] = 0

                pnl_delta[date] = portfolio_delta[prev_date] * (self.spot.loc[date] - self.spot.loc[prev_date])

                pnl_gamma[date] = 0.5 * portfolio_gamma[prev_date] * ((self.spot.loc[date] - self.spot.loc[prev_date]) ** 2)

                if self.implied_vol_for_vega:
                    pnl_vega[date] = portfolio_vega[prev_date] * (weighted_implied_vol[date] - weighted_implied_vol[prev_date])
                else:
                    pnl_vega[date] = portfolio_vega[prev_date] * (self.vix.loc[date] - self.vix.loc[prev_date])

                pnl_theta[date] = portfolio_theta[prev_date] * (date - prev_date).total_seconds() / 3600 / 24

                pnl_rho[date] = 0

                pnl_unexplained[date] = pnl[date] - self.fees[date] - pnl_delta[date] - pnl_gamma[date] - pnl_vega[date] - pnl_theta[date] - pnl_rho[date]

        self.pnl = pnl
        self.pnl_unexplained = pnl_unexplained

        self.weighted_implied_vol = weighted_implied_vol

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
        self.results.loc[:, "vix"] = self.vix.loc[self.calendar]

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

    def run_backtest(self):
        self.build_rolling_portfolios()
        self.attach_market_data()
        self.compute_pnl()
        if self.pnl_explanation:
            self.explain_pnl()
        self.create_report()


