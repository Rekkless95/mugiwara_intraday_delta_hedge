import pandas as pd
from dateutil import rrule
from lib.helpers.rrule_helper import (get_roll_dates, get_eligible_maturity,
                                      weekdays_rule, mondays_wednesdays_fridays_rule, fridays_rule, third_fridays_rule, third_fridays_quarterly_rule)
import os
import pytz

ny_timezone = pytz.timezone('America/New_York')
class SingleOptionRoll():
    def __init__(self, params):
        self.params = params
        self.underlying_ticker = 'QQQ'

        self.start_date = pd.to_datetime(self.params['start_date'])
        self.end_date = pd.to_datetime(self.params['end_date'])
        self.data_folder = os.path.join(r'X:\Main Folder\Options Data', self.underlying_ticker, 'Pre Processed')

        # Get spot close prices
        self.spot = pd.read_csv(r'X:\Main Folder\Options Data\QQQ\Spot\QQQ Daily.csv', index_col=0)['Close']
        self.spot.index = pd.to_datetime(self.spot.index)
        # Get VIX close level
        self.vix = pd.read_csv(r'X:\Main Folder\Options Data\QQQ\Spot\VXN Daily.csv', index_col=0)['Close']
        self.vix.index = pd.to_datetime(self.vix.index)

        self.calendar = self.get_calendar()
        self.roll_calendar = self.get_roll_calendar()

        self.live_portfolio = {}
        self.expired_portfolio = {}
        self.unwind_portfolio = {}
        self.new_options = {}

        self.mvop = {}
        self.cash = {}
        self.fees = {}
        self.il = {}

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

    def get_calendar(self):
        calendar = pd.read_csv(r'X:\Main Folder\Options Data\QQQ\calendar.csv')['Index'].values
        calendar = pd.to_datetime(calendar)
        calendar = calendar[(calendar >= self.start_date) & (calendar <= self.end_date)]
        return calendar.to_list()

    def get_roll_calendar(self):
        return get_roll_dates(self.params['roll_days'], self.calendar)

    def get_target_maturity(self, date):
        return get_eligible_maturity(self.params['eligible_maturities'], date, self.params['maturity_number'])

    def get_unwind_date(self, date):
        return get_eligible_maturity(self.params['unwind_dates'], date, self.params['holding_period'])

    def get_option_data(self, date):
        file_name = f'{self.underlying_ticker.lower()}_eod_{date.strftime('%Y%m')}.csv'
        file_path = os.path.join(self.data_folder, file_name)
        data = pd.read_csv(file_path, index_col=0)
        data.index = pd.to_datetime(data.index)
        data = data.loc[date]
        data['Maturity'] = pd.to_datetime(data['Maturity'])
        data.loc[:, 'Strike'] = pd.to_numeric(data['Strike'], errors='coerce')
        data.loc[:, 'Bid'] = pd.to_numeric(data['Bid'], errors='coerce')
        data.loc[:, 'Ask'] = pd.to_numeric(data['Ask'], errors='coerce')
        data.loc[:, 'Volume'] = pd.to_numeric(data['Volume'], errors='coerce')
        data.loc[:, 'Implied Vol'] = pd.to_numeric(data['Implied Vol'], errors='coerce')
        data.loc[:, 'Delta'] = pd.to_numeric(data['Delta'], errors='coerce')
        data.loc[:, 'Gamma'] = pd.to_numeric(data['Gamma'], errors='coerce')
        data.loc[:, 'Vega'] = pd.to_numeric(data['Vega'], errors='coerce')
        data.loc[:, 'Theta'] = pd.to_numeric(data['Theta'], errors='coerce')
        data.loc[:, 'Rho'] = pd.to_numeric(data['Rho'], errors='coerce')
        data.loc[:, 'Type'] = pd.to_numeric(data['Type'], errors='coerce')
        data.loc[:, 'Mid'] = (data.loc[:, 'Bid'] + data.loc[:, 'Ask']) / 2
        data.loc[:, 'Spread'] = data.loc[:, 'Ask'] - data.loc[:, 'Bid']
        return data

    def get_new_options(self, date):
        new_options = []
        # get today's option data and spot close
        spot = self.spot.loc[ny_timezone.localize(date)]
        data = self.get_option_data(date)
        # select closest higher or equal maturity available
        maturity = data['Maturity'].loc[(data['Maturity'] >= self.get_target_maturity(date)) & (data['Type'] == self.type)].min()
        data = data.loc[(data['Maturity'] == maturity) & (data['Type'] == self.type)]
        if self.type == 0:
            strike = data['Strike'].loc[data['Strike'] <= spot * self.moneyness].max()
        else:
            strike = data['Strike'].loc[data['Strike'] >= spot * self.moneyness].min()
        # Select Option
        option = {'Maturity' : maturity,
                  'Unwind' : self.get_unwind_date(date),
                  'Strike' : strike,
                  'Type' : self.type,
                  'Quantity' : 1}
        new_options.append(option)
        return new_options

    def build_rolling_portfolios(self):
        for ix, date in enumerate(self.calendar):
            # select the correct previous date and previous portfolio
            if ix == 0:
                prev_date = date
                prev_portfolio = {}
            else:
                prev_date = self.calendar[ix-1]
                prev_portfolio = self.live_portfolio[prev_date]

            # Keep only live options today
            live_portfolio = [option for option in prev_portfolio if (option['Maturity'] > date and option['Unwind'] > date)]
            # Update with added options
            self.new_options[date] = []
            if date in self.roll_calendar:
                new_options = self.get_new_options(date)
                self.new_options[date] = new_options
                live_portfolio.extend(new_options)
            # Add live portfolio for today
            self.live_portfolio[date] = live_portfolio
            # Add expired options portfolio for today
            self.expired_portfolio[date] = [option for option in prev_portfolio if (option['Maturity'] <= date)]
            # Add unwind options portfolio for today
            self.unwind_portfolio[date] = [option for option in prev_portfolio if (option['Maturity'] > date >= option['Unwind'])]

    def attach_market_data(self):
        for date in self.calendar:
            data = self.get_option_data(date)
            for option in self.live_portfolio[date]:
                option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
            for option in self.expired_portfolio[date]:
                option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]
            for option in self.unwind_portfolio[date]:
                option_data = data.loc[(data['Strike'] == option['Strike']) & (data['Type'] == option['Type']) & (data['Maturity'] == option['Maturity'])]
                option['Mid'], option['Spread'], option['Implied Vol'], option['Delta'], option['Gamma'], option['Vega'], option['Theta'], option['Rho'] = option_data[['Mid', 'Spread', 'Implied Vol', 'Delta', 'Gamma', 'Vega', 'Theta', 'Rho']].iloc[0]

    def compute_pnl(self):
        mvop = {}
        cash = {}
        fees = {}
        il = {}
        for ix, date in enumerate(self.calendar):
            # MVOP is equal to the new valuation at mid of current live options
            mvop[date] = sum([option['Mid'] * option['Quantity'] for option in self.live_portfolio[date]])
            # Initialize cash = Notional or previous cash level
            if date == self.calendar[0]:
                cash[date] = self.notional
            else:
                prev_date = self.calendar[ix-1]
                cash[date] = cash[prev_date]
            # Adjust cash balance to latest options added removed at Mid
            cash[date] = (cash[date]
                          - sum([option['Mid'] * option['Quantity'] for option in self.new_options[date]])
                          + sum([option['Mid'] * option['Quantity'] for option in self.unwind_portfolio[date]])
                          + sum([option['Mid'] * option['Quantity'] for option in self.expired_portfolio[date]]))
            # Compute Daily Execution fees
            fees[date] = (sum([option['Spread'] * abs(option['Quantity']) for option in self.new_options[date]])
                          + sum([option['Spread'] * abs(option['Quantity']) for option in self.unwind_portfolio[date]])
                          + sum([option['Spread'] * abs(option['Quantity']) for option in self.expired_portfolio[date]]))
            # Remove execution fees from cash balance
            cash[date] = cash[date] - fees[date]
            # Compute Index Level
            il[date] = cash[date] + mvop[date]

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
                prev_date = self.calendar[ix-1]
                pnl[date] = self.il[date] - self.il[prev_date]

                portfolio_delta[date] = sum([option['Delta'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_gamma[date] = sum([option['Gamma'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_vega[date] = sum([option['Vega'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_theta[date] = sum([option['Theta'] * option['Quantity'] for option in self.live_portfolio[prev_date]])
                portfolio_rho[date] = sum([option['Rho'] * option['Quantity'] for option in self.live_portfolio[prev_date]])



                pnl_delta[date] = portfolio_delta[date] * (self.spot[ny_timezone.localize(date)] - self.spot[ny_timezone.localize(prev_date)])
                pnl_gamma[date] = 0.5 * portfolio_gamma[date] * ((self.spot[ny_timezone.localize(date)] - self.spot[ny_timezone.localize(prev_date)]) ** 2)
                pnl_vega[date] = portfolio_vega[date] * (self.vix[ny_timezone.localize(date)] - self.vix[ny_timezone.localize(prev_date)])
                pnl_theta[date] = portfolio_theta[date] * 1
                pnl_rho[date] = 0

                pnl_unexplained[date] = pnl[date] - self.fees[date] - pnl_delta[date] - pnl_gamma[date] - pnl_vega[date] - pnl_theta[date] - pnl_rho[date]

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



params = {
    'start_date': '2023-01-01',
    'end_date': '2023-01-30',
    'underlying_ticker': 'QQQ',
    'notional': 1000,

    'roll_days':  fridays_rule,
    'eligible_maturities':  fridays_rule,
    'maturity_number': 2,
    'unwind_dates':  fridays_rule,
    'holding_period': 1,
    'type': 1,
    'moneyness': 1,
    'leverage': 1,


}

bt = SingleOptionRoll(params)

bt.build_rolling_portfolios()
bt.attach_market_data()
bt.compute_pnl()
bt.explain_pnl()
print('end')



