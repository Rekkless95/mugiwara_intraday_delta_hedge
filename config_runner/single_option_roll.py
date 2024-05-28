import pandas as pd

from lib.core.SingleOption import SingleOptionRoll
from lib.helpers.rrule_helper import (get_roll_dates, get_eligible_maturity, weekdays_rule, mondays_wednesdays_fridays_rule, fridays_rule, third_fridays_rule, third_fridays_quarterly_rule)

if __name__ == '__main__':
    params = {
        'start_date': '2023-01-01',
        'end_date': '2023-01-30',
        'underlying_ticker': 'QQQ',
        'vol_ticker': 'VXN',
        'notional': 1000,

        'roll_days': fridays_rule,
        'eligible_maturities': fridays_rule,
        'maturity_number': 2,
        'unwind_dates': fridays_rule,
        'holding_period': 2,

        'type': 0,
        'moneyness': 0.98,
        'leverage': -1,

        'delta_hedge': False,
        'delta_fees_bps': 10,

        'pnl_explanation': False,
        'implied_vol_for_vega': False,

    }

    bt = SingleOptionRoll(params)

    bt.run_backtest()

    bt.results.to_clipboard()

    print('end')
