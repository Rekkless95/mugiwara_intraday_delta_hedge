import pandas as pd

from lib.core.MultiOptions import MultiOptionsRoll
from lib.helpers.rrule_helper import (get_roll_dates, get_eligible_maturity, weekdays_rule, mondays_wednesdays_fridays_rule, fridays_rule, third_fridays_rule, third_fridays_quarterly_rule)

if __name__ == '__main__':
    params = {
        'start_date': '2022-01-01',
        'end_date': '2023-12-30',
        'underlying_ticker': 'QQQ',
        'vol_ticker': 'VXN',
        'notional': 100,
        'legs':
            [{'roll_days': weekdays_rule,
              'eligible_maturities': weekdays_rule,
              'maturity_number': 1,
              'unwind_dates': weekdays_rule,
              'holding_period': 1,
              'type': 0,
              # 'moneyness': 0.98,
              'delta_strike': 0.05,
              'leverage': -2.5,
              'delta_hedging_time': ['16:00']},
             ],

        'delta_fees_bps': 5,
        'close_time': '16:00',
        'pnl_explanation': True,

    }

    bt = MultiOptionsRoll(params)

    bt.run_backtest()

    bt.results.to_clipboard()

    print('end')
