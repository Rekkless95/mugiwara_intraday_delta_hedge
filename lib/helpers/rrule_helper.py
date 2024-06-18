from dateutil import rrule
import pytz
ny_tz = pytz.timezone('America/New_York')

def get_roll_dates(rules, calendar, close_time='16:00'):
    # Extract rule parameters
    freq = rules['freq']
    byday = rules.get('byday', None)
    bymonth = rules.get('bymonth', None)
    bysetpos = rules.get('bysetpos', None)

    # Define the start and end date for the calendar
    start_date = min(calendar)
    end_date = max(calendar)

    # Generate the recurring rule based on the provided parameters
    recurrence_rule = rrule.rrule(
        freq=freq,
        byweekday=byday,
        bymonth=bymonth,
        bysetpos=bysetpos,
        dtstart=start_date,
        until=end_date
    )
    recurrence_rule = [dt.astimezone(ny_tz).replace(hour=int(close_time.split(":")[0]), minute=int(close_time.split(":")[1])) for dt in recurrence_rule]
    # Filter the calendar based on the recurrence rule
    filtered_dates = [dt for dt in calendar if dt in recurrence_rule]

    return filtered_dates


def get_eligible_maturity(rules, start_date, count):
    # Extract rule parameters
    freq = rules['freq']
    byday = rules.get('byday', None)
    bymonth = rules.get('bymonth', None)
    bysetpos = rules.get('bysetpos', None)

    # Generate the recurring rule based on the provided parameters
    recurrence_rule = rrule.rrule(
        freq=freq,
        byweekday=byday,
        bymonth=bymonth,
        bysetpos=bysetpos,
        dtstart=start_date,
        count=count + 1
    )
    return recurrence_rule[count]

# Define rules
weekdays_rule = {
    'freq': rrule.DAILY,
    'byday': [rrule.MO, rrule.TU, rrule.WE, rrule.TH, rrule.FR]
}

mondays_wednesdays_fridays_rule = {
    'freq': rrule.DAILY,
    'byday': [rrule.MO, rrule.WE, rrule.FR]
}

fridays_rule = {
    'freq': rrule.DAILY,
    'byday': [rrule.FR]
}


third_fridays_rule = {
    'freq': rrule.MONTHLY,
    'byday': rrule.FR,
    'bysetpos': 3
}

third_fridays_quarterly_rule = {
    'freq': rrule.MONTHLY,
    'bymonth': [3, 6, 9, 12],  # June and December
    'byday': rrule.FR,
    'bysetpos': 3
}