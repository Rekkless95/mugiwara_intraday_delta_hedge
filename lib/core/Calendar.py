import doubledate

class Calendar:
    def __init__(self, start_date, end_date, calendar='XNYS'):
        self.start_date = start_date
        self.end_date = end_date
        self.calendar = calendar
        self.holidays = None
        self.cdr = None

    def create_calendar_object(self):
        # Create full Calendar
        cdr = doubledate.Calendar.create(starting=self.start_date, ending=self.end_date, freq="D")
        # Remove weekends
        cdr = doubledate.Calendar(cdr).weekdays()
        # # Remove holidays
        # holidays_np64 = mcal.get_calendar(self.calendar).holidays().holidays
        # self.holidays = [date.astype('datetime64[s]').astype(datetime) for date in holidays_np64]
        # # Convert back to doubledate.Calendar
        # cdr = doubledate.Calendar(cdr).difference(self.holidays)


        self.cdr = cdr





