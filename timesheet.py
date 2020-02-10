import argparse
import csv
from datetime import date
import json
from pathlib import Path
import pickle
import sys

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


CONFIG_DIR = Path(__file__).parent / 'config'
CREDENTIALS_FILE = CONFIG_DIR / 'credentials.json'
TOKEN_FILE = CONFIG_DIR / 'token.pickle'
CONSTANTS_FILE = CONFIG_DIR / 'constants.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


try:
    with CONSTANTS_FILE.open() as fp:
        constants = json.load(fp)
except FileNotFoundError:
    constants = {}

def get_constant(key):
    try:
        return constants[key]
    except KeyError:
        raise ImproperlyConfigured(f'Constant {key.__repr__()} is not defined in {CONSTANTS_FILE}')


NAME = get_constant('NAME')
HOURLY_RATE = get_constant('HOURLY_RATE')
SPREADSHEET_ID = get_constant('SPREADSHEET_ID')
DATA_RANGE = get_constant('DATA_RANGE')


def parse_args():
    parser = argparse.ArgumentParser(
        description='Make MSP invoice by pulling data and formatting data from Google Sheets'
    )
    parser.add_argument('year', type=int, help='4-digit year')
    parser.add_argument('month', type=int, choices=range(1,13), help='month (1–12)')
    parser.add_argument('period', type=int, choices=[1,2], help='period (1 or 2)')
    parser.add_argument('--pto', type=int, help='number of hours of PTO', default=0)
    # parser.add_argument('--save', dest='save', action='store_true', help='save raw data and completed report to files')
    # parser.set_defaults(save=False)
    return parser.parse_args()


def get_service():
    credentials = None
    if TOKEN_FILE.exists():
        with TOKEN_FILE.open(mode='rb') as token:
            credentials = pickle.load(token)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            credentials = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with TOKEN_FILE.open(mode='wb') as token:
            pickle.dump(credentials, token)
    return build('sheets', 'v4', credentials=credentials)


# ensure spreadsheet row has all 5 coordinates filled out
def is_complete(row):
    return len(row) >= 5 and all(row[:5])


def pay(hours):
    return f'{hours * HOURLY_RATE:.2f}'


class WorkEvent:
    '''
    Bag of data for a single clock-in/clock-out event.
    Input to constructor is a list (of strings):
        [date (YYYY-MM-DD), time in, time out, duration (hrs), project, class, note]
    '''
    def __init__(self, event):
        self.date = date.fromisoformat(event[0])
        self.duration = float(event[3])
        self.project = event[4]
        self._class = event[5]


class PayPeriod:
    '''
    Used to determine whether work events should be counted in the timesheet
    '''
    def __init__(self, year, month, period):
        self.year = int(year)
        self.month = int(month)
        self.period = int(period)
        self.fake_date = date(self.year, self.month, 1)  # for formatting purposes

    def __contains__(self, work_event):
        if work_event.date.year != self.year or work_event.date.month != self.month:
            return False
        if self.period == 1:
            return work_event.date.day <= 15
        else:
            return work_event.date.day > 15

    def __repr__(self):
        return f'{self.fake_date:%Y-%m}-{self.period}'  # eg 2020-02-2

    def fancy_repr(self):
        return f'{self.fake_date:%B %Y} pay period {self.period}'  # eg February 2020


def daily_report(date, work_events):
    date_string = f'{date:%m/%d/%y}'
    daily_events = [event for event in work_events if event.date == date]
    daily_hours = sum([event.duration for event in daily_events])
    hours_string = f'{daily_hours:.2f}'
    daily_pay = pay(daily_hours)
    return [date_string, hours_string, daily_pay]


def project_report(project, _class, work_events):
    proj_class_events = [event for event in work_events
                         if event.project == project
                         and event._class == _class]
    proj_class_hours = sum([event.duration for event in proj_class_events])
    hours_string = f'{proj_class_hours:.2f}'
    proj_class_pay = pay(proj_class_hours)
    return [project, _class, hours_string, proj_class_pay]


def report(pay_period, raw_data, pto):
    work_events = [WorkEvent(row) for row in raw_data if is_complete(row)]
    work_events = [event for event in work_events if event in pay_period]

    # report by days
    work_days = [event.date for event in work_events]
    work_days = list(set(work_days))
    work_days.sort()
    daily_reports = [daily_report(date, work_events) for date in work_days]

    # report by projects
    proj_classes = [(event.project, event._class) for event in work_events]
    proj_classes = list(set(proj_classes))
    project_reports = [project_report(project, _class, work_events)
                       for project, _class in proj_classes]

    # summary (including PTO)
    headers = [
        [f'Timesheet for {NAME}'],
        [pay_period.fancy_repr()],
    ]
    pto_report = []
    if pto:
        pto_report = [
            [f'{args.pto} hours of PTO used this pay period'],
            [],
        ]
    total_hours = sum([event.duration for event in work_events]) + pto
    total_pay = pay(total_hours)
    summary = f'Total: {total_hours:.2f}hrs ✕ ${HOURLY_RATE}/hr = ${total_pay}'

    return [
        *headers,
        [],
        ['Date', 'Hours', 'Amount'],
        *daily_reports,
        [],
        ['Project', 'Class', 'Hours', 'Amount'],
        *project_reports,
        [],
        *pto_report,
        [summary],
    ]


def main():
    args = parse_args()
    pay_period = PayPeriod(args.year, args.month, args.period)

    service = get_service()
    sheet = service.spreadsheets()
    response = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=DATA_RANGE,
        majorDimension='ROWS'
    ).execute()
    raw_data = response['values']  # may contain incomplete entries and entries not in the pay period

    final_report = report(pay_period, raw_data, args.pto)

    writer = csv.writer(sys.stdout, lineterminator='\n')
    writer.writerows(final_report)


if __name__ == '__main__':
    main()
