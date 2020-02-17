import argparse
import csv
from datetime import date
import json
import logging
from pathlib import Path
import pickle
import re
import sys
import traceback

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


class ImproperlyConfigured(Exception):
    pass


# defines what operations you're allowed to do to the google doc
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

CONFIG_DIR = Path(__file__).parent / 'config'
CONFIG_FILE = CONFIG_DIR / 'config.json'
CREDENTIALS_FILE = CONFIG_DIR / 'credentials.json'
TOKEN_FILE = CONFIG_DIR / 'token.pickle'

with CONFIG_FILE.open() as fp:
    constants = json.load(fp)


def get_constant(key):
    try:
        return constants[key]
    except KeyError:
        raise ImproperlyConfigured(f'Constant {key.__repr__()} is not defined in {CONFIG_FILE}')


NAME = get_constant('NAME')
HOURLY_RATE = get_constant('HOURLY_RATE')
SPREADSHEET_ID = get_constant('SPREADSHEET_ID')
DATA_RANGE = get_constant('DATA_RANGE')

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description='Make MSP timesheet by fetching and formatting data from Google Sheets'
    )
    parser.add_argument('year', type=int, help='4-digit year')
    parser.add_argument('month', type=int, choices=range(1,13), help='month (1–12)')
    parser.add_argument('period', type=int, choices=[1,2], help='period (1 or 2)')
    parser.add_argument('--pto', type=int, help='number of hours of paid time off taken this pay cycle', default=0)
    parser.add_argument('--save', dest='save', action='store_true', help='save raw data and completed report to files')
    parser.set_defaults(save=False)
    return parser.parse_args()


def get_raw_data():
    '''
    Fetches raw work data from Google spreadsheet.

    Result may contain incomplete entries and entries not in the pay period.
    The final report is built from this. The raw data may also be saved for
    forensic purposes if the --save option is invoked.
    '''
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

    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    response = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=DATA_RANGE,
        majorDimension='ROWS'
    ).execute()
    return response['values']


# ensure spreadsheet row has all 5 coordinates filled out
def is_complete(row):
    return len(row) >= 5 and all(row[:5])


def pay(hours):
    return f'{hours * HOURLY_RATE:.2f}'


class WorkEvent:
    '''
    Bag of data for a single clock-in/clock-out event.

    Input to constructor is a list (of strings) from a spreadsheet row:
        [date (YYYY-MM-DD), time in, time out, duration (hrs), project, class, note]
    We don't care about time in/time out because the spreadsheet already calculates
    the duration of the work event.
    '''
    def __init__(self, event):
        self.date = date.fromisoformat(event[0])
        self.duration = float(event[3])
        self.project = event[4]
        self._class = event[5]


class PayPeriod:
    '''
    Used to determine whether work events should be counted in the timesheet
    and to do some formatting.
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

    def __repr__(self):  # eg 2020-02-2
        return f'{self.fake_date:%Y-%m}-{self.period}'

    def fancy_repr(self):  # eg February 2020 pay period 2
        return f'{self.fake_date:%B %Y} pay period {self.period}'


def _daily_report(date, work_events):
    date_string = f'{date:%m/%d/%Y}'
    daily_events = [event for event in work_events if event.date == date]
    daily_hours = sum([event.duration for event in daily_events])
    hours_string = f'{daily_hours:.2f}'
    daily_pay = pay(daily_hours)
    return [date_string, hours_string, daily_pay]


def _project_report(project, _class, work_events):
    proj_class_events = [event for event in work_events
                         if event.project == project
                         and event._class == _class]
    proj_class_hours = sum([event.duration for event in proj_class_events])
    hours_string = f'{proj_class_hours:.2f}'
    proj_class_pay = pay(proj_class_hours)
    return [project, _class, hours_string, proj_class_pay]


def _pto_report(hours):
    if hours:
        return [
            [f'{hours} hours of PTO used this pay period'],
            [],
        ]
    return []


def report(pay_period, raw_data, pto):
    work_events = [WorkEvent(row) for row in raw_data if is_complete(row)]
    work_events = [event for event in work_events if event in pay_period]

    # report by days
    work_days = [event.date for event in work_events]
    work_days = list(set(work_days))
    work_days.sort()
    daily_reports = [_daily_report(date, work_events) for date in work_days]

    # report by projects
    proj_classes = [(event.project, event._class) for event in work_events]
    proj_classes = list(set(proj_classes))
    project_reports = [_project_report(project, _class, work_events)
                       for project, _class in proj_classes]

    # summary (including PTO)
    headers = [
        [f'Timesheet for {NAME}'],
        [pay_period.fancy_repr()],
    ]
    pto_report = _pto_report(pto)
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


def _check_dirs():
    timesheet_dir = get_constant('TIMESHEET_DIR')
    timesheet_dir = Path(timesheet_dir)
    raw_data_dir = timesheet_dir / 'raw'
    for dir in [timesheet_dir, raw_data_dir]:
        if not dir.exists():
            raise FileNotFoundError(f"Directory '{dir}' not found. Create it before saving timesheets.")
    return timesheet_dir, raw_data_dir


def save(pay_period, report, raw_data):
    timesheet_dir, raw_data_dir = _check_dirs()
    clean_name = re.sub('\W', '_', NAME)  # \W matches non-word characters

    timesheet_file = timesheet_dir / f'{pay_period}_{clean_name}.csv'
    with timesheet_file.open(mode='w') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerows(report)

    raw_data_file = raw_data_dir / f'{pay_period}_raw.csv'
    with raw_data_file.open(mode='w') as csvfile:
        writer = csv.writer(csvfile, lineterminator='\n')
        writer.writerows(raw_data)


def main():
    args = parse_args()
    pay_period = PayPeriod(args.year, args.month, args.period)
    raw_data = get_raw_data()
    final_report = report(pay_period, raw_data, args.pto)

    if args.save:
        save(pay_period, final_report, raw_data)

    writer = csv.writer(sys.stdout, lineterminator='\n')
    writer.writerows(final_report)

    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        logger.error(traceback.format_exc())
        sys.exit(1)
