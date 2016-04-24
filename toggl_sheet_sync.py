#!/usr/bin/env python

import argparse
from datetime import datetime, date
import toggl
import logging
import json
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

service_scope = ['https://spreadsheets.google.com/feeds']

UTC = pytz.timezone("UTC")
localtz = toggl.DateAndTime().tz

def fd(d):
    """formats a date in the local timezone in isoformat"""
    d = localtz.localize(d)
    return d.isoformat()

def get_entries(date_start, date_end, client):
    for toggl_entry in toggl.TimeEntryList():
        print(toggl_entry)

def get_or_add_worksheet(spreadsheet, sheet_name, rows=1000, cols=20):
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(sheet_name, rows, cols)

def setup_sheets(spreadsheet, year):
    today = datetime.today()
    months = range(1, today.month+1 if today.year == year else 13)
    for month in months:
        d = date(year=year, month=month, day=1)
        month_sheet = get_or_add_worksheet(spreadsheet, d.strftime("%b"))
    summary_sheet = get_or_add_worksheet(spreadsheet, "Summary")
    
def main():
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--credentials', action='store', type=str, help='Credentials filename')
    this_year = datetime.today().year
    parser.add_argument('--year', action='store', type=int, help='Year to store in spreadsheet', default=this_year)
    parser.add_argument('spreadsheet', help='URL of Google Sheet to edit')
    options = parser.parse_args()
    credentials = ServiceAccountCredentials.from_json_keyfile_name(options.credentials, service_scope)
    c = gspread.authorize(credentials)
    spreadsheet = c.open_by_url(options.spreadsheet)
    setup_sheets(spreadsheet, options.year)
    get_entries(None, None, None)


if __name__ == '__main__':
    main()

