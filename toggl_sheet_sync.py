#!/usr/bin/env python

import argparse
from datetime import datetime, date, timedelta
import dateutil.parser
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

def get_entries(start_date, end_date, client=None):
    finished = False
    start_search, end_search = start_date, end_date
    # TODO: This is a manual way to search through time entry list pages. Switch to reports API rather
    if client:
        projects = toggl.ProjectList()
        valid_project_ids = {project['id'] for project in projects if project.get('cid', None) == client['id']}
    while not finished:
        search_list = toggl.TimeEntryList(start_search, end_search)
        max_date = None
        for toggl_entry in search_list:
            entry_end = toggl_entry.get('end')
            if entry_end > max_date:
                max_date = entry_end
            if not client or toggl_entry.get('pid') in valid_project_ids:
                yield toggl_entry
        if len(search_list.time_entries) < 1000:
            finished = True
        else:
            start_search = max_date + timedelta(seconds=1)

def get_or_add_worksheet(spreadsheet, sheet_name, rows=1000, cols=20):
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(sheet_name, rows, cols)

SHEET_HEADERS = ['Date', 'toggl_id', 'Start', 'End', 'Project', 'Description', 'Duration']
SUMMARY_HEADERS = ['Period', 'Days Worked', 'Total Hours']

def format_time(t):
    s = t.strftime("%H:%M").lstrip('0')
    return '0' + s if s.startswith(':') else s

def entry_to_sheet_row(entry):
    start_time = dateutil.parser.parse(entry.get('start')) if entry.get('start') else None
    end_time = dateutil.parser.parse(entry.get('stop')) if entry.get('stop') else None
    start_time = localtz.fromutc(start_time.replace(tzinfo=None))
    end_time = localtz.fromutc(end_time.replace(tzinfo=None))
    duration = end_time - start_time
    duration = duration.days * 86400 + duration.seconds
    if duration != entry.get('duration'):
        raise ValueError("Error checking duration: Calculated %r not %r", duration, entry.get('duration'))
    project = toggl.ProjectList().find_by_id(entry.get('pid')) if entry.get('pid') else None
    return {
        'Date': start_time.strftime('%Y-%m-%d'),
        'toggl_id': entry.get('id'),
        'Start': format_time(start_time),
        'End': format_time(end_time),
        'Project': "'" + project.get('name') if project else None,
        'Description': "'" + entry.get('description'),
        'Duration': '%d:%02d' % divmod(duration//60, 60)
    }

def cell_name(row, col):
    col_name = chr(ord('A') + col-1) if col <= 26 else (chr(ord('A') + (col-1)/26) + chr(ord('A') + (col-1) % 26))
    return "%s%d" % (col_name, row)

def setup_header(worksheet, headers=SHEET_HEADERS):
    header_cells = []
    header_row = worksheet.row_values(1)
    for n, header in enumerate(headers):
        h_value = header_row[n] if len(header_row) > n else None
        if not h_value:
            h = worksheet.cell(1, n+1)
            h.value = header
            header_cells.append(h)
        if h_value and h_value != header:
            raise ValueError("Header cell %s at %s doesn't match %s" % (h_value, cell_name(1, n+1), header))
    logging.info("Updating %d header cells in %s", len(header_cells), worksheet.title)
    if header_cells:
        worksheet.update_cells(header_cells)

def start_of_week(d):
    """Returns the starting day of the week for the given datetime"""
    return (d - timedelta(days=d.weekday())).date()

def sync_sheets(spreadsheet, year, client=None):
    today = datetime.today()
    months = reversed(range(1, today.month+1 if today.year == year else 13))
    weekly_summary = get_or_add_worksheet(spreadsheet, "Weekly Summary")
    setup_header(weekly_summary, SUMMARY_HEADERS)
    monthly_summary = get_or_add_worksheet(spreadsheet, "Monthly Summary")
    setup_header(monthly_summary, SUMMARY_HEADERS)
    summary_weeks = {}
    summary_months = {}
    for month in months:
        start_date = datetime(year=year, month=month, day=1)
        end_date = (start_date + timedelta(days=32)).replace(day=1)
        month_sheet = get_or_add_worksheet(spreadsheet, start_date.strftime("%b"))
        setup_header(month_sheet)
        toggl_id_map = {}
        append_row = 2
        update_cells = []
        added, updated, unchanged = 0, 0, 0
        logging.info("Fetching toggl data for %s", month_sheet.title)
        month_entries = list(get_entries(start_date, end_date, client))
        logging.info("Fetching existing spreadsheet data for %s", month_sheet.title)
        full_range = month_sheet.range("%s:%s" % (month_sheet.get_addr_int(2, 1),
                                                  month_sheet.get_addr_int(month_sheet.row_count, len(SHEET_HEADERS))))
        def get_row(row_num):
            if 2 <= row_num <= month_sheet.row_count:
                start_cell = (row_num-2)*len(SHEET_HEADERS)
                cells = full_range[start_cell:start_cell+len(SHEET_HEADERS)]
            else:
                cells = month_sheet.range("%s:%s" % (month_sheet.get_addr_int(row_num, 1),
                                                     month_sheet.get_addr_int(row_num, len(SHEET_HEADERS))))
            assert len(cells) == len(SHEET_HEADERS)
            for n, cell in enumerate(cells):
                assert cell.row == row_num
                assert cell.col == n + 1
            return cells

        for row_num in range(2, month_sheet.row_count+1):
            row_cells = get_row(row_num)
            sheet_row = {SHEET_HEADERS[n]: row_cells[n].value for n in range(len(SHEET_HEADERS))}
            if sheet_row['toggl_id']:
                toggl_id_map[int(sheet_row['toggl_id'])] = row_num, sheet_row
                if row_num >= append_row:
                    append_row = row_num + 1
        logging.info("Synchronizing data")
        for time_entry in month_entries:
            if time_entry.get('start'):
                start_time = localtz.fromutc(dateutil.parser.parse(time_entry.get('start')).replace(tzinfo=None))
                week = start_of_week(start_time)
                summary_weeks.setdefault(week, 0)
                summary_weeks[week] += time_entry.get('duration')
                summary_months.setdefault(month, 0)
                summary_months[month] += time_entry.get('duration')
            toggl_id = time_entry.get('id')
            sheet_values = entry_to_sheet_row(time_entry)
            if toggl_id in toggl_id_map:
                row, sheet_row = toggl_id_map[toggl_id]
                cell_list = get_row(row)
                was_changed = False
                for n, (header, update_cell) in enumerate(zip(SHEET_HEADERS, cell_list)):
                    cell_value = sheet_row[header]
                    if header == 'toggl_id':
                        cell_value = int(cell_value)
                    elif header in ("Project", "Description") and cell_value:
                        cell_value = "'" + cell_value
                    if cell_value != sheet_values[header]:
                        update_cell.value = sheet_values[header]
                        logging.info("Mismatch on id %s at %s on %s: %r %r", toggl_id,
                                     month_sheet.get_addr_int(row, n+1), header, cell_value, sheet_values[header])
                        update_cells.append(update_cell)
                        was_changed = True
                if was_changed:
                    updated += 1
                else:
                    unchanged += 1
            else:
                cell_list = get_row(append_row)
                for header, update_cell in zip(SHEET_HEADERS, cell_list):
                    if update_cell is not None:
                        update_cell.value = sheet_values[header]
                update_cells.extend(cell_list)
                added += 1
                append_row += 1
            if (added + updated + unchanged) % 100 == 0:
                logging.info("Added %d, updated %d, didn't change %d rows", added, updated, unchanged)
            if len(update_cells) > 250:
                logging.info("Sending %d cells to sheet", len(update_cells))
                month_sheet.update_cells(update_cells)
                del update_cells[:]
        if update_cells:
            logging.info("Sending %d cells to sheet", len(update_cells))
            month_sheet.update_cells(update_cells)
    week_cells = weekly_summary.range("%s:%s" % (weekly_summary.get_addr_int(2, 1),
                                                weekly_summary.get_addr_int(len(summary_weeks)+2, len(SUMMARY_HEADERS))))
    month_cells = monthly_summary.range("%s:%s" % (monthly_summary.get_addr_int(2, 1),
                                                   monthly_summary.get_addr_int(len(summary_months) + 2,
                                                                                len(SUMMARY_HEADERS))))
    logging.info("Updating summary cells")
    for n, (week, duration) in enumerate(sorted(summary_weeks.items())):
        minutes = duration // 60
        logging.info("Week starting %s had %d minutes", week.strftime("%Y-%m-%d"), minutes)
        week_cells[n * len(SUMMARY_HEADERS) + 0].value = week.strftime("%Y-%m-%d")
        week_cells[n * len(SUMMARY_HEADERS) + 2].value = "'%d:%02d" % (minutes//60, minutes%60)
    weekly_summary.update_cells(week_cells)
    for n, (month, duration) in enumerate(sorted(summary_months.items())):
        minutes = duration // 60
        month_start = datetime(year, month, 1)
        logging.info("Month %s had %d minutes", month_start.strftime("%Y-%m (%b)"), minutes)
        month_cells[n * len(SUMMARY_HEADERS) + 0].value = month_start.strftime("%Y-%m (%b)")
        month_cells[n * len(SUMMARY_HEADERS) + 2].value = "'%d:%02d" % (minutes // 60, minutes % 60)
    monthly_summary.update_cells(month_cells)


def main():
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("requests").setLevel(logging.WARNING)
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--credentials', action='store', type=str, help='Credentials filename')
    parser.add_argument('-C', '--client', action='store', type=str, help='Client name')
    this_year = datetime.today().year
    parser.add_argument('--year', action='store', type=int, help='Year to store in spreadsheet', default=this_year)
    parser.add_argument('spreadsheet', help='URL of Google Sheet to edit')
    options = parser.parse_args()
    client = toggl.ClientList().find_by_name(options.client) if options.client else None
    credentials = ServiceAccountCredentials.from_json_keyfile_name(options.credentials, service_scope)
    c = gspread.authorize(credentials)
    spreadsheet = c.open_by_url(options.spreadsheet)
    sync_sheets(spreadsheet, options.year, client)


if __name__ == '__main__':
    main()

