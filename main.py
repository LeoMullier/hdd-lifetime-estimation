"""
Created on 25 March. 2023.

@author: nicolas.parmentier
"""

import argparse
import json
import multiprocessing as mp
import os
import pickle
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
from tqdm import tqdm


def merge_dictionary(dict1, dict2):
    """Merge two dictionaries."""
    merged_dict = dict1.copy()
    for key, value in dict2.items():
        if key in merged_dict:
            if isinstance(merged_dict[key], list):
                merged_dict[key].append(value)
            else:
                merged_dict[key] = [merged_dict[key], value]
        else:
            merged_dict[key] = value
    return merged_dict


def csv_to_dataframe(csv_name) -> pd.DataFrame:
    """Transform a csv to a dataframe object."""
    try:
        dataframe = pd.read_csv('data/' + csv_name)
        return dataframe
    except (Exception,):  # pylint: disable=broad-except
        print(f'Cannot read {csv_name}')
        sys.exit(1)


def get_data_files():
    """Return data csv files list from data folder."""
    return sorted(file for file in os.listdir('data') if file.endswith('.csv'))


def get_first_file_date():
    """Return older file date."""
    return get_data_files()[0][:10]


def get_start_date_from_serial_numbers(serial_numbers, date):
    """Return serial numbers first appearance in data files."""
    print('\nLooking for first date')
    data_files = get_data_files()
    indexes = {}

    # Init dict
    for serial_number in serial_numbers:
        indexes[serial_number] = {'min': 0, 'max': len(data_files) - 1}

    # Get Info from old run
    process_file = None
    if os.path.exists('process'):
        for process_file_candidate in sorted(os.listdir('process')):
            if f'start_date_{date}_{data_files[0][:10]}' in process_file_candidate:
                print(f'Found process file : {process_file_candidate}')
                process_file = process_file_candidate
    if process_file is not None:
        with open(f'process/{process_file}', 'r', encoding='utf-8') as process_file:
            process_data = json.load(process_file)
        for serial_number, start_date in process_data.items():
            if serial_number in indexes:
                indexes[serial_number] = start_date
                serial_numbers.remove(serial_number)

    if serial_numbers:
        # Process
        with ProcessPoolExecutor() as executor:
            futures = []
            for serial_number in serial_numbers:
                futures.append(
                    executor.submit(get_start_date_from_serial_number, serial_number, data_files)
                )

            for future in tqdm(futures):
                serial_number, start_date = future.result()
                indexes[serial_number] = start_date

                # Saving for next run
                os.makedirs('process', exist_ok=True)
                with open(
                    f'process/start_date_{date}_{data_files[0][:10]}.json', 'w', encoding='utf-8'
                ) as process_file:
                    json.dump(
                        {sn: fd for sn, fd in indexes.items() if isinstance(fd, str)},
                        process_file,
                        indent=4,
                    )

    return indexes


def get_start_date_from_serial_number(serial_number, data_files):
    """Return serial number first appearance in data files."""
    min_index = 0
    max_index = len(data_files) - 1

    while min_index != max_index:
        index = (min_index + max_index) // 2
        dataframe = pd.read_csv(f'data/{data_files[index]}')
        interesting_row = dataframe.loc[dataframe['serial_number'] == serial_number]
        if interesting_row.empty:
            min_index = index + 1
        else:
            max_index = min(index, max_index)

    start_date = data_files[max_index]
    # print(serial_number, ' : ', start_date)
    return serial_number, start_date


def parse_file(file_name, serial_numbers):
    """Parse input csv file from BackBlaze."""
    interesting_rows = {}
    dataframe_it = csv_to_dataframe(file_name)

    for serial_number in serial_numbers:
        interesting_row = dataframe_it.loc[dataframe_it['serial_number'] == serial_number]
        if not interesting_row.empty:
            interesting_rows[serial_number] = interesting_row
    return interesting_rows


def parse_files(serial_numbers, files_to_open, date):
    """Parse input csv files from BackBlaze."""
    rows = {}
    print('\nOpening files to get history')

    # Get Info from old run
    process_file = None
    if os.path.exists('process'):
        for process_file_candidate in sorted(os.listdir('process')):
            if f'parsed_data_{date}_{get_first_file_date()}' in process_file_candidate:
                print(f'Found process file : {process_file_candidate}')
                process_file = process_file_candidate
    if process_file is not None:
        with open(f'process/{process_file}', 'rb') as process_file:
            rows = pickle.load(process_file)
    else:
        with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
            futures = [
                executor.submit(parse_file, file_to_open, serial_numbers)
                for file_to_open in files_to_open
            ]
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    data = future.result()
                    if data is not None:
                        rows = merge_dictionary(rows, data)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f'Parsing generated an exception: {exc}')

        # Saving for next run
        os.makedirs('process', exist_ok=True)
        with open(f'process/parsed_data_{date}_{get_first_file_date()}.json', 'wb') as process_file:
            process_file.write(pickle.dumps(rows))

    return rows


def merge_lists(list1, list2):
    """Merge two lists together without duplicates."""
    merged_list = list1 + list2
    merged_list = list(dict.fromkeys(merged_list))
    return merged_list


def get_failed_serial_number_from_file(file):
    """Get failed serial numbers list from file."""
    serial_numbers = []

    dataframe = csv_to_dataframe(file)
    failures_dataframe = dataframe[(dataframe['failure'] == 1)]
    if failures_dataframe.empty:
        return None
    for index, _ in failures_dataframe.iterrows():
        serial_numbers.append(dataframe.iloc[index]['serial_number'])

    return serial_numbers


def get_failed_serial_number_from_files(data_files, date):
    """Check failures presence in dataframe."""
    serial_numbers = []
    print('Getting failed sn...')

    # Get Info from old run
    process_file = None
    if os.path.exists('process'):
        for process_file_candidate in sorted(os.listdir('process')):
            if f'failed_sn_{date}' in process_file_candidate:
                print(f'Found process file : {process_file_candidate}')
                process_file = process_file_candidate
    if process_file is not None:
        with open(f'process/{process_file}', 'r', encoding='utf-8') as process_file:
            serial_numbers = json.load(process_file)
    else:
        with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
            futures = [
                executor.submit(get_failed_serial_number_from_file, file) for file in data_files
            ]
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    data = future.result()
                    if data is not None:
                        serial_numbers = merge_lists(serial_numbers, data)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f'Parsing generated an exception: {exc}')

    # Saving for next run
    os.makedirs('process', exist_ok=True)
    with open(
        f'process/failed_sn_{date}_{data_files[0][:10]}.json', 'w', encoding='utf-8'
    ) as process_file:
        json.dump(serial_numbers, process_file, indent=4)

    return serial_numbers


def get_files_to_open(
    data_files_dict, first_date_dict, month, year, history_length_recent, history_length_old
):
    """Return list of files to open."""
    print('\nGetting files to open')

    files_to_open = []

    if os.path.exists('process'):
        for process_file_candidate in sorted(os.listdir('process')):
            if f'parsed_data_{year}-{month}_{get_first_file_date()}' in process_file_candidate:
                print(f'Found process file : {process_file_candidate}')
                return files_to_open

    for month_file in data_files_dict[year][month]:
        start_date = datetime.strptime(month_file[:10], '%Y-%m-%d')
        for idx in range(history_length_recent):
            file_to_open = (start_date - timedelta(days=idx)).strftime('%Y-%m-%d') + '.csv'
            if file_to_open in get_data_files() and file_to_open not in files_to_open:
                files_to_open.append(file_to_open)
    # For most recent history
    for first_date in first_date_dict.values():
        start_date = datetime.strptime(first_date[:10], '%Y-%m-%d')
        for idx in range(history_length_old):
            file_to_open = (start_date + timedelta(days=idx)).strftime('%Y-%m-%d') + '.csv'
            if file_to_open in get_data_files() and file_to_open not in files_to_open:
                files_to_open.append(file_to_open)
    print(f'{len(files_to_open)} files to open')

    return files_to_open


def create_csv_file(serial_number, rows, month, year, history_length_recent, history_length_old):
    """Generate csv file."""
    print('1')
    disk_data = pd.concat(rows[serial_number], ignore_index=True)
    print('2')
    disk_data['date'] = pd.to_datetime(disk_data['date'])
    print('3')
    disk_data = disk_data.sort_values(by='date', ascending=False)
    print('4')
    os.makedirs(f'results/{year}/{month}', exist_ok=True)
    print('5')
    disk_data.to_csv(
        f'results/{year}/{month}/{serial_number}_{history_length_old}_{history_length_recent}.csv',
        sep='\t',
        decimal=',',
    )


def create_csv_files(serial_numbers, rows, month, year, history_length_recent, history_length_old):
    """Generate csv files."""
    print('\nCreating csv files')
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        futures = [
            executor.submit(
                create_csv_file,
                serial_number,
                rows,
                month,
                year,
                history_length_recent,
                history_length_old,
            )
            for serial_number in serial_numbers
        ]
        for _ in tqdm(as_completed(futures), total=len(futures)):
            print('ok')


def process(data_files_dict, history_length_recent, history_length_old):
    """Process data_files."""
    for year in sorted(data_files_dict.keys(), reverse=True):
        for month in sorted(data_files_dict[year].keys(), reverse=True):

            # Display
            print('-------------------------------------')
            print(f'--------- Computing {month}-{year} ---------')
            print('-------------------------------------')

            # Skip if folder exists
            if os.path.exists(f'results/{year}/{month}'):
                print(f'Results for {month}/{year} already exist\n')
                continue

            # Get failed serial-numbers
            serial_numbers = get_failed_serial_number_from_files(
                data_files_dict[year][month], f'{year}-{month}'
            )
            if not serial_numbers:
                print('No sn found !')
                continue

            # look for first apparition date
            first_date_dict = get_start_date_from_serial_numbers(serial_numbers, f'{year}-{month}')

            # Remove sn with no enough data history
            first_date_dict = {
                key: value for key, value in first_date_dict.items() if value != get_data_files()[0]
            }
            serial_numbers = list(first_date_dict.keys())

            # Which files do we need to open now ?
            # For most recent history
            files_to_open = get_files_to_open(
                data_files_dict,
                first_date_dict,
                month,
                year,
                history_length_recent,
                history_length_old,
            )

            # Parsing files to get history
            rows = parse_files(serial_numbers, files_to_open, f'{year}-{month}')
            size_in_bytes = sys.getsizeof(rows)
            size_in_gb = size_in_bytes / (1024 * 1024)
            print(f'The size of my_object is {size_in_gb:.2f} MB')
            for key in rows.keys():
                print(rows[key][0])
            sys.exit(0)

            # Create csv files
            create_csv_files(
                serial_numbers, rows, month, year, history_length_recent, history_length_old
            )

            print()


def main():
    """Entry point."""
    # Variables
    data_files_dict = {}

    # Handle args
    parser = argparse.ArgumentParser(description='BackBlaze data parser.')
    parser.add_argument(
        '--history_length_recent',
        type=int,
        default=90,
        help='Entier représentant la longueur de l\'historique récent',
    )
    parser.add_argument(
        '--history_length_old',
        type=int,
        default=30,
        help='Entier représentant la longueur de l\'historique plus ancien',
    )

    args = parser.parse_args()

    for data_file in get_data_files():
        # Extracting year - month
        year, month, _ = data_file.split('-')

        if year not in data_files_dict:
            data_files_dict[year] = {}
        if month not in data_files_dict[year]:
            data_files_dict[year][month] = []
        data_files_dict[year][month].append(data_file)

    process(data_files_dict, args.history_length_recent, args.history_length_old)


if __name__ == '__main__':
    main()
