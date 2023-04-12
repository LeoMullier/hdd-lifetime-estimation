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


def get_start_date_from_serial_numbers(sn_dict, date):
    """Return serial numbers first appearance in data files."""
    print('\nLooking for first date')
    data_files = get_data_files()
    sn_not_computed = []

    # Init dict
    for serial_number in sn_dict.keys():
        sn_dict[serial_number]['start_date'] = {'min': 0, 'max': len(data_files) - 1}

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
        for serial_number in sn_dict.keys():
            if serial_number in process_data.keys():
                sn_dict[serial_number]['start_date'] = process_data[serial_number]['start_date']
            else:
                sn_not_computed.append(serial_number)
    else:
        sn_not_computed = sn_dict.keys()

    if sn_not_computed:
        # Process
        with ProcessPoolExecutor() as executor:
            futures = []
            for serial_number in sn_not_computed:
                futures.append(
                    executor.submit(get_start_date_from_serial_number, serial_number, data_files)
                )

            for future in tqdm(futures):
                serial_number, start_date = future.result()
                sn_dict[serial_number]['start_date'] = start_date

                # Saving for next run
                os.makedirs('process', exist_ok=True)
                with open(
                    f'process/start_date_{date}_{data_files[0][:10]}.json', 'w', encoding='utf-8'
                ) as process_file:
                    json.dump(sn_dict, process_file)

    return sn_dict


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


def parse_file(filename, serial_numbers):
    """Parse input csv file from BackBlaze."""
    results_df = pd.DataFrame()
    dataframe = csv_to_dataframe(filename)

    for serial_number in serial_numbers:
        interesting_row = dataframe.loc[dataframe['serial_number'] == serial_number]
        if not interesting_row.empty:
            results_df = pd.concat([results_df, interesting_row])
    if results_df.empty:
        return None
    return results_df


def parse_files(files_to_open, date):
    """Parse input csv files from BackBlaze."""
    results_df = pd.DataFrame()
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
            results_df = pickle.load(process_file)
    else:
        with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
            futures = [
                executor.submit(parse_file, filename, serial_numbers)
                for filename, serial_numbers in files_to_open.items()
            ]
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    data = future.result()
                    if data is None:
                        continue
                    results_df = pd.concat([results_df, data])
                except Exception as exc:  # pylint: disable=broad-except
                    print(f'Parsing generated an exception: {exc}')

        results_df['date'] = pd.to_datetime(results_df['date'])

        # Saving for next run
        os.makedirs('process', exist_ok=True)
        with open(f'process/parsed_data_{date}_{get_first_file_date()}.json', 'wb') as process_file:
            process_file.write(pickle.dumps(results_df))

    return results_df


def merge_lists(list1, list2):
    """Merge two lists together without duplicates."""
    merged_list = list1 + list2
    merged_list = list(dict.fromkeys(merged_list))
    return merged_list


def get_failed_serial_number_from_file(file):
    """Get failed serial numbers list from file."""
    serial_numbers = {}

    dataframe = csv_to_dataframe(file)
    failures_dataframe = dataframe[(dataframe['failure'] == 1)]
    if failures_dataframe.empty:
        return None
    for index, _ in failures_dataframe.iterrows():
        serial_numbers[dataframe.iloc[index]['serial_number']] = {'file': file}

    return serial_numbers


def get_failed_serial_number_from_files(data_files, date):
    """Check failures presence in dataframe."""
    serial_numbers = {}
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
                        serial_numbers = merge_dictionary(serial_numbers, data)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f'Parsing generated an exception: {exc}')

    # Saving for next run
    os.makedirs('process', exist_ok=True)
    with open(
        f'process/failed_sn_{date}_{data_files[0][:10]}.json', 'w', encoding='utf-8'
    ) as process_file:
        json.dump(serial_numbers, process_file, indent=4)

    print(f'{len(serial_numbers)} serial numbers found')
    return serial_numbers


def get_files_to_open(sn_dict, month, year, history_length_recent, history_length_old):
    """Return list of files to open."""
    print('\nGetting files to open')
    data_files = get_data_files()
    files_to_open = {}

    if os.path.exists('process'):
        for process_file_candidate in sorted(os.listdir('process')):
            if f'parsed_data_{year}-{month}_{get_first_file_date()}' in process_file_candidate:
                print(f'Found process file : {process_file_candidate}')
                return files_to_open

    for serial_number, info_dict in sn_dict.items():
        # Most recent history
        failure_date = datetime.strptime(info_dict['file'][:10], '%Y-%m-%d')
        for idx in range(history_length_recent):
            file_to_open = (failure_date - timedelta(days=idx)).strftime('%Y-%m-%d') + '.csv'
            if file_to_open in data_files:
                if file_to_open in files_to_open:
                    files_to_open[file_to_open].append(serial_number)
                else:
                    files_to_open[file_to_open] = [serial_number]

        # Older history
        start_date = datetime.strptime(info_dict['start_date'][:10], '%Y-%m-%d')
        for idx in range(history_length_old):
            file_to_open = (start_date + timedelta(days=idx)).strftime('%Y-%m-%d') + '.csv'
            if file_to_open in data_files:
                if file_to_open in files_to_open:
                    files_to_open[file_to_open].append(serial_number)
                else:
                    files_to_open[file_to_open] = [serial_number]

    print(f'{len(files_to_open.keys())} files to open')

    return files_to_open


def create_csv_file(
    serial_number, results_df, month, year, history_length_recent, history_length_old
):
    """Generate csv file."""
    disk_df = results_df[(results_df['serial_number'] == serial_number)]
    disk_df = disk_df.sort_values(by='date', ascending=False)
    os.makedirs(f'results/{year}/{month}', exist_ok=True)
    disk_df.to_csv(
        f'results/{year}/{month}/{serial_number}_{history_length_old}_{history_length_recent}.csv',
        sep='\t',
        decimal=',',
    )


def create_csv_files(
    serial_numbers, results_df, month, year, history_length_recent, history_length_old
):
    """Generate csv files."""
    print('\nCreating csv files')
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        futures = [
            executor.submit(
                create_csv_file,
                serial_number,
                results_df,
                month,
                year,
                history_length_recent,
                history_length_old,
            )
            for serial_number in serial_numbers
        ]
        for _ in tqdm(as_completed(futures), total=len(futures)):
            pass


def remove_sn_without_enough_history(sn_dict):
    """Remove sn without enough history."""
    print('\nRemoving sn without enough history to succeed')
    new_sn_dict = {}
    older_file = get_data_files()[0]

    for serial_number, info_dict in sn_dict.items():
        if info_dict['start_date'] != older_file:
            new_sn_dict[serial_number] = info_dict

    print(f'{len(new_sn_dict.keys())} serial numbers left')
    return new_sn_dict


def process(data_files_dict, history_length_recent, history_length_old):
    """Process data_files."""
    for year in sorted(data_files_dict.keys(), reverse=True):
        for month in sorted(data_files_dict[year].keys(), reverse=True):

            # Display
            print('-------------------------------------')
            print(f'--------- Computing {month}-{year} ---------')
            print('-------------------------------------')

            # Get failed serial-numbers
            sn_dict = get_failed_serial_number_from_files(
                data_files_dict[year][month], f'{year}-{month}'
            )
            if not sn_dict:
                print('No sn found !')
                continue

            # look for first apparition date
            sn_dict = get_start_date_from_serial_numbers(sn_dict, f'{year}-{month}')

            # Remove sn with no enough data history
            sn_dict = remove_sn_without_enough_history(sn_dict)

            # Skip all serial numbers already processed
            if os.path.exists(f'results/{year}/{month}'):
                for serial_number in list(sn_dict.keys()).copy():
                    if os.path.isfile(
                        f'results/{year}/{month}/{serial_number}_{history_length_old}_{history_length_recent}.csv'
                    ):
                        del sn_dict[serial_number]
            if not bool(sn_dict):
                print('All serial numbers csv files exist in result folder\n\n')
                continue

            # Which files do we need to open now ?
            # For most recent history
            files_to_open = get_files_to_open(
                sn_dict,
                month,
                year,
                history_length_recent,
                history_length_old,
            )

            # Parsing files to get history
            results_df = parse_files(files_to_open, f'{year}-{month}')

            # Create csv files
            create_csv_files(
                sn_dict, results_df, month, year, history_length_recent, history_length_old
            )

            print('\n\n')


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
