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


def merge_dictionary(sn_dict_1, sn_dict_2):
    """Merge two dictionaries."""
    merged_dict = sn_dict_1.copy()
    for key, value in sn_dict_2.items():
        if key in merged_dict:
            if not (isinstance(merged_dict[key], dict) and isinstance(value, dict)):
                continue
            if 'file' in merged_dict[key].keys() and 'file' in value.keys():
                if datetime.strptime(merged_dict[key]['file'][:10], '%Y-%m-%d') > datetime.strptime(
                    value['file'][:10], '%Y-%m-%d'
                ):
                    merged_dict[key]['file'] = value['file']
            if not ('start_date' in merged_dict[key].keys()) and 'start_date' in value.keys():
                merged_dict[key]['start_date'] = value['start_date']
            if 'start_date' in merged_dict[key].keys() and 'start_date' in value.keys():
                if datetime.strptime(value['start_date'][:10], '%Y-%m-%d') < datetime.strptime(
                    merged_dict[key]['start_date'][:10], '%Y-%m-%d'
                ):
                    merged_dict[key]['start_date'] = value['start_date']
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


def get_data_files(reverse=False):
    """Return data csv files list from data folder."""
    return sorted((file for file in os.listdir('data') if file.endswith('.csv')), reverse=reverse)


def get_first_file_date():
    """Return older file date."""
    return get_data_files()[0][:10]


def split_list(lst, n):
    """Divise une liste en n parties égales (ou presque égales)."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def get_start_files(sn_dict):
    """Return serial numbers first appearance in data files."""
    print('\nLooking for start file')
    data_files = get_data_files()
    sn_not_computed = []
    process_file_name = f'start_file_{data_files[0][:10]}.json'

    # Get Info from old run
    if os.path.isfile(f'process/{process_file_name}'):
        print(f'Found process file : {process_file_name}')
        with open(f'process/{process_file_name}', 'r', encoding='utf-8') as process_file:
            process_data = json.load(process_file)
        for serial_number in sn_dict.keys():
            if serial_number in process_data.keys():
                if 'start_file' in process_data[serial_number].keys():
                    if process_data[serial_number]['start_file'] is not None:
                        sn_dict[serial_number]['start_file'] = process_data[serial_number][
                            'start_file'
                        ]
                else:
                    sn_not_computed.append(serial_number)
                    sn_dict[serial_number]['start_file'] = None
    else:
        sn_not_computed = list(sn_dict.keys())

    # Remove serial_numbers that cannot be processed
    print('Remove serial_numbers that cannot be processed, (too few files)')
    dataframe = pd.read_csv(f'data/{data_files[0]}')
    existing_serial_numbers = dataframe['serial_number'].values
    for serial_number in tqdm(sn_not_computed.copy()):
        if serial_number in existing_serial_numbers:
            sn_not_computed.remove(serial_number)
            sn_dict[serial_number]['start_file'] = None
    print()

    print('Getting start file')
    if sn_not_computed:
        # Process
        with mp.Manager() as manager:
            with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
                futures = [
                    executor.submit(get_start_files_process, sn_not_computed, data_file)
                    for data_file in data_files
                ]
                for future in tqdm(as_completed(futures), total=len(futures)):
                    sn_found_list, data_file = future.result()
                    for sn_found in sn_found_list:
                        if 'start_file' not in sn_dict[sn_found]:
                            sn_dict[sn_found]['start_file'] = data_file
                        elif datetime.strptime(data_file[:10], '%Y-%m-%d') < datetime.strptime(
                            sn_dict[sn_found]['start_file'][:10], '%Y-%m-%d'
                        ):
                            sn_dict[sn_found]['start_file'] = data_file

                    # Saving for next run
                    os.makedirs('process', exist_ok=True)
                    with open(
                        f'process/{process_file_name}', 'w', encoding='utf-8'
                    ) as process_file:
                        json.dump(sn_dict, process_file)

    # Saving for next run
    os.makedirs('process', exist_ok=True)
    with open(f'process/{process_file_name}', 'w', encoding='utf-8') as process_file:
        json.dump(sn_dict, process_file)

    return sn_dict


def get_start_files_process(sn_to_process, data_file):
    """Return serial number first appearance in data files."""
    sn_found = []
    dataframe = pd.read_csv(f'data/{data_file}')
    existing_serial_numbers = dataframe['serial_number'].values
    for serial_number in sn_to_process:
        if serial_number in existing_serial_numbers:
            sn_found.append(serial_number)

    return sn_found, data_file


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


def parse_files(files_to_open):
    """Parse input csv files from BackBlaze."""
    results_df = pd.DataFrame()
    data_files = get_data_files()
    process_file_name = f'parsed_data_{data_files[0][:10]}.json'
    print('\nOpening files to get history')

    # Get Info from old run
    if os.path.isfile(f'process/{process_file_name}'):
        with open(f'process/{process_file_name}', 'rb') as process_file:
            print(f'Found process file : {process_file_name}')
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
        if results_df.empty:
            print('Parsing failed. No data available')
            return None
        results_df['date'] = pd.to_datetime(results_df['date'])

        # Saving for next run
        os.makedirs('process', exist_ok=True)
        with open(f'process/{process_file_name}', 'wb') as process_file:
            pickle.dump(results_df, process_file)

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


def get_failed_serial_number_from_files(files_to_process):
    """Check failures presence in dataframe."""
    sn_dict = {}
    data_files = get_data_files()
    process_file_name = f'failed_sn_{data_files[0][:10]}.json'
    print('Getting failed sn...')

    # Get Info from old run
    if os.path.isfile(f'process/{process_file_name}'):
        with open(f'process/{process_file_name}', 'r', encoding='utf-8') as process_file:
            sn_dict = json.load(process_file)
    else:
        with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
            futures = [
                executor.submit(get_failed_serial_number_from_file, file)
                for file in files_to_process
            ]
            for future in tqdm(as_completed(futures), total=len(futures)):
                try:
                    new_sn_dict = future.result()
                    if new_sn_dict is not None:
                        sn_dict = merge_dictionary(sn_dict, new_sn_dict)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f'Parsing generated an exception: {exc}')

    # Saving for next run
    os.makedirs('process', exist_ok=True)
    with open(f'process/{process_file_name}', 'w', encoding='utf-8') as process_file:
        json.dump(sn_dict, process_file, indent=4)

    print(f'{len(sn_dict)} serial numbers found')
    return sn_dict


def get_files_to_open(sn_dict, history_length_recent, history_length_old):
    """Return list of files to open."""
    print('\nGetting files to open')
    data_files = get_data_files()
    process_file_name = f'files_to_open_{data_files[0][:10]}.json'
    files_to_open = {}

    if os.path.isfile(f'process/{process_file_name}'):
        with open(f'process/{process_file_name}', 'r', encoding='utf-8') as process_file:
            print(f'Found process file : {process_file_name}')
            return json.load(process_file)

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
        start_date = datetime.strptime(info_dict['start_file'][:10], '%Y-%m-%d')
        for idx in range(history_length_old):
            file_to_open = (start_date + timedelta(days=idx)).strftime('%Y-%m-%d') + '.csv'
            if file_to_open in data_files:
                if file_to_open in files_to_open:
                    files_to_open[file_to_open].append(serial_number)
                else:
                    files_to_open[file_to_open] = [serial_number]

    with open(f'process/{process_file_name}', 'w', encoding='utf-8') as process_file:
        json.dump(files_to_open, process_file, indent=4)

    print(f'{len(files_to_open.keys())} files to open')

    return files_to_open


def create_csv_file(serial_number, sn_dict, disk_df):
    """Generate csv file."""
    if serial_number not in sn_dict.keys():
        return
    if sn_dict[serial_number]['result_filename'] is None:
        return
    print(sn_dict[serial_number])
    disk_df = disk_df.sort_values(by='date', ascending=False)
    os.makedirs('results/', exist_ok=True)
    result_filename = sn_dict[serial_number]['result_filename']
    disk_df.to_csv(
        f'results/{result_filename}',
        sep='\t',
        decimal=',',
    )


def create_csv_files(sn_dict, results_df):
    """Generate csv files."""
    print('\nCreating csv files')
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        futures = [
            executor.submit(
                create_csv_file,
                serial_number,
                sn_dict,
                disk_df,
            )
            for serial_number, disk_df in results_df.groupby('serial_number')
        ]
        for _ in tqdm(as_completed(futures), total=len(futures)):
            pass


def set_result_filename(sn_dict, history_length_recent, history_length_old):
    """Set result csv filename."""
    for serial_number in sn_dict.keys():
        if 'start_file' not in sn_dict[serial_number].keys():
            continue
        if sn_dict[serial_number]['start_file'] is None:
            sn_dict[serial_number]['result_filename'] = None
            continue
        prefix = (
            str(sn_dict[serial_number]['file'][:10])
            + '_'
            + str(sn_dict[serial_number]['start_file'][:10])
            + '_'
            + str(history_length_old)
            + '_'
            + str(history_length_recent)
        )
        sn_dict[serial_number]['result_filename'] = f'{prefix}_{serial_number}.csv'

    return sn_dict


def process(process_size, history_length_recent, history_length_old):
    """Process data_files."""
    # Variables
    data_files = get_data_files(True)
    files_to_process = data_files[:process_size]
    index = -1

    while files_to_process:
        # Variables
        index = index + 1
        if (index + 1) * process_size > len(data_files):
            files_to_process = data_files[index * process_size :]
        else:
            files_to_process = data_files[index * process_size : (index + 1) * process_size]

        # Display
        text = f'Computing files from {files_to_process[-1]} to {files_to_process[0]}'
        line = '-' * len(text)
        print(line)
        print(text)
        print(line)

        # Get failed serial-numbers
        sn_dict = get_failed_serial_number_from_files(files_to_process)
        if not sn_dict:
            print('No sn found !')
            continue

        # look for first apparition date
        sn_dict = get_start_files(sn_dict)

        # Set result filename
        sn_dict = set_result_filename(sn_dict, history_length_recent, history_length_old)

        # Skip all serial numbers already processed
        for serial_number in list(sn_dict.keys()).copy():
            if 'result_filename' not in sn_dict[serial_number]:
                continue
            if sn_dict[serial_number]['result_filename'] is None:
                del sn_dict[serial_number]
            elif os.path.isfile('results/' + sn_dict[serial_number]['result_filename']):
                del sn_dict[serial_number]
        if not bool(sn_dict):
            print('All serial numbers csv files exist in result folder\n\n')
            continue

        # Which files do we need to open now ?
        # For most recent history
        files_to_open = get_files_to_open(
            sn_dict,
            history_length_recent,
            history_length_old,
        )

        # Parsing files to get history
        results_df = parse_files(files_to_open)
        if results_df is None:
            continue

        # Create csv files
        create_csv_files(sn_dict, results_df)

        print('\n\n')


def main():
    """Entry point."""
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
    parser.add_argument(
        '--process_size',
        type=int,
        default=30,
        help='Entier représentant le nombre de disques à traiter à la fois',
    )

    args = parser.parse_args()

    process(args.process_size, args.history_length_recent, args.history_length_old)


if __name__ == '__main__':
    main()
