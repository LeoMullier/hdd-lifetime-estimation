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

CSV_DIR = 'data/csv/'
PARQUET_DIR = 'data/parquet/'
PROCESS_DIR = 'process/'


def convert_csv_to_parquet(csv_file_name):
    """Convert csv to parquet files."""
    csv_path = os.path.join(CSV_DIR, csv_file_name)
    parquet_path = os.path.join(PARQUET_DIR, csv_file_name.replace('.csv', '.parquet'))

    if not os.path.exists(parquet_path):
        dataframe = pd.read_csv(csv_path)
        dataframe.to_parquet(parquet_path, compression=None)


def convert_csvs_to_parquets():
    """Convert csv to parquet files."""
    print('Converting csv to parquet files...')
    csv_files = get_csv_data_files()

    for csv_file in csv_files.copy():
        if os.path.exists(os.path.join(PARQUET_DIR, csv_file.replace('.csv', '.parquet'))):
            csv_files.remove(csv_file)

    if csv_files:
        with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
            futures = [
                executor.submit(convert_csv_to_parquet, csv_file_name)
                for csv_file_name in csv_files
            ]

            for _ in tqdm(as_completed(futures), total=len(futures)):
                pass
    print('All csv files have been converted to parquet files...')


def csv_to_dataframe(csv_name) -> pd.DataFrame:
    """Transform a csv to a dataframe object."""
    try:
        dataframe = pd.read_csv(CSV_DIR + csv_name)
        return dataframe
    except (Exception,):  # pylint: disable=broad-except
        print(f'Cannot read {csv_name}')
        if input('Delete file ? (y/n)') == 'y':
            os.remove(CSV_DIR + csv_name)
        sys.exit(1)


def parquet_to_dataframe(parquet_name) -> pd.DataFrame:
    """Transform a csv to a dataframe object."""
    try:
        dataframe = pd.read_parquet(PARQUET_DIR + parquet_name)
        return dataframe
    except (Exception,):  # pylint: disable=broad-except
        print(f'Cannot read {PARQUET_DIR + parquet_name}')
        sys.exit(1)


def get_csv_data_files(reverse=False):
    """Return data csv files list from data folder."""
    return sorted((file for file in os.listdir(CSV_DIR) if file.endswith('.csv')), reverse=reverse)


def get_parquet_data_files(reverse=False):
    """Return data parquet files list from data folder."""
    return sorted(
        (file for file in os.listdir(PARQUET_DIR) if file.endswith('.parquet')), reverse=reverse
    )


def get_first_file_date():
    """Return older file date."""
    return get_parquet_data_files()[0][:10]


def get_start_files(sn_dict):
    """Return serial numbers first appearance in data files."""
    print('\n---Looking for start file...---')
    data_files = get_parquet_data_files()
    process_file_name = f'start_file_{data_files[0][:10]}.json'

    # Get Info from old run
    if os.path.isfile(PROCESS_DIR + process_file_name):
        print(f'Found process file : {process_file_name}')
        with open(PROCESS_DIR + process_file_name, 'r', encoding='utf-8') as process_file:
            sn_dict = json.load(process_file)

    else:
        for serial_number, sn_info in sn_dict.items():
            sn_info['start_file'] = None
        sn_not_computed = list(sn_dict.keys())

        # Compute start file for SNs that were not found in the process file

        print(f'\n{len(sn_not_computed)} still not computed')
        print('\nGetting start file')
        for data_file in tqdm(data_files):
            sn_found_list = get_start_files_process(sn_not_computed, data_file)
            for sn_found in sn_found_list:
                sn_not_computed.remove(sn_found)
                sn_info = sn_dict[sn_found]
                if sn_info['start_file'] is None or datetime.strptime(
                    data_file[:10], '%Y-%m-%d'
                ) < datetime.strptime(sn_info['start_file'][:10], '%Y-%m-%d'):
                    sn_info['start_file'] = data_file

        # Saving for next run
        os.makedirs('process', exist_ok=True)
        with open(PROCESS_DIR + process_file_name, 'w', encoding='utf-8') as process_file:
            json.dump(sn_dict, process_file)

    # Remove SNs that have their start file in the first data file
    sn_dict = {
        serial_number: sn_info
        for serial_number, sn_info in sn_dict.items()
        if sn_info['start_file'] != data_files[0]
    }

    # Display
    print(f'{len(sn_dict)} valid sn will be computed')

    return sn_dict


def get_start_files_process(sn_to_process, data_file):
    """Return serial number first appearance in data files."""
    dataframe = parquet_to_dataframe(data_file)
    existing_serial_numbers = dataframe['serial_number'].values
    sn_found = list(set(sn_to_process) & set(existing_serial_numbers))

    return sn_found


def parse_file(file_path, serial_numbers):
    """Parse input csv file from BackBlaze."""
    dataframe = parquet_to_dataframe(file_path)
    mask = dataframe['serial_number'].isin(serial_numbers)
    results_df = dataframe.loc[mask]

    if results_df.empty:
        return None
    return results_df


def parse_files(files_to_open):
    """Parse input csv files from BackBlaze."""
    data_files = get_parquet_data_files()
    process_file_name = f'parsed_data_{data_files[0][:10]}.pickle'
    print('\n---Opening files to get history---')

    # Get Info from old run
    if os.path.isfile(PROCESS_DIR + process_file_name):
        with open(PROCESS_DIR + process_file_name, 'rb') as process_file:
            print(f'Found process file : {process_file_name}')
            results_df = pickle.load(process_file)
    else:
        results_list = []
        for filename, serial_numbers in tqdm(files_to_open.items()):
            data = parse_file(filename, serial_numbers)
            if data is not None:
                results_list.append(data)
        if not results_list:
            print('Parsing failed. No data available')
            return None
        results_df = pd.concat(results_list, ignore_index=True)
        results_df['date'] = pd.to_datetime(results_df['date'])

        # Saving for next run
        with open(PROCESS_DIR + process_file_name, 'wb') as process_file:
            pickle.dump(results_df, process_file)

    return results_df


def merge_lists(list1, list2):
    """Merge two lists together without duplicates."""
    merged_list = list1 + list2
    merged_list = list(dict.fromkeys(merged_list))
    return merged_list


def get_failed_serial_number_from_file(file):
    """Get failed serial numbers list from file."""
    serial_numbers = []

    dataframe = parquet_to_dataframe(file)
    failures_dataframe = dataframe[(dataframe['failure'] == 1)]
    for index, _ in failures_dataframe.iterrows():
        serial_numbers.append(dataframe.iloc[index]['serial_number'])

    return serial_numbers


def get_failed_serial_number_from_files(files_to_process):
    """Check failures presence in dataframe."""
    sn_dict = {}
    data_files = get_parquet_data_files()
    process_file_name = f'failed_sn_{data_files[0][:10]}.json'
    print('\n---Getting failed sn...---')

    # Get Info from old run
    if os.path.isfile(PROCESS_DIR + process_file_name):
        print(f'Found process file : {process_file_name}')
        with open(PROCESS_DIR + process_file_name, 'r', encoding='utf-8') as process_file:
            sn_dict = json.load(process_file)
    else:
        for file in tqdm(files_to_process):
            serial_numbers = get_failed_serial_number_from_file(file)
            if serial_numbers:
                for serial_number in serial_numbers:
                    if serial_number not in sn_dict:
                        sn_dict[serial_number] = {'file': file}
                    elif isinstance(sn_dict[serial_number], str):
                        sn_dict[serial_number] = {'file': file}
                    elif datetime.fromisoformat(
                        sn_dict[serial_number]['file'][:10]
                    ) < datetime.fromisoformat(file[:10]):
                        sn_dict[serial_number]['file'] = file

        # Saving for next run
        with open(PROCESS_DIR + process_file_name, 'w', encoding='utf-8') as process_file:
            json.dump(sn_dict, process_file, indent=4)

    print(f'\n{len(sn_dict)} serial numbers found')
    return sn_dict


def get_sn_from_file(file, sns_to_check):
    """Get present sn from data file."""
    dataframe = parquet_to_dataframe(file)
    mask = dataframe['serial_number'].isin(sns_to_check)
    strange_serial_numbers = dataframe.loc[mask]['serial_number'].tolist()

    return strange_serial_numbers


def remove_strange_behaviors(sn_dict: dict):
    """Remove strange failure behaviors from sn_dict."""
    data_files = get_parquet_data_files(reverse=True)
    process_file_name = f'strange_behaviors_{data_files[-1][:10]}.json'
    print('\n---Getting strange behaviors...---')
    sns_to_check = list(sn_dict.keys())

    # Get Info from old run
    if os.path.isfile(PROCESS_DIR + process_file_name):
        print(f'Found process file : {process_file_name}')
        with open(PROCESS_DIR + process_file_name, 'r', encoding='utf-8') as process_file:
            sn_dict = json.load(process_file)
    else:

        for val in sn_dict.values():
            val['strange'] = None

        for file in tqdm(data_files):
            candidates = get_sn_from_file(file, sns_to_check)
            if candidates:
                for serial_number in candidates:
                    if datetime.fromisoformat(
                        sn_dict[serial_number]['file'][:10]
                    ) < datetime.fromisoformat(file[:10]):
                        sn_dict[serial_number]['strange'] = file
                        # print(f"{serial_number} died on {sn_dict[serial_number]['file']} but still working on {file}")
                    sns_to_check.remove(serial_number)

        # Saving for next run
        with open(PROCESS_DIR + process_file_name, 'w', encoding='utf-8') as process_file:
            json.dump(sn_dict, process_file, indent=4)

    # Remove SNs that have their start file in the first data file
    sn_dict = {
        serial_number: sn_info
        for serial_number, sn_info in sn_dict.items()
        if sn_info['strange'] is None
    }

    print(f'{len(sn_dict)} still processable')
    return sn_dict


def get_files_to_open(sn_dict, history_length_recent, history_length_old):
    """Return list of files to open."""
    print('\n---Getting files to open---')
    data_files = get_parquet_data_files()
    process_file_name = f'files_to_open_{data_files[0][:10]}.json'
    files_to_open = {}

    if os.path.isfile(PROCESS_DIR + process_file_name):
        with open(PROCESS_DIR + process_file_name, 'r', encoding='utf-8') as process_file:
            print(f'Found process file : {process_file_name}')
            return json.load(process_file)

    for serial_number, info_dict in tqdm(sn_dict.items()):
        # Most recent history
        failure_date = datetime.strptime(info_dict['file'][:10], '%Y-%m-%d')
        for idx in range(history_length_recent):
            file_to_open = (failure_date - timedelta(days=idx)).strftime('%Y-%m-%d') + '.parquet'
            if file_to_open in data_files:
                if file_to_open in files_to_open:
                    files_to_open[file_to_open].append(serial_number)
                else:
                    files_to_open[file_to_open] = [serial_number]

        # Older history
        start_date = datetime.strptime(info_dict['start_file'][:10], '%Y-%m-%d')
        for idx in range(history_length_old):
            file_to_open = (start_date + timedelta(days=idx)).strftime('%Y-%m-%d') + '.parquet'
            if file_to_open in data_files:
                if file_to_open in files_to_open:
                    files_to_open[file_to_open].append(serial_number)
                else:
                    files_to_open[file_to_open] = [serial_number]

    with open(PROCESS_DIR + process_file_name, 'w', encoding='utf-8') as process_file:
        json.dump(files_to_open, process_file, indent=4)

    print(f'{len(files_to_open.keys())} files to open')

    return files_to_open


def create_csv_file(serial_number, sn_dict, disk_df):
    """Generate csv file."""
    result_path = f'results/{get_first_file_date()}/'
    if serial_number not in sn_dict.keys():
        return
    if sn_dict[serial_number]['result_filename'] is None:
        return
    os.makedirs(result_path, exist_ok=True)
    disk_df = disk_df.sort_values(by='date', ascending=False)
    result_filename = sn_dict[serial_number]['result_filename']
    disk_df.to_csv(
        result_path + result_filename,
        sep='\t',
        decimal=',',
    )


def create_csv_files(sn_dict, results_df):
    """Generate csv files."""
    print('\n---Creating csv files...---')
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


def process(history_length_recent, history_length_old, failure_start_date):
    """Process data_files."""
    # Variables
    convert_csvs_to_parquets()
    data_files = get_parquet_data_files(True)
    files_to_process = data_files
    try:
        files_to_process = data_files[0 : data_files.index(failure_start_date + '.parquet')]
    except (Exception,):  # pylint: disable=broad-except
        print(f'Error with arg :{failure_start_date}')

    # Display
    text1 = f'Computing files from {data_files[-1][:10]} to {data_files[0][:10]}'
    text2 = f'Looking for failures from {failure_start_date} to {data_files[0][:10]}'
    line = '-' * max(len(text1), len(text2))
    print(line)
    print(text1)
    print(text2)
    print(line)

    # Get failed serial-numbers
    sn_dict = get_failed_serial_number_from_files(files_to_process)
    if not sn_dict:
        print('No sn found !')
        sys.exit(1)

    # look for first apparition date
    sn_dict = get_start_files(sn_dict)

    # Remove strange behaviors (failure but disk still working ??)
    sn_dict = remove_strange_behaviors(sn_dict)

    # Set result filename
    sn_dict = set_result_filename(sn_dict, history_length_recent, history_length_old)

    # Skip all serial numbers already processed
    for serial_number in list(sn_dict.keys()).copy():
        if sn_dict[serial_number]['result_filename'] is None:
            del sn_dict[serial_number]
        elif os.path.isfile(
            f'results/{data_files[-1][:10]}/' + sn_dict[serial_number]['result_filename']
        ):
            del sn_dict[serial_number]
    if not bool(sn_dict):
        print('\nAll serial numbers csv files exist in result folder\n\n')
        sys.exit(1)

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
        sys.exit(1)

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
        '--failure_start_date',
        type=str,
        default=None,
        help='A partir de quelle date commencer la recherche de failures ? (format YYYY-mm-dd)',
    )

    args = parser.parse_args()

    os.makedirs(PROCESS_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(PARQUET_DIR, exist_ok=True)

    process(args.history_length_recent, args.history_length_old, args.failure_start_date)


if __name__ == '__main__':
    main()
