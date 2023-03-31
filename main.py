from datetime import datetime
import pandas as pd
import os
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import argparse


def merge_dictionary(dict1, dict2):
    merged_dict = dict1.copy()
    for key, value in dict2.items():
        if key in merged_dict:
            if type(merged_dict[key]) is list:
                merged_dict[key].append(value)
            else:
                merged_dict[key] = [merged_dict[key], value]
        else:
            merged_dict[key] = value
    return merged_dict


def csv_to_dataframe(csv_name) -> pd.DataFrame:
    try:
        dataframe = pd.read_csv('data/' + csv_name)
        return dataframe
    except (Exception,):
        print(f'Cannot read {csv_name}')
        exit(1)
        return pd.DataFrame()


def parse_file(file_name, serial_numbers):
    interesting_rows = {}
    new_serial_numbers = []
    dataframe_it = csv_to_dataframe(file_name)

    failures_dataframe = dataframe_it[(dataframe_it["failure"] == 1)]
    if not failures_dataframe.empty:
        for index, row in failures_dataframe.iterrows():
            new_serial_number = dataframe_it.iloc[index]['serial_number']
            if new_serial_number not in serial_numbers:
                serial_numbers.append(new_serial_number)
                new_serial_numbers.append(new_serial_number)

    for serial_number in serial_numbers:
        interesting_row = dataframe_it.loc[dataframe_it['serial_number'] == serial_number]
        interesting_rows[serial_number] = interesting_row
    return interesting_rows, new_serial_numbers


def main():
    # Variables
    file_list = os.listdir('data')
    history_length = 90
    max_date = None

    parser = argparse.ArgumentParser(description='BackBlaze data parser.')
    parser.add_argument("-v", "--maxdate", help="max date")
    args = parser.parse_args()
    if args.maxdate:
        max_date = datetime.strptime(args.maxdate, '%Y-%m-%d')

    for idx, file in enumerate((sorted(file_list, reverse=True))):
        serial_numbers = []
        print(f'Computing {file}')
        if max_date is not None:
            if datetime.strptime(file[:10], '%Y-%m-%d') > max_date:
                continue

        dataframe = csv_to_dataframe(file)
        failures_dataframe = dataframe[(dataframe["failure"] == 1)]

        if not failures_dataframe.empty:
            for index, row in failures_dataframe.iterrows():
                serial_numbers.append(dataframe.iloc[index]['serial_number'])

            for serial_number in serial_numbers.copy():
                if os.path.exists(f'results/{file[:10]}/{serial_number}_{history_length}.csv'):
                    print(f'File already exists : results/{file[:10]}/{serial_number}_{history_length}.csv')
                    serial_numbers.remove(serial_number)
            if not serial_numbers:
                continue

            if idx + history_length >= len(file_list):
                print("Cannot continue, too few files to continue")
                break

            print(f'Serial Numbers with failures in {file} : {serial_numbers}')
            print(f'Parsing history from {file[:10]} to {sorted(file_list, reverse=True)[idx+history_length][:10]}')
            rows = {}

            with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
                futures = [executor.submit(parse_file, file_name, serial_numbers) for file_name in
                           sorted(file_list, reverse=True)[idx:idx+history_length]]
                for future in tqdm(as_completed(futures), total=len(futures)):
                    try:
                        data = future.result()
                        if data is not None:
                            rows = merge_dictionary(rows, data[0])
                    except Exception as exc:
                        print(f'Parsing generated an exception: {exc}')

            for serial_number in serial_numbers:
                print(f'Creating csv file for {serial_number}...')
                disk_data = pd.concat(rows[serial_number], ignore_index=True)
                disk_data['date'] = pd.to_datetime(disk_data['date'])
                disk_data = disk_data.sort_values(by='date', ascending=False)
                os.makedirs(f'results/{file[:10]}', exist_ok=True)
                disk_data.to_csv(f'results/{file[:10]}/{serial_number}_{history_length}.csv', sep="\t", decimal=",")
            print()


if __name__ == '__main__':
    main()
