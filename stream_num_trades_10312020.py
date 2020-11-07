import os
import sys
import time
import logging, threading
import pandas as pd
from glob import glob
from datetime import datetime
from filepath_server import *
import xlwings as xw

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)

formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')


def LOG_insert(file_name, format, text, level):
    file = os.path.join(LOG_FILE_DIRECTORY, file_name)
    infoLog = logging.FileHandler(file)
    infoLog.setFormatter(format)
    logger = logging.getLogger(file)
    logger.setLevel(level)

    if not logger.handlers:
        logger.addHandler(infoLog)
        if (level == logging.INFO):
            logger.info(text)
        if (level == logging.ERROR):
            logger.error(text)
        if (level == logging.WARNING):
            logger.warning(text)

    infoLog.close()
    logger.removeHandler(infoLog)

    return


def return_factor_buy(element):
    return element.agg({6: sum}).values[0] / element.agg({7: sum}).values[0]


# weight average
def return_ratio(name):
    name[9] = (name[5] * name[6])
    return name.agg({9: sum}).values[0] // name.agg({6: sum}).values[0]


# last min weight average
def return_ratio_final(name):
    name['product'] = (name['sum_qty'] * name['qty_weighted_price'])
    return name.agg({'product': sum}).values[0] // name.agg({'sum_qty': sum}).values[0]


def return_result_by_odr_T(df_stream, token):
    frame = {}
    filtered_df_stream = df_stream[df_stream[1] == 'T']
    frame['row_count'] = filtered_df_stream.groupby([7, 4]).agg('count')[0]
    frame['sum_qty'] = filtered_df_stream.groupby([7, 4]).agg({6: sum})[6]
    frame['qty_weighted_price'] = filtered_df_stream.groupby([7, 4]).apply(return_ratio)
    result = pd.DataFrame(frame).reset_index()
    result[4] = result[4].astype(int)
    result.rename(columns={7: 'Date', 4: 'token'}, inplace=True)
    result = pd.merge(result, token, on='token')
    return result


def row_count_N_and_B(stream, token):
    frame = {}
    filtered_df_stream = stream[(stream[1] == 'N') & (stream[4] == 'B')]
    frame['row_count_buy'] = filtered_df_stream.groupby([7, 3]).agg('count')[0]
    result = pd.DataFrame(frame).reset_index()
    result[3] = result[3].astype(int)
    result.rename(columns={7: 'Date', 3: 'token'}, inplace=True)
    result = pd.merge(result, token, on='token')
    return result


def row_count_N_and_S(stream, token):
    frame = {}
    filtered_df_stream = stream[(stream[1] == 'N') & (stream[4] == 'S')]
    frame['row_count_sell'] = filtered_df_stream.groupby([7, 3]).agg('count')[0]
    result = pd.DataFrame(frame).reset_index()
    result[3] = result[3].astype(int)
    result.rename(columns={7: 'Date', 3: 'token'}, inplace=True)
    result = pd.merge(result, token, on='token')
    return result


def start_thread(df_norman_trd, interval, token, dumper_file, LOG_FILE_NAME):
    print("**************************** thread  started ***************************")

    resulting_filepath = os.path.join(TRADE_WATCH, "tradeWatch_historical.csv")
    skip_row_path = os.path.join(SKIP_ROWS_NUM_DIRECTORY, str(str(dumper_file).split('\\')[-1]).split('.')[0]) + ".csv"
    print(skip_row_path, "skip row path")

    header = True

    read_row = 0
    if os.path.exists(skip_row_path):
        read_row = pd.read_csv(skip_row_path)['read_row']

    last_min_df = pd.DataFrame()

    while datetime.now() <= datetime(year=datetime.today().year, day=datetime.today().day, month=datetime.today().month,
                                     hour=23, minute=31, second=00):
        # import pdb;pdb.set_trace()

        if not header:
            read_row = pd.read_csv(skip_row_path)['read_row']
            read_row = int(read_row[0])
        try:
            chunk = pd.read_csv(dumper_file, header=None, chunksize=1000000, skiprows=read_row)

            for df_stream in chunk:

                df_stream[7] = pd.to_datetime(df_stream[7]).dt.floor("{}".format("1min" if interval == 1 else "5min"))
                # df_stream[(df_stream[3] == 29113) or (df_stream[4] == 29113)]

                last_min = df_stream[7].max()

                if len(df_stream):

                    df_1 = return_result_by_odr_T(df_stream, token)

                    df_2 = row_count_N_and_B(df_stream, token)

                    df_3 = row_count_N_and_S(df_stream, token)

                    df_4 = pd.merge(df_1, df_2, on=['Date', 'token', 'Symbol'])

                    df_5 = pd.merge(df_4, df_3, on=['Date', 'token', 'Symbol'])

                    if len(last_min_df):
                        df_5 = pd.concat([df_5, last_min_df])
                        last_min_df = pd.DataFrame()
                        df_5['qty_weighted_price'] = df_5.groupby(['Date', 'Token']).apply(return_ratio_final)

                    if (df_5['Date'] != last_min).any():

                        df_5 = df_5[(df_5['Date'] != last_min)]
                        # print(df_5.head,'all')
                        last_min_df = df_5[~(df_5['Date'] != last_min)]
                        # print(last_min_df,'lm')

                        df_5.to_csv(resulting_filepath, mode='a', index=False, header=False)  # keep same

                        pd.DataFrame([{'read_row': df_stream[0].max()}]).to_csv(skip_row_path, index=False)

                        try:
                            df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

                            df_6 = pd.merge(df_5, df_norman_trd, on='Symbol')

                            df_6 = df_6[(df_6['row_count'] > 3 * df_6['normean_trd']) & (df_6['normean_trd'] > 0)]

                            df_6.to_csv(os.path.join(TRADE_WATCH, "trade_result_final_{}.csv".format(DATE_TODAY)),
                                        mode='a', index=False, header=False)
                            header = False

                            # import pdb;pdb.set_trace()
                            df_7 = pd.DataFrame()
                            df_7 = pd.read_csv(
                                os.path.join(TRADE_WATCH, "trade_result_final_{}.csv".format(DATE_TODAY)), header=None)

                            data_excel_file = "E:\\DUMPER\\tradewatch\\not_sure.xlsx"
                            wb = xw.Book(data_excel_file)
                            wb.sheets("Dump").range("A2").options(index=False, header=False).value = df_7

                            df_6 = df_6.set_index(['token'])

                            df_8 = pd.DataFrame()
                            df_8['repeat'] = df_7.groupby([1]).agg('count')[0]
                            # df_8['factor_buy'] = df_7.groupby([1]).apply(return_factor_buy)

                            df_6 = pd.concat([df_6, df_8], axis=1)

                            df_6['ratio_qty'] = df_6['sum_qty'] // df_6['normean_quantity']
                            df_6.dropna(subset=['Symbol'], inplace=True)

                            # df_6.to_csv(os.path.join(TRADE_WATCH,"df7_{}.csv".format(DATE_TODAY)),index=False, header=False)

                            # check num of traded in X min should be 30 , and
                            df_6 = df_6[(df_6['repeat'] >= 4) & (df_6['row_count'] >= 20) & (
                                        df_6['sum_qty'] >= 2 * df_6['normean_quantity'])]
                            # df_6 = df_6[(df_6['sum_qty'] >= df_6['normean_quantity'])]
                            if len(df_6):
                                # wb.sheets("Current").range("A1").options(index=False).value = df_6
                                print(df_6)

                            # time.sleep(interval * 55)
                        except Exception as e:
                            print(e)
                            LOG_insert(LOG_FILE_NAME, formatLOG, str(e) + "error in df_6 or 7", logging.INFO)


                    else:
                        last_min_df = df_5
                        # df_5.to_csv(resulting_filepath, mode='a', index=False, header=header) # chage path
                        pd.DataFrame([{'read_row': df_stream[0].max()}]).to_csv(skip_row_path, index=False)

                    # header = False

                else:
                    LOG_insert(LOG_FILE_NAME, formatLOG, "Please wait for Data", logging.INFO)



        except Exception as e:
            LOG_insert(LOG_FILE_NAME, formatLOG, str(e) + "file is over", logging.ERROR)


def filtering_stream_data():
    try:
        interval = int(input("Enter Time Interval in min eg. 1 0r 5"))
    except Exception as e:
        interval = None
        LOG_insert(LOG_FILE_NAME, formatLOG, e, logging.ERROR)

    if interval in [1, 5]:

        division_factor = 75 if int(interval) == 5 else 375

        df_norman_trd = pd.read_csv(NORMEN_TRD_FILE)

        df_norman_trd['normean_quantity'] = df_norman_trd['normean_quantity'].apply(lambda x: (x // division_factor))

        df_norman_trd['normean_trd'] = df_norman_trd['normean_trd'].apply(lambda x: (x // division_factor))

        token = pd.read_csv(TOKEN_SECURITY_FILE)

        files = glob(os.path.join(DUMPER_FILE_DIRECTORY, 'DUMP_*.csv_Clean'))

        for dumper_file in files:
            if str(str(dumper_file).split('\\')[-1]).split('_')[1] == DATE_TODAY:  # or 1==1:
                thread = threading.Thread(target=start_thread,
                                          args=(df_norman_trd, interval, token, dumper_file, LOG_FILE_NAME))
                thread.start()
            else:
                print("File for date {} not found".format(DATE_TODAY))
                LOG_insert(LOG_FILE_NAME, formatLOG, "File for date {} not found".format(DATE_TODAY), logging.INFO)


    else:
        LOG_insert(LOG_FILE_NAME, formatLOG, "time interval provided is not 1 minute or 5 minute", logging.INFO)


LOG_FILE_NAME = str(str(os.path.abspath(__file__)).split('\\')[-1]).split('.')[0] + ".log"
DATE_TODAY = ''.join(str(datetime.today().date()).split('-'))

try:
    filtering_stream_data()
except Exception as e:
    LOG_insert(LOG_FILE_NAME, formatLOG, e, logging.ERROR)


