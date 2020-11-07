import sys
import threading,io,time
from datetime import datetime
#from .get_stock_metadata import HOLIDAY_FILE_PATH
import dateutil
import paramiko
import logging
import pandas as pd
import datetime as dt
import subprocess
from os import path

formatLOG = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

def LOG_insert(file, format, text, level):
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

# Opening a file does NOT implicitly read nor load its contents. Even when you do so using Python's context management protocol (the with keyword)

def get_connected():
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('192.168.100.19', port=22, username='ankit', password='Ankit@1234', timeout=20)
        print("connected successfully!")
        return ssh

    except Exception as e:
        LOG_insert("E:\\DUMPER\\file.log", formatLOG, e, logging.INFO)
        print(e)
        return None



def holiday_check(path):
    try:
        hol_df = pd.read_csv(path)
        holiday_list = [dateutil.parser.parse(dat).date() for dat in hol_df['Holiday'].values]
        "Weekday Return day of the week, where Monday == 0 ... Sunday == 6."
        date_today = datetime.today().date()
        if date_today not in holiday_list and date_today.weekday() != 6 and date_today.weekday() != 5:
            return True
        else:return False
    except Exception as e:
        LOG_insert("E:\\DUMPER\\file.log", formatLOG, e, logging.INFO)

def del_last_day_file(i,ssh):
    try:
        i = i+1
        # Files in Linux
        past_date = (datetime.today() - dt.timedelta(days=1)).date()
        past_date_str = ''.join(str(past_date).split('-'))
        stream_file_path="//home/data/stream_{}_{}.csv".format(str(i), past_date_str)
        output_file_path="//home/data/stream_{}_{}.DAT".format(str(i), past_date_str)
        ssh.exec_command('[ -f {} ] && rm {}'.format(stream_file_path,stream_file_path))
        ssh.exec_command('[ -f {} ] && rm {}'.format(output_file_path, output_file_path))
        
        # Files in windows
        final_path_file = "E:\\DUMPER\\DUMP_" + str(past_date_str)  +"_07300" + str(i)   +".csv"
        subprocess.call("del {}".format(final_path_file+"_i"), shell=True)
        subprocess.call("del {}".format(final_path_file), shell=True)
        return ssh
    except Exception as e:
        LOG_insert("E:\\DUMPER\\file.log", formatLOG, e, logging.INFO)
        return None

def download_stream(index,input_path,stream_file_path,output_path,break_time,final_path,ssh,final_path_file):
    ssh=del_last_day_file(index, ssh)
    ftp_client = ssh.open_sftp()
    print('run ssh')
    while datetime.now()<=break_time: # loop will break when time at server exceed 3:31:00 (10:01:00 at server) PM
        try:
            
            stdin, stdout, stderr = ssh.exec_command('end_var=0 ; [ -f {} ] && end_var=$(awk -F, \'{}\' {}) ; start_var=$end_var ; tail -1 {} > {} ; end_var=$(awk -F, \'{}\' {}) ; awk -F, -v a="$start_var" -v b="$end_var" \'{}\' {} > {}'.format(output_path, "{print$1}", output_path,input_path,output_path,"{print$1}",output_path,"{if($1>a && $1<=b)print$0}",input_path,stream_file_path))
            exit_status = stdout.channel.recv_exit_status()

            # it's writting it to an in memory buffer. In other words a chunk of RAM
            if path.exists(final_path):
                ftp_client.get(stream_file_path, final_path+"_i") 
                subprocess.call("E: && cd E:\\DUMPER && copy /B /Y {}+{}".format(final_path_file,final_path_file+"_i"), shell=True)

                #cleanse
                chunk = pd.read_csv(final_path+"_i", header = None, chunksize=1000000) #nrows=100000)
                for df_stream_token in chunk:
                    # drop columns not needed
                    df_stream_token['datetime'] = pd.to_datetime('1980-01-01 00:00:00') + pd.to_timedelta(df_stream_token[6])
                    df_stream_token = df_stream_token.drop([0, 1, 2, 3,6], axis=1)
                    df_stream_token = df_stream_token[df_stream_token[5]!="M"]
                    df_stream_token.to_csv(final_path +"_Clean", index=False, mode='a', header=False)
                    
                subprocess.call("del {}".format(final_path+"_i"), shell=True)
                
            else:
                
                ftp_client.get(stream_file_path, final_path)
                
                #cleanse
                chunk = pd.read_csv(final_path, header = None, chunksize=1000000) #nrows=100000)
                for df_stream_token in chunk:
                    # drop columns not needed
                    df_stream_token['datetime'] = pd.to_datetime('1980-01-01 00:00:00') + pd.to_timedelta(df_stream_token[6])
                    df_stream_token = df_stream_token.drop([0, 1, 2, 3,6], axis=1)
                    df_stream_token = df_stream_token[df_stream_token[5]!="M"]
                    df_stream_token.to_csv(final_path +"_Clean", index=False, mode='a', header=False)


        except Exception as e:
            # if there is no file at server or any other issue
            LOG_insert("E:\\DUMPER\\file.log", formatLOG, e, logging.INFO)
            logging.info(str(e))


    ftp_client.close()



def start_stream_download():
    try:
        # establishing connection with server
        ssh=get_connected()

        # removing hyphon (-) from date and making it continuous string for appending it in path
        date = ''.join(str(datetime.today().date()).split('-'))

        holiday_file_path="C:\\home\\holiday.csv"
        if 0==0: #holiday_check(holiday_file_path):
            for j in range(4):
                i=j+1
                break_time=datetime(year=datetime.today().year,day=datetime.today().day,month=datetime.today().month,hour=15,minute=31,second=00)
                input_file_path="//dwh1/extbles/tbt-ncash-str"+ str(i)  + "/DUMP_" + str(date)  +"_07300" + str(i)   +".DAT " #path of file at server
                stream_file_path = "//home/data/stream_{}_{}_Inc.csv".format(str(i),date)  # incremental file 
                output_file_path="//home/data/stream_{}_{}.DAT".format(str(i),date) # file to store last row number
                final_path = "E:\\DUMPER\\DUMP_" + str(date)  +"_07300" + str(i)   +".csv"  # it is the path of file in windows server where we need to append data getting from server
                final_path_file = "DUMP_" + str(date)  +"_07300" + str(i)   +".csv"
                print('thread start')
                thread=threading.Thread(target=download_stream,args=(j,input_file_path,stream_file_path,output_file_path,break_time,final_path,ssh,final_path_file))
                thread.start() #starting thread for each stream, here we have 4 stream

    except Exception as e:
        LOG_insert("E:\\DUMPER\\file.log", formatLOG, e, logging.INFO)

#start_stream_download()



def return_ratio(name):
    name[9]=(name[5]*name[6])
    return name.agg({9:sum}).values[0]//name.agg({6:sum}).values[0]


def return_result_by_odr_T(df_stream, interval,token):
    frame = {}
    filtered_df_stream = df_stream[df_stream[1] == 'T']
    filtered_df_stream[7] = filtered_df_stream[7].apply(lambda date: dateutil.parser.parse(date)).dt.floor("{}".format("1min" if interval == 1 else "5min"))
    last_min = filtered_df_stream[7].max()
    frame['row_count'] = filtered_df_stream[filtered_df_stream[7] != last_min].groupby([7,4]).agg('count')[0]
    frame['sum_qty'] = filtered_df_stream[filtered_df_stream[7] != last_min].groupby([7,4]).agg({6: sum})[6]
    frame['qty_weighted_price'] = filtered_df_stream[filtered_df_stream[7] != last_min].groupby([7,4]).apply(return_ratio)
    result = pd.DataFrame(frame).reset_index()
    result[4]=result[4].astype(int)
    result.rename(columns={7: 'Date',4:'token'}, inplace=True)
    result=pd.merge(result,token,on='token')
    return result

def row_count_N_and_B(stream,interval,token):
    frame = {}
    filtered_df_stream = stream[(stream[1]=='N') & (stream[4]=='B')]
    filtered_df_stream[7] = filtered_df_stream[7].apply(
        lambda date: dateutil.parser.parse(date)
    ).dt.floor(
        "{}".format("1min" if interval == 1 else "5min")
    )
    last_min = filtered_df_stream[7].max()
    frame['row_count_buy'] = filtered_df_stream[filtered_df_stream[7] != last_min].groupby([7,3]).agg('count')[0]
    result = pd.DataFrame(frame).reset_index()
    result[3] = result[3].astype(int)
    result.rename(columns={7: 'Date', 3: 'token'}, inplace=True)
    result = pd.merge(result, token, on='token')
    return result


def row_count_N_and_S(stream,interval,token):
    frame = {}
    filtered_df_stream = stream[(stream[1]=='N') & (stream[4]=='S')]
    filtered_df_stream[7] = filtered_df_stream[7].apply(
        lambda date: dateutil.parser.parse(date)
    ).dt.floor(
        "{}".format("1min" if interval == 1 else "5min")
    )
    last_min = filtered_df_stream[7].max()
    frame['row_count_sell'] = filtered_df_stream[filtered_df_stream[7] != last_min].groupby([7,3]).agg('count')[0]
    result = pd.DataFrame(frame).reset_index()
    result[3] = result[3].astype(int)
    result.rename(columns={7: 'Date', 3: 'token'}, inplace=True)
    result = pd.merge(result, token, on='token')
    return result


def filtering_stream_data():
    try:
        interval=int(input("Enter Time Interval in min eg. 1 0r 5"))
    except Exception as e:
        interval=None
        LOG_insert("file.log", formatLOG, e, logging.INFO)
    if interval in [1,5]:
        read_row=0

        division_factor=375 if interval ==1 else 75

        df_norman_trd=pd.read_csv("/home/shashank/workspace/trade/task2/ProjectDir/master/Normen_trd.csv")

        df_norman_trd['normean_quantity']=df_norman_trd['normean_quantity'].apply(lambda x: 2*(x//division_factor))

        df_norman_trd['normean_trd']=df_norman_trd['normean_trd'].apply(lambda x: 2*(x//division_factor))

        token = pd.read_csv("/home/shashank/workspace/trade/task2/ProjectDir/master/Token_security.csv")

        header=True

        while datetime.now()<=datetime(year=datetime.today().year,day=datetime.today().day,month=datetime.today().month,hour=15,minute=31,second=00):

            if not header:
                read_row=pd.read_csv("/home/shashank/workspace/trade/task2/ProjectDir/tradeWatch/row_skip.csv")['read_row']
                read_row=int(read_row[0])
            try:
                chunk = pd.read_csv(
                    "/Dumper/DUMP_Doc_1.csv",
                    header=None,
                    chunksize=1000000,
                    skiprows=read_row
                )
                for df_stream in chunk:
                    print("************************* Start New Session **************************************")
                    df_1 = return_result_by_odr_T(df_stream, interval, token)

                    print(df_1.head(2))

                    df_2 = row_count_N_and_B(df_stream, interval, token)

                    print(df_2.head(2))

                    df_3 = row_count_N_and_S(df_stream, interval, token)

                    print(df_3.head(3))

                    df_4 = pd.merge(df_1, df_2, on=['Date', 'token', 'Symbol'])

                    print(df_4.head(2))

                    df_5 = pd.merge(df_4, df_3, on=['Date', 'token', 'Symbol'])

                    print(df_5.head(2))

                    df_5.to_csv('/home/shashank/workspace/trade/task2/ProjectDir/tradeWatch/trade_result.csv', mode='a', index=False,header=header)

                    pd.DataFrame([{'read_row':df_stream[0].max()}]).to_csv("/home/shashank/workspace/trade/task2/ProjectDir/tradeWatch/row_skip.csv",index=False)

                    df_norman_trd.rename(columns={'Stock': 'Symbol'}, inplace=True)

                    df_6 = pd.merge(df_5, df_norman_trd, on='Symbol')

                    df_6 = df_6[df_6['row_count'] > df_6['normean_trd']]

                    print(df_6)

                    header = False

                    time.sleep(interval*60)
            except Exception as e:
                LOG_insert("file.log", formatLOG, e, logging.ERROR)


    else:
        LOG_insert("file.log", formatLOG, "time interval provided is not 1 minute or 5 minute", logging.INFO)

filtering_stream_data()

