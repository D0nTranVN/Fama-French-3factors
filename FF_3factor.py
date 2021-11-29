# import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
from random import randrange

#################################### IMPORT DATA ###############################
# import data
df_price = pd.read_csv("price.csv", skiprows=1, header=0,
                       index_col=0, parse_dates=True)

df_market_value = pd.read_csv("market_value.csv", skiprows=1, header=0,
                              index_col=0, parse_dates=True)

df_book_value = pd.read_csv("book_value.csv", skiprows=1, header=0,
                            index_col=0, parse_dates=True)

# import data to get the first row
df_price_fr = pd.read_csv("price.csv", header=None, index_col=0,
                          parse_dates=True, nrows=1)

df_market_value_fr = pd.read_csv("market_value.csv", header=None, index_col=0,
                                 parse_dates=True, nrows=1)

df_book_value_fr = pd.read_csv("book_value.csv", header=None, index_col=0,
                               parse_dates=True, nrows=1)

# get same column's name
df_price_fr.columns = df_price.columns
df_market_value_fr.columns = df_market_value.columns
df_book_value_fr.columns = df_book_value.columns

# append first row to the main dataframe
df_price = pd.concat([df_price_fr, df_price])
df_market_value = pd.concat([df_market_value_fr, df_market_value])
df_book_value = pd.concat([df_book_value_fr, df_book_value])


#################################################################################


################################ DEAL WITH DELIST ###############################

# create function to find delist date & column index
def extract_delist(df):
    delisted_index_list = []
    for i in df.iloc[0]:
        if 'DEAD - DELIST' in i:
            delisted_index = list(df.iloc[0]).index(i)
            delisted_index_list.append(delisted_index)
    delist_list = df.iloc[0][delisted_index_list]
    return delist_list, delisted_index_list


# create function to fill "NaN" on cell after delist date
def fix_delist(df, market_value=0):
    for i in range(0, len(extract_delist(df)[1])):
        if market_value == 1:
            delist_index = list(df.index).index(
                datetime.datetime.strptime(extract_delist(df)[0][i][-23:-15], '%d/%m/%y'))
        else:
            delist_index = list(df.index).index(datetime.datetime.strptime(extract_delist(df)[0][i][-8:], '%d/%m/%y'))

        for j in range(delist_index, df.shape[0]):
            df.iloc[j, extract_delist(df)[1][i]] = np.nan


# apply function
fix_delist(df_price)
fix_delist(df_market_value, 1)

# check


extract_delist(df_market_value)[0][0]
extract_delist(df_market_value)[1][0]

delist_index = list(df_price.index).index(datetime.datetime.strptime(extract_delist(df_price)[0][0][-8:], '%d/%m/%y'))
delist_index

df_price.iloc[delist_index, extract_delist(df_market_value)[1][0]]

df_market_value.iloc[4000, 413]


#################################################################################


################################ DEAL WITH ETF ##################################

# create function to find etf
def extract_etf(df):
    etf_index_list = []
    for i in df.iloc[0]:
        if 'ETF' in i:
            etf_index = list(df.iloc[0]).index(i)
            etf_index_list.append(etf_index)
    etf_list = df.iloc[0][etf_index_list]
    return etf_list, etf_index_list


# check etf columns
extract_etf(df_price)
extract_etf(df_market_value)
extract_etf(df_book_value)

# delete etf columns
df_price.drop(df_price.columns[extract_etf(df_price)[1]], axis=1, inplace=True)
df_market_value.drop(df_market_value.columns[extract_etf(df_market_value)[1]], axis=1, inplace=True)
df_book_value.drop(df_book_value.columns[extract_etf(df_book_value)[1]], axis=1, inplace=True)


#################################################################################


################################ DEAL WITH ERROR ###############################

# create function to extract error columns
def extract_error_columns(df):
    columns_name = df.columns
    for i in columns_name:
        if 'VT:' not in i:
            print(i)


# extract error columns
extract_error_columns(df_price)
extract_error_columns(df_market_value)
extract_error_columns(df_book_value)


# create clean columns function
def clean_columns(df):
    columns_name = df.columns

    # get column list without error (both long & short version)
    columns_without_error = []
    columns_without_error_short = []
    for i in columns_name:
        if 'VT:' in i:
            # append to list of column's name
            columns_without_error.append(i)

            # append to list of short column's name
            symbol = i.split(':')[1].split('(')[0]
            columns_without_error_short.append(symbol)

            # extract data without error
    df = df[columns_without_error]

    # rename columns into symbol
    df.columns = columns_without_error_short
    return df


# apply function to clean columns (note: run once only)
df_price = clean_columns(df_price)
df_market_value = clean_columns(df_market_value)
df_book_value = clean_columns(df_book_value)

# delete first row
df_price = df_price.drop(df_price.index[0])
df_market_value = df_market_value.drop(df_market_value.index[0])
df_book_value = df_book_value.drop(df_book_value.index[0])


#################################################################################


################################ RESAMPLE TO MONTHLY ############################

# create a function to reset datetime index
def reset_datetimeindex(df):
    df['date'] = pd.to_datetime(df.index)
    df = df.set_index('date')
    return df


# apply function
df_price = reset_datetimeindex(df_price)
df_market_value = reset_datetimeindex(df_market_value)
df_book_value = reset_datetimeindex(df_book_value)

# Resample to monthly data

df_monthly_price = df_price.resample('BM').ffill().iloc[1:-1]
df_monthly_market_value = df_market_value.resample('BM').ffill().iloc[1:-1]
df_monthly_book_value = df_book_value

# calculate return
df_monthly_return = df_monthly_price.pct_change()

#################################################################################


################################ COMMON COLUMNS ##################################

# get common column in all 3 dataframe
common_column = []
for i in df_monthly_return.columns:
    if (i in df_monthly_book_value.columns) and (i in df_monthly_market_value.columns):
        common_column.append(i)

# check
len(common_column)

# extract 3 dataframe with common column
df_monthly_return = df_monthly_return[common_column]
df_monthly_book_value = df_monthly_book_value[common_column]
df_monthly_market_value = df_monthly_market_value[common_column]


#################################################################################


################################ CLEAN ROWS ####################################

# create function to get empty row index
def get_empty_row_index(df):
    empty_row_index = []
    for i in range(0, df.shape[0]):
        if df.isnull().sum(axis=1)[i] == df.shape[1]:
            empty_row_index.append(i)

    return empty_row_index


# apply function
return_empty_row = get_empty_row_index(df_monthly_return)
market_value_empty_row = get_empty_row_index(df_monthly_market_value)
book_value_empty_row = get_empty_row_index(df_monthly_book_value)

# get common empty rows
common_empty_row = list(set(return_empty_row + market_value_empty_row + book_value_empty_row))

# delete empty rows
df_monthly_return = df_monthly_return.drop(df_monthly_return.index[common_empty_row])
df_monthly_book_value = df_monthly_book_value.drop(df_monthly_book_value.index[common_empty_row])
df_monthly_market_value = df_monthly_market_value.drop(df_monthly_market_value.index[common_empty_row])


#################################################################################


################################ RE-ARRANGE DATA ##################################

# create function to rearrange data
def rearrange_data(df):
    df_arranged = pd.DataFrame({'time': [],
                                'id': [],
                                'number': []})
    for i in df.columns:
        df_arranged_unit = pd.DataFrame({'time': df.index,
                                         'id': [i] * df.shape[0],
                                         'number': df[i].values})
        df_arranged = df_arranged.append(df_arranged_unit)
    return df_arranged


# create new dataframe for rearranged data
return_value = rearrange_data(df_monthly_return).reset_index(drop=True)
market_value = rearrange_data(df_monthly_market_value).reset_index(drop=True)
book_value = rearrange_data(df_monthly_book_value).reset_index(drop=True)

# merge into 1 dataframe
df = return_value
df.columns = ['time', 'id', 'return']
df['market_value'] = market_value['number']
df['book_value'] = book_value['number']

#################################################################################


################################ CALCULATE CORE DATA ############################

# calculate B/M ratio
df['b/m'] = np.nan
for i in range(0, df.shape[0]):
    if pd.isnull(df['market_value'][i]) == False:
        df['b/m'][i] = df['book_value'][i] / df['market_value'][i]

# create holder for 'size' & 'value' category
df['size'] = np.nan
df['value'] = np.nan


# create function to assign category
def assign_category(df):
    # get time list to assign category for each time point
    time_list = list(set(df['time'].values))

    for i in time_list:
        unit_df = df[df['time'] == i]

        # assign size (S or B)
        avg_market_value = unit_df['market_value'].mean()
        for j in unit_df.index:
            if pd.isnull(df['market_value'][j]) == False:
                if df['market_value'][j] > avg_market_value:
                    df['size'][j] = 'B'
                else:
                    df['size'][j] = 'S'

        # assign value (L, M & H)
        pct30_check_point = unit_df['b/m'].quantile(0.3)
        pct70_check_point = unit_df['b/m'].quantile(0.7)
        for k in unit_df.index:
            if pd.isnull(df['b/m'][k]) == False:
                if df['b/m'][k] < pct30_check_point:
                    df['value'][k] = 'L'
                elif df['b/m'][k] > pct70_check_point:
                    df['value'][k] = 'H'
                else:
                    df['value'][k] = 'M'
    return df


# apply function
df = assign_category(df)

# check with a unit_df
time_list = list(set(df['time'].values))
time_ex = time_list[0]

len(list(set(df['id'])))

time_ex

unit_df = df[df['time'] == time_ex]


#################################################################################


################################ CALCULATE CORE DATA ############################

# create function to calculate SMB & HML
def cal_SMB_HML(df):
    # create DataFrame for fama data
    df_fama = pd.DataFrame({'date': df[df['id'] == list(set(df['id']))[randrange(len(list(set(df['id']))))]]['time'],
                            'smb': [np.nan] * len(list(set(df['time']))),
                            'hml': [np.nan] * len(list(set(df['time'])))})

    df_fama.set_index('date', inplace=True)

    # get time list to assign category for each time point
    time_list = list(set(df['time'].values))

    for i in time_list:
        unit_df = df[df['time'] == i]

        avg_return_S = unit_df[unit_df['size'] == 'S']['return'].mean()
        avg_return_B = unit_df[unit_df['size'] == 'B']['return'].mean()
        SMB = avg_return_S - avg_return_B
        df_fama['smb'][i] = SMB

        avg_return_L = unit_df[unit_df['value'] == 'L']['return'].mean()
        avg_return_M = unit_df[unit_df['value'] == 'M']['return'].mean()
        avg_return_H = unit_df[unit_df['value'] == 'H']['return'].mean()
        HML = avg_return_H - avg_return_L
        df_fama['hml'][i] = HML

    return df_fama


df_fama = cal_SMB_HML(df)

df.to_csv('rearranged_data.csv')


#################################################################################


################################ MERGE WITH A PORTFOLIO ##########################

# Example with portfolio of "size = B" and "value = M"

def query_portfolio(df, df_fama):
    # get time list
    time_list = df_fama.index

    portfolio_return_list = []
    for i in time_list:
        port_return = df[(df['size'] == 'B') & (df['value'] == 'M')]['return'].mean()
        portfolio_return_list.append(port_return)

    # merger portfolio value to df_fama
    df_fama['portfolio_return'] = portfolio_return_list

    return df_fama


df_fama = query_portfolio(df, df_fama)
#################################################################################

df_fama.to_csv('fama_data.csv')