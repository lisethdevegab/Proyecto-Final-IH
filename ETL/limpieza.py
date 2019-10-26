import pandas as pd
import numpy as np 
import re

def separate_col(df, column):
    df[['Longitude','Latitude']] = df.pop('Location Coordinates').str.split(',\s*', n=1, expand=True)
    return df

def remove_nonecessary(df):
    whiteandspecial = lambda x: x.strip(r'.!?" " \n\t') if isinstance(x, str) else x
    return df.applymap(whiteandspecial)

def medianfornul(df, column):
    median_value= df[column].median()
    df[column]= df[column].fillna(median_value)
    df[column]= df[column].astype(int)
    return df

def new_values(df, column, dictionary):
    df[column].replace(dictionary, inplace=True)
    print(df)

def for_int(df, column):
    df[column]= df[column].astype(int)
    return df

def fillna_col(df,column1, column2):
    df[column1].fillna(df[column2], inplace=True)
    return df

def delete_null(df, column):
    df.dropna(subset=[column], inplace=True)
    return df

def take_day(df, column):
    df['Reported day'] = df[column].apply(lambda date: date.split(" ")[1])
    df['Reported day'] = df['Reported day'].str.replace(",","").astype(int)
    return df

def completadate(df, column1, column2, column3):
    df['Reported case'] = (pd.to_datetime(df[column1].astype(str) + '-' +
                                  df[column2].astype(str) + '-' +
                                  df[column3].astype(str)))
                                
    