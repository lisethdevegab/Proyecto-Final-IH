import pandas as pd
import numpy as np 

import ETL.extraccion
import ETL.limpieza


def open_file(file):
    dfemi = ETL.extraccion.load_data(file)
    return dfemi


def cleaning(dfemi):
    dfemi = ETL.limpieza.separate_col(dfemi,['Location Coordinates'])
    dfemi = ETL.limpieza.medianfornul(dfemi,['Number of Children'])
    dfemi = ETL.limpieza.medianfornul(dfemi,['Number of Survivors'])
    dfemi = ETL.limpieza.medianfornul(dfemi,['Number of Females'])
    dfemi = ETL.limpieza.medianfornul(dfemi, ['Number of Males'])
    dfemi = ETL.limpieza.medianfornul(dfemi, ['Minimum Estimated Number of Missing'])
    dfemi = ETL.limpieza.medianfornul(dfemi,['Number Dead'])
    dfemi = ETL.limpieza.remove_nonecessary(dfemi)
    dfemi = ETL.limpieza.new_values(dfemi, 'Cause of Death', dict1)
    dfemi = ETL.limpieza.new_values(dfemi, 'Reported Month', dict2)
    dfemi = ETL.limpieza.for_int(dfemi, 'Total Dead and Missing')
    dfemi = ETL.limpieza.fillna_col(dfemi,'UNSD Geographical Grouping', 'Region of Incident')
    dfemi = ETL.limpieza.fillna_col(dfemi, 'Migration Route', 'UNSD Geographical Grouping')
    dfemi = ETL.limpieza.fillna_col(dfemi, 'Location Description', 'Region of Incident')
    dfemi = ETL.limpieza.fillna_col(dfemi, 'Information Source', 'Source Quality')
    dfemi = ETL.limpieza.delete_null(dfemi, 'Cause of Death')
    dfemi = ETL.limpieza.take_day(dfemi, 'Reported Date')
    dfemi = ETL.limpieza.completadate(dfemi,'Reported day', 'Reported Month', 'Reported Year')



dict1 = {'Train Accide':'Train Accident', 'Unknow':'Unknown','Unknown (found dead next to train tracks), Train Accide': 'Train Accident', 
         'Train Accident, Unknown (found dead next to train tracks)': 'Train Accident', 
         'Unknown (decomposed remains)': 'Decomposed Remains', 
         'Unknown (found dead next to train tracks)': 'Found dead next to train tracks', 
         'Unknown (found dead next to train tracks), Train Accident': 'Train Accident', 
         'Unknown (found on dinghy)': 'Found on dinghy', 
         'Unknown (multiple blunt force injuries)': 'Multiple blunt force injuries', 
         'Unknown (mummified and skeletal remains)': 'Mummified and skeletal remains', 
         'Unknown (mummified remains)': 'Mummified remains', 
         'Unknown (skeletal remains)': 'Skeletal remains', 'Unknown (violence)': 'Violence'}

dict2 = {'Apr' : 4, 'Aug': 8, 'Dec': 12, 'Feb': 2, 'Ja': 1, 'Ju': 6, 'Jul': 7, 'Mar': 3, 'May': 5, 'Nov': 11, 'Oc':10, 'Sep':9}

dict3 = {'1,022': '1022'}


def download_csv(df):
        return df.to_csv(r'../output/dataemi.csv')


def main():
    d = open_file('../input/missingmigrantes.csv')
    d_clean = cleaning(d)
    download_csv(d_clean)


if __name__ == '__main__':
    main()
    