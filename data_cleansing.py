import pandas as pd
import os



def int_records_lost(str_num:str)-> int:
    """ 
    Convert string of number into an approprirate floating integer.
    If comma found, then replace with empty string.
    if M found, replace with 000000.
    If ALL or nan found, then 0 as it's unidentified.

    :params str_num: a string of number
    :type str
    :return int
    """
    # check if it's nan (unable to use the value to analyse)
    # check if 'ALL' (unable to use the value to analyse)
    # then put to 0 meaning to ignore
    if isinstance(str_num, float) or str_num == 'ALL':
        return 0
    # 2,000,000 -> 2000000
    if ',' in str_num:
        return int(str_num.replace(',',''))
    # 25M -> 25000000
    if 'M' in str_num:
        return int( str_num[0: str_num.index('M')] + '0'*6 )
    # < 2000000 -> 2000000
    if '<' in str_num:
        return int( str_num[str_num.index('<')+1:] )
    # any other unidentified value at the moment
    else:
        return 0

def extract_month(string):
    """
    Extract month from Story column as Title format.
    If extracted month length is greater than 3, meaning the month is
    in full form, then return only first 3 letters as abbreviation,
    else return its data

    :param string: a string of Story column
    """
    month = string[0: string.find(' ')]
    if len(month) > 3: 
        return month[0:3].title()
    return month.title()

def lowercase_underscore_header(header):
    return header.replace(' ', '_').lower()

def remove_double_quote(entity):
    if entity.startswith('"'):
        entity = entity[1:]
    if entity.endswith('"'):
        entity = entity[:-1]
    return entity

def main():
    project_root = os.path.abspath(os.path.dirname(__file__))
    data_path = os.path.join(project_root, 'data')
    raw_data_path = os.path.join(data_path, 'Data Breaches - Raw Data.csv')

    print('Start data cleansing process...')
    df = pd.read_csv(raw_data_path, skiprows=[1], header=0)

    # format headers to be more consistent
    df.columns = list(map(lowercase_underscore_header, list(df.columns)))

    # clean up entity
    df['entity'] = df['entity'].apply(remove_double_quote)
    
    # convert `records lost` to float
    df['int_records_lost'] = df['records_lost'].apply(int_records_lost)

    # extract month
    df['month'] = df['story'].apply(extract_month)

    # fill NaN on interesting story column
    df['interesting_story'].fillna('n', inplace=True)

    # export new csv for analysis
    target_cols = ['entity','records_lost','int_records_lost','year','month','sector',
                    'method','data_sensitivity','source_name', 'interesting_story']
    clean_data_breaches = os.path.join(data_path, 'clean_data_breaches.csv')

    df[target_cols].to_csv(clean_data_breaches, index=False)
    print('Finish data cleansing process. DONE!')

if __name__ == "__main__":
    main()