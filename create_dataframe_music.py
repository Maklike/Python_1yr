import pandas as pd
import numpy as np
import os

from tqdm import tqdm_notebook as tqdm

import json
from pandas.io.json import json_normalize

from dateutil.relativedelta import relativedelta


def create_music_df(file_name):
    print("There are {} genres of music to process...\n\n".format(len(file_name)))

    # divide into proper columns & with and without dates
    cols = ['date_of_birth', 'date_of_death',
            'cause_of_deathLabel', 'manner_of_deathLabel',
            'country_of_citizenshipLabel', 'instrumentLabel',
            'occupationLabel', 'place_of_birthLabel',
            'place_of_deathLabel', 'sex_or_genderLabel']

    col_no_dates = ['cause_of_deathLabel', 'manner_of_deathLabel',
                    'country_of_citizenshipLabel', 'place_of_birthLabel',
                    'place_of_deathLabel', 'sex_or_genderLabel']

    col_dates = [i for i in cols if i in ['date_of_birth', 'date_of_death']]

    col_instr_occup = ['instrumentLabel', 'occupationLabel']

    # Create a final list of dataframes
    DB_final = pd.DataFrame()


    # BEGIN OPERATIONS
    for num, file in tqdm(enumerate(file_name)):
        # path separator for Win
        if os.path.sep == '\\':
            name = file.split('\\')[-1].split('.')[0][6:]
        # path separator for Mac/Linux
        else:
            name = file.split('/')[-1].split('.')[0][6:]

        print('******************* Starting the file: {}...'.format(name))

        with open(file, encoding='utf-8', errors='ignore') as json_data:
             data = json.load(json_data, strict=False)

        df_json = pd.DataFrame.from_dict(json_normalize(data), orient='columns')

        # check whether there are duplicated names

        if len(df_json['instance_ofLabel'].unique()) == len(df_json['instance_of'].unique()):
            print("---=== OK ===--- There are no persons with the same name in the DB")
        else:
            print("--------======WARNING======-------- There are persons with the same name in the DB")

        # CREATE AN EMPTY DATAFRAME

        df = pd.DataFrame(columns=['instance_ofLabel', 'instance_of']+cols,
                          index=range(len(df_json['instance_ofLabel'].unique())))

        df['instance_ofLabel'] = np.sort(df_json['instance_ofLabel'].unique())


        # FILL THE DATAFRAME WITH LIST OF LISTS
        print('Filling the data with list of lists from json...')
        for col in cols:
            df.loc[:, col] = [list(set(x)) for x in df_json.groupby('instance_ofLabel')[col].apply(list).values]

        # transform dates & no_dates columns

        for col in (col_dates + col_no_dates):
            df.loc[:, col] = df.loc[:, col].apply(lambda y: np.nan if pd.isnull(y).any() else y[0])

        for col in col_dates:
            df.loc[:, col] =  pd.to_datetime(df.loc[:, col], errors = 'coerce',
                                          format='%Y-%m-%dT%H:%M:%SZ')


        # add columns with occupation and instruments quantity

        for col in col_instr_occup:
            df.loc[:, 'quantity_'+col] = df.loc[:, col].apply(lambda y: np.nan if pd.isnull(y).all() else int(len(y)))


        # calculate years of living

        print('Calculating years of living...\n\n')
        for i in range(len(df['date_of_death'])):
            if not pd.isnull(df.loc[i, 'date_of_death']) and not pd.isnull(df.loc[i, 'date_of_birth']):
                rdelta = relativedelta(df.loc[i, 'date_of_death'], df.loc[i, 'date_of_birth'])
                df.loc[i, 'years_of_life'] = rdelta.years + round(rdelta.months/12, 2)
            else:
                df.loc[i, 'years_of_life'] = np.nan

        # Add database group name
        df.loc[:, 'group'] = name

        # Create df without 'instrumentLabel' and 'occupationLabel'
        # (otherwise there will be lists of items in the cells)
        df = df.loc[:, [i for i in df.columns if i not in ['instrumentLabel', 'occupationLabel']]]

        DB_final = DB_final.append(df, ignore_index=True)

    return DB_final
