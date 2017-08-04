import os
import sqlite3, csv
import pandas as pd

DEFAULT_CONFIG = {

    'database': os.getcwd(),
    'input_chunk_size': 2,
    'df_chunk_size': 5
}

##helpers
def create_table(cursor, name, header):
    try:
        qry = 'drop table ' + name
        cursor.execute(qry)
    except:
        pass

    qry = 'create table ' + name + ' (' + \
          ','.join([column + ' real' for column in header]) + ')'
    cursor.execute(qry)


def insert_batch(cursor, name, header, reader, size):
    i, pack = 0, []
    qry = 'insert into ' + name + ' values( ' + \
          ','.join(['?' for name in header]) + ')'
    for line in reader:
        if i > size:
            cursor.executemany(qry, pack)
            i = 0
            pack = []
        pack.append(line)
        i += 1
    cursor.executemany(qry, pack)


##THE MAIN BUSINESS
class Runner:
    def __init__(self):
        self._mapping = {}
        self._config = DEFAULT_CONFIG
        self._connexion = sqlite3.connect(os.path.join(self._config['database'], 'data.db'))
        self._preprocess_load = {}

    def preprocess(self, input_files={}):
        def wrapper(function):
            self._mapping['preprocess'] = function
            for name, input_file in input_files.items():
                if os.path.exists(input_file):
                    self._preprocess_load[name] = input_file
                else:
                    raise RunnerException("File " + input_file + ' does not exist.')

        return wrapper

    def __getitem__(self, key):
        return self._mapping[key]

    def _load_files_to_db(self):
        cursor = self._connexion.cursor()
        for name, file in self._preprocess_load.items():
            f = open(file, 'r')
            reader = csv.reader(f)
            header = next(reader)

            #drop and create table
            create_table(cursor, name, header)
            self._connexion.commit()

            # insert by batch
            insert_batch(cursor, name, header, reader, self._config['input_chunk_size'])
            self._connexion.commit()

    def _load_result_to_db(self, result):
        cursor = self._connexion.cursor()
        for name, df in result.items():
            header = list(df.columns)
            create_table(cursor, name, header)
            self._connexion.commit()

            # insert by batch
            reader = df.values
            insert_batch(cursor, name, header, reader, self._config['input_chunk_size'])
            self._connexion.commit()

    def get_df_by_name(self, name):
        cursor = self._connexion.cursor()
        qry = 'select * from ' + name

        cursor.execute(qry)

        names = [description[0] for description in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns = names)
        return df

    def get(self, key):
        return self._mapping.get(key)

    def execute_preprocessing(self):
        if self.get('preprocess'):
            self._load_files_to_db()
            result = self['preprocess']()
            self._load_result_to_db(result)

        else:
            raise PreprocException("No preprocessing function registered.")

    def run(self):
        self.execute_preprocessing()
        print('end of pre-processing')





class RunnerException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class PreprocException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
