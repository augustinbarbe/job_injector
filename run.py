from job_runner.job import Runner
import os

# provide the path of the data files
CURRENT_DIR = os.getcwd()
input_file = os.path.join(CURRENT_DIR, 'data/input.csv')
result_file = os.path.join(CURRENT_DIR, 'data/results.csv')


# instantiate a runner
runner = Runner()

# register your pre-processing operation and provide data files with a map
@runner.preprocess(input_files={'input':input_file, 'result':result_file})
def data_prep():

    #retrieve dataframe from name of data set provided
    df = runner.get_df_by_name('input')
    df_lab = runner.get_df_by_name('result')

    #play with data as dataframe
    df2 = df.drop('size', axis=1)
    df2['result'] = df_lab['value']

    print(df2)
    #return the dataframe you want save for later use - in training a model for example
    return {'features': df2}

# to run locally
if __name__ == "__main__":
	runner.run()
