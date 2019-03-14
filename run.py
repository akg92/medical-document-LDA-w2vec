"""
    Arguments are time interval eg 10, that means 10*240
"""

import LDA
import pandas as pd
import gc

def run_main(iteration,step=12,master_file='df_FINAL_MASTER_DATA.csv'):
    time_from_admission = iteration*step

    ## create hadm_subject_id file
    df = pd.read_csv(master_file)
    df = df[['subject_id','hadm_id']]
    df.to_csv('hadm_subject.csv',index=False)

    LDA.lda_train(time_from_admission)



if __name__ == '__main__':

    iterations = 20
    for i in range(iterations):
        run_main(i)
