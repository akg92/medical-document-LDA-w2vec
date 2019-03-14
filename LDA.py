
"""
Do the :) lda.
"""
from gensim.test.utils import common_texts
from gensim.corpora.dictionary import Dictionary
from gensim.models.ldamulticore import LdaModel
import pickle
import numpy as np
from data_reader import  Connection
import csv
import tf_idf
import pandas as pd
def lda_predict(texts,lda,num_topics=50):

    n = len(texts)
    m = num_topics
    represenation = np.zeros((n,m))
    common_dictionary = Dictionary(texts)


    i = 0
    for text in texts:
        corpus = common_dictionary.doc2bow(text)
        topics = lda.get_document_topics(corpus)

        for tindex in range(num_topics):
            represenation[i][topics[tindex][0]] = topics[tindex][1]
    return represenation

def lda_model_gen(texts,model_file_name,num_topics=50):
    # Create a corpus from a list of texts
    common_dictionary = Dictionary(texts)
    common_corpus = [common_dictionary.doc2bow(text) for text in common_texts]
    # Train the model on the corpus.
    lda = LdaModel(common_corpus, num_topics=num_topics,per_word_topics=200,iterations=100)
    """
        Save the model
    """
    with open(model_file_name,'wb') as f:
        pickle.dump(lda,f)

    ## create the representation

    representation = lda_predict(texts,lda)
    return lda,representation

def get_lda_mapping(texts,model_file):

    with open(model_file,'wb') as f:
        lda_model = pickle.load(f)

    representation = lda_predict(texts,model_file)
    return representation


def lda_train(time_from_admission,data_file='hadm_subject.csv',num_topics=50):

    df = pd.read_csv(data_file)
    hadms = set(df['hadm_id'].unique())


    con = Connection()
    ## return hadm textdict
    hadm_text_dict =con.get_comments_by_time(time_from_admission,hadms)
    model_file_name = "ldamodel_"+str(time_from_admission)+".model"
    texts = []
    hadm_ids = []
    ## create mapping
    for key,value in hadm_text_dict.items():
        texts.append(value)
        hadm_ids.append(key)
    texts = tf_idf.preprocess_documents(texts)

    ## removal of less patients with not enough data should be done here.

    lda,representation = lda_model_gen(texts,model_file_name)


    ## write data to file
    data_file_name = 'lda_feature_{}.csv'.format(time_from_admission)
    headers = ["hadm_id"]
    for i in range(num_topics):
        headers.append("topic_"+str(i))

    with open(data_file_name,'w') as f:
        csv_writer = csv.writer(f,delimiter=',')
        ## write header
        csv_writer.write_row(headers)
        for  i in range(len(texts)):
            row = list(representation[i,:])
            row.index(0,hadm_ids[i])

            csv_writer.write_row(row)











