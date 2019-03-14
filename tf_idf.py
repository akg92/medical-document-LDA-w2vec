import numpy as np
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from gensim.parsing.preprocessing import preprocess_documents
import pickle
import pandas as pd
from data_reader import  Connection
import os
patient_id_col='person_id'

def remove_stop_wrods(text):
    return preprocess_documents(text)

def tf_idf_texts(texts):
    texts = remove_stop_wrods(texts)
    dct = Dictionary(texts)  # fit dictionary
    corpus = [dct.doc2bow(line) for line in texts]  # convert corpus to BoW format

    model = TfidfModel(corpus)  # fit model
    return model,dct

def get_important_vocab(texts,n=500):
    model,dict = tf_idf_texts(texts)

    vector = np.zeros(len(texts))
    for i in range(len(texts)):
        vector = np.add(vector,model[i])

    indexes = np.argsort(vector)
    ## take only n
    result = [ str(dict[indexes[i]]) for i in range(n)]
    return result

def get_all_important_words(file_name='hadm_subject.csv',save_file_name='set.set'):

    ## check if file exist do nothing
    if os.path.exists(save_file_name):
        return

    df = pd.read_csv(file_name)
    ## get unique patients
    patients = df[patient_id_col].unique()
    vocab = set()
    con = Connection()
    for patient in patients:
        texts = con.get_comments_by_patient(patient)
        p_vocab = get_important_vocab(texts)
        vocab.update(p_vocab)
    ## save the vocabulary for the future use.
    with open(save_file_name,'wb') as f:
        pickle.dump(vocab,f)

"""
    This function remove stopwords and select only the words in the vocabulary.
"""


def pre_process_data(texts,set_file_name='set.set'):

    ## do a call to generate the vocabulary
    get_all_important_words()

    texts = remove_stop_wrods(texts)
    with open(set_file_name,'rb') as f:
        dictionary = pickle.load(f)

    temp_texts = []
    for text in texts:
        new_text = ""
        for word in text.split(" "):
            if word in dictionary:
                new_text = new_text+" "+word
        temp_texts.append(new_text)



    return temp_texts





