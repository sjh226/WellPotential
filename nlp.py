import pandas as pd
import numpy as np
import spacy
import sys
import re
import string
import csv
nlp = spacy.load('en')
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
import matplotlib.pyplot as plt
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk import pos_tag
from string import digits
import string
from matplotlib.ticker import FuncFormatter, MultipleLocator
from sklearn.model_selection import train_test_split, KFold
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import NMF as NMF_sklearn
from sklearn.linear_model import RidgeClassifier
from sklearn.linear_model import Perceptron
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from time import time
from sklearn import metrics
from sklearn.utils.extmath import density
from matplotlib.ticker import FuncFormatter, MaxNLocator, AutoMinorLocator
import seaborn as sns
from sql_pull import fetchdata


def root_group(df):
    # Group all root_cause entries by Event_API
    # Function mimics the event grouping function below
    df['API'] = df['API'].astype(str)
    df['Event_ID'] = df['Event_ID'].astype(str)
    f_events = lambda x: x['API'] +'_'+ x['Event_ID']
    df['Event_API'] = df.apply(f_events, axis =1)

    f_un = lambda x: x.unique()[0] if len(x.unique()) ==1 else '; '\
            .join([i if type(i) == str else str(i) for i in x.unique()])

    f = {'API':f_un, 'WellName':f_un,'EventCode':f_un,'Objective':f_un,
    'ProjectNumber':f_un, 'Asset':f_un, 'StartDate':['min'], 'EndDate':['max'],
    'Failure_Location':f_un, 'Root_Cause': f_un, 'RootCause_Integ': f_un, 'Event_ID': f_un}

    group = df.groupby(['Event_API']).agg(f)
    group = group.iloc[1:]
    swap_index = group.reset_index()
    fillna = swap_index.replace(np.nan, '', regex = True)
    fillna.columns = fillna.columns.get_level_values(0)

    return fillna

def event_group(df):
    # Group all the text entered for an event
    df['API'] = df['API'].astype(str)
    df['Event_ID'] = df['Event_ID'].astype(str)

    # Make a new identifier with the API and EventID, this prevents issues with
    # an event ID across two wells
    f_events = lambda x: x['API'] +'_'+ x['Event_ID']
    df['Event_API'] = df.apply(f_events, axis =1)

    # This function is used during the combination of events into one entry
    # If the value is unique use it, if there is more than one value in the column
    # then join the values with a semi-colon
    f_un = lambda x: x.unique()[0] if len(x.unique()) ==1 else '; '\
            .join([i if type(i) == str else str(i) for i in x.unique()])

    # Method for combination... apply the above function except for start and
    # end data, take the min and max respectivly
    f = {'Daily_ID': f_un, 'API':f_un, 'WellName':f_un,'Route':f_un,'FirstProductionDate':f_un,
    'WorkOver_Type':f_un, 'WorkOver_Code':f_un, 'Start_Date':['min'], 'End_Date':['max'],
    'Date_Report':f_un, 'Comment_Summary': f_un, 'Activity_Memo': f_un, 'Event_ID': f_un}

    # Group all entries by the event_api identifier and aggregate entries based
    # on the function above (f_un)
    group = df.groupby(['Event_API']).agg(f)

    # This makes a multi-layer df, need to reset back to one layer
    # ***************** What is meant by "back to one layer"?
    group = group.iloc[1:]
    swap_index = group.reset_index()
    swap_index['Activity_Memo'] = swap_index['Activity_Memo'].astype(str)
    swap_index['Comment_Summary'] = swap_index['Comment_Summary'].astype(str)
    fillna = swap_index.replace(np.nan, '', regex=True)

    # Make a column that holds all of the text
    merge = lambda x: ';'.join(x)
    swap_index['All_Comments'] = swap_index[['Activity_Memo', 'Comment_Summary']].apply(merge, axis=1)

    # If there is no entry then replace with an empty string so it can still
    # pass through the model
    fillna = swap_index.replace(np.nan, '', regex = True)

    return fillna

def search_comments(df, rep_key=False):
    '''
    Search the free text entries within the input dataframe for key words
    designated by the "search_for" parameter.
    '''
    df['All_Comments'] = df['All_Comments'].str.lower()

    # List of known integrity words to search for within the comments
    search_for = ['mud','scale', 'tag high','obstruction', 'tight',\
                  'lcm','LCM','lost circulation material',  'cement', \
                  'part tubing', 'parted tubing', 'parted coil tubing', \
                  'part coil tubing', 'part high', 'part shallow', 'gray', \
                  'grey sample', 'gray sample', 'grey matter', 'gray matter']

    # Locate all the entries that contain a key word within "search_for"
    hits = df[df['All_Comments'].str.contains('|'.join(search_for))]
    df.reset_index(inplace = True)
    for i in range(len(df)):
        if df.loc[i,'Event_API'] in list(hits.Event_API):
            # Find the sentence that had the key word
            sentences = []

            # Label with a 1 if in the hits data frame
            df.loc[i,'Class'] = 1

            # Make a list of the key words found in All_Comments
            in_list = [x for x in search_for if x in df.loc[i,'All_Comments']]
            df.loc[i,'KeyWords'] = ', '.join(in_list)

            # Create a list of lines split by sentence identifiers
            for line in re.split('; |, |\*|\n',df.loc[i,'All_Comments']):
                for word in search_for:
                    if word in line:
                        first_word_key = word.split(' ')[0]
                        line_s = re.sub(r'[^\w\s]','',line)
                        words = line_s.split(' ')
                        catch = [x for x in words if first_word_key in x]
                        before_key = words[words.index(catch[0])-1]
                        if before_key in ['no', 'not', 'none']:
                            # If negation occurs before the word then change
                            # class and add negation to the Negative column
                            df.loc[i, 'Class'] = 0
                            df.loc[i, 'Negative'] = before_key

                        # Add the line with the key word to the senetences list
                        sentences.append('Event_ID, ' + df.loc[i,'Event_ID'] +': '+ line)
                        break

            # Create a new column in the df with the sentences
            df.loc[i, 'Sentences'] = ';'.join(sentences)

        else:
            # If the event did not have a key word then label the class 0
            df.loc[i, 'Class'] = 0

    if rep_key ==True:
        # No longer in use but can be done to remove the key word from the
        # Comments section...
        # Was used to test if the ML algorithms could be trained to find the
        # entries labeled to have key words if the key words had been removed
        df['All_Comments_remove'] = df['All_Comments'].str.replace('|'.join(search_for), '')

    return hits, df

def root_cause_label(df):
    # Take the list of root_causes determined to indicate integrity problems
    # These are labeled with 1, in RootCause_Integ
    search_for = ['BARIUM SULFATE', 'CALCIUM CARBONATE', 'CO2', 'CORROSION', \
                  'FAILED CP-CASING LEAK', 'IRON CARBONATE', 'PARAFFIN']
    hits = df[df['Root_Cause'].str.contains('|'.join(search_for))]

    for i in range(len(df)):
        if df.loc[i,'Event_ID'] in list(hits.Event_ID):
            df.loc[i,'RootCause_Integ'] = 1
        else:
            df.loc[i,'RootCause_Integ'] = 0
    return df

def classifier_process(df, use_stemmer=False, remove=False, y_label='Class', \
                       text_label=None, df2=None):
    if remove == True:
        # Used for when removing the integrity key words from the comment
        X = df['All_Comments_remove']
    else:
        # Otherwise, predict using the entire comment
        X = df['All_Comments']

    if text_label:
        X = df[text_label]

    y = df[y_label]

    # Split data, currently on an 80/20 split
    kf = KFold(6)
    fold_df = pd.DataFrame(columns=['fold', 'score', 'recall', 'precision'])
    i = 0
    for train_index, test_index in kf.split(X):
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        i += 1

        tokenizer = RegexpTokenizer("[\w']+")
        stem = PorterStemmer().stem if use_stemmer else (lambda x: x)
        stop_set = set(stopwords.words('english'))

        # Closure over the tokenizer et al.
        def tokenize_and_stem(text, chi2_k = False):
            tokens = tokenizer.tokenize(text)
            stems = [stem(token) for token in tokens if token not in stop_set]
            return stems

        vectorizer_model = TfidfVectorizer(max_df=1.0, max_features=200000, \
                                           min_df=0, stop_words='english', \
                                           use_idf=True, tokenizer=tokenize_and_stem, \
                                           ngram_range=(1,3))

        X_train = vectorizer_model.fit_transform(X_train)
        X_test = vectorizer_model.transform(X_test)

        # Mapping from integer feature name to original token string
        feature_names = vectorizer_model.get_feature_names()

        result, model, score, pred, recall, precision = \
            dive(X_train, X_test, y_train, y_test, df2, vectorizer_model.transform(df2['All_Comments']))
        fold_df = fold_df.append(pd.DataFrame([[i, score, recall, precision]],
                                              columns=['fold', 'score', 'recall', 'precision']))

    print('Score: {}\nRecall: {}\nPrecision: {}\n--------------------'
                  .format(fold_df['score'].mean(),
                          fold_df['recall'].mean(),
                          fold_df['precision'].mean()))
    fold_df.to_csv('data/metics.csv')

    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, \
    #                                                     random_state=1, stratify=y)
    #
    # try:
    #     if df2.shape[0] > 1:
    #         return X_train, X_test, y_train, y_test, feature_names, \
    #                vectorizer_model.transform(df['All_Comments']), \
    #                vectorizer_model.transform(df2['All_Comments'])
    # except:
    #     pass
    #
    # return X_train, X_test, y_train, y_test, feature_names, \
    #        vectorizer_model.transform(df['All_Comments'])

def clean_string(comments):
    comments2 = "".join([ch for ch in str(comments) if ch not in string.punctuation])

    return re.sub(r'/[^\w\s]|_/g', ' ', comments2)

def benchmark(clf, X_train, X_test, y_train, y_test, print_report=False,
              print_cm=False, print_top10=False, feature_names=False,
              target_names=False, clf_name=None):
    print('_' * 80)
    print("Training: ")
    print(clf)
    t0 = time()
    clf.fit(X_train, y_train)
    train_time = time() - t0
    # print("train time: %0.3fs" % train_time)

    t0 = time()
    pred = clf.predict(X_test)
    test_time = time() - t0
    # print("test time:  %0.3fs" % test_time)

    score = metrics.accuracy_score(y_test, pred)
    print("accuracy:   %0.3f" % score)

    pre_score = metrics.precision_score(y_test, pred)
    recall = metrics.recall_score(y_test, pred)

    # if hasattr(clf, 'coef_'):
    #     print("dimensionality: %d" % clf.coef_.shape[1])
    #     print("density: %f" % density(clf.coef_))
    #
    #     if print_top10 and feature_names is not None:
    #         print("top 10 keywords per class:")
    #         for i, label in enumerate(target_names):
    #             top10 = np.argsort(clf.coef_[i])[-10:]
    #             print(label, feature_names)
    #             # print(trim("%s: %s" % (str(label), " ".join(feature_names[top10]))))
    #     print()

    if print_report:
        print("classification report:")
        print(metrics.classification_report(y_test, pred,))

    if print_cm:
        print("confusion matrix:")
        print(metrics.confusion_matrix(y_test, pred))

    print()
    if clf_name:
        clf_descr = clf_name
    else:
        clf_descr = str(clf).split('(')[0]
    # return clf_descr, score, pre_score, recall, train_time, test_time
    return clf_descr, '{}: {}'.format('Accuracy', round(score, 2)), '{}: {}'.format('Precision',round(pre_score,2)),\
     '{}: {}'.format('Recall',round(recall, 2))

def EDA_plot_ax(df, label, filename = 'histogram.png', color_labels = None):
    #problems with labeling
    fig, ax = plt.subplots(1, figsize=(14, 8))
    labels = list(df[label].value_counts().index)
    number_col = len(df[label].unique())
    if color_labels:
        colors = ['red' if i in color_labels else 'blue' for i in labels]
        print(colors)
        ax.bar(np.arange(number_col), df[label].value_counts(), color = colors, tick_label = labels)
        for item in ax.get_xticklabels():
            item.set(rotation = 90)
            item.set_fontsize(8)
        ax.set_xlabel(xlabel = 'Root Cause', labelpad = 5)
        ax.set_ylabel(ylabel = 'Count', labelpad = 5)
    else:
        fig = df[label].value_counts().plot(kind='bar')

    if compare:
        colors = ['red' if i in color_labels else 'blue' for i in labels]
        print(colors)
        ax.bar(np.arange(number_col), df[label].value_counts(), color = colors, tick_label = labels)
        for item in ax.get_xticklabels():
            item.set(rotation = 90)
            item.set_fontsize(8)
        ax.set_xlabel(xlabel = 'Root Cause', labelpad = 5)
        ax.set_ylabel(ylabel = 'Count', labelpad = 5)

    def format_fn(tick_val, tick_pos):
        if int(tick_val) in range(number_col):
            return labels[int(tick_val)]
        else:
            return ''
    minorLocator = AutoMinorLocator()
    # ax.tick_params(axis='x',which='major',bottom='off')
    # ax.tick_params(axis='x',which='minor',bottom='on')
    # ax.xaxis.set_minor_locator(minorLocator)
    # ax.xaxis.set_minor_formatter(FuncFormatter(format_fn))
    # plt.setp(ax.xaxis.get_minorticklabels(), rotation = 70)
    # ax.tick_params(axis = 'x', which = 'minor', labelrotation = 0.9 )
    plt.tight_layout()

    plt.savefig(filename)

def EDA_plots(df, label, filename = 'histogram.png', color_labels = None):
    plt.figure(figsize=(14, 8))
    labels = list(df[label].value_counts().index)
    number_col = len(df[label].unique())
    if color_labels:
        colors = ['red' if i in color_labels else 'blue' for i in labels]
        # print(colors)
        fig = df[label].value_counts().plot(kind='bar', color = colors )
        print(type(fig))
    else:
        fig = df[label].value_counts().plot(kind='bar')
    for item in fig.get_xticklabels():
        item.set(rotation = 90)
        item.set_fontsize(8)
    plt.tight_layout()
    plt.savefig(filename)

def EDA_seaborn(df, label, filename = 'histogram.png', color_labels = None):
    plt.figure()
    labels = list(df[label].value_counts().index)
    if color_labels:
        colors = ['red' if i in color_labels else 'blue' for i in labels]
        bar = sns.countplot(x=label, data = df, palette = colors)
    else:
        bar = sns.countplot(x=label, data = df)
    for item in bar.get_xticklabels():
        item.set(rotation = 90)
        item.set_fontsize(8)

    plt.tight_layout()
    plt.savefig(filename)
    plt.clf()

def dive(X_train, X_test, y_train, y_test, df, data):
    # Train and predict using a perceptron based on the comments provided
    # whether the work over contains an integrity issue
    clf = Perceptron(penalty='l2', n_iter=100, random_state=1, class_weight='balanced')
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)

    # Store the various statistics associated with the predictions
    score = metrics.accuracy_score(y_test, pred)
    recall = metrics.recall_score(y_test, pred)
    pre_score = metrics.precision_score(y_test, pred)

    # Create a copy of the dataframe to be returned with the model with the
    # associated predictions
    df_copy = df.copy()
    df_copy['Integ_Issue_Prediction'] = clf.predict(data)

    # Clean the free text within the original dataframe
    # Does not seem to be used currently, not sure if this is necessary
    free_text_columns = ['All_Comments', 'Activity_Memo', 'Comment_Summary']
    for column in free_text_columns:
        df[column]= df_copy[column].apply(clean_string)

    return df_copy, clf, score, pred, recall, pre_score

def run_benchmark(X_train, X_test, y_train, y_test, filename='benchmark.txt'):
    results = []

    # Loop through various models and run them each through the "benchmark"
    # function. Looks like a custom grid search
    for clf, name in (
            (Perceptron(n_iter=100, random_state=1, class_weight='balanced'), "Perceptron"),
            (Perceptron(n_iter=100, random_state=1, alpha=0.01), "Perceptron_alpha"),
            (RidgeClassifier(tol=1e-2, solver="lsqr", random_state=1, class_weight='balanced'), "Ridge Classifier_1"),
            (RidgeClassifier(tol=1e-2, solver="auto", random_state=1, class_weight='balanced'), "Ridge Classifier_2"),
            (Perceptron(n_iter=100, random_state=1), "Perceptron"),
            (PassiveAggressiveClassifier(n_iter=50, random_state=1), "Passive-Aggressive"),
            (PassiveAggressiveClassifier(n_iter=100, random_state=1, class_weight='balanced'), "Passive-Aggressive_2"),
            (KNeighborsClassifier(n_neighbors=10), "kNN"),
            (RandomForestClassifier(n_estimators=100, random_state=1, class_weight='balanced'), "Random forest_1"),
            (RandomForestClassifier(n_estimators=500, random_state=1, class_weight='balanced'), "Random forest_2"),
            (RandomForestClassifier(n_estimators=100, random_state=1, class_weight='balanced', max_depth = 100), "Random forest_3"),
            (RandomForestClassifier(n_estimators=500, random_state=1, class_weight='balanced', max_depth = 100), "Random forest_4")):
        results.append(benchmark(clf, X_train, X_test, y_train, y_test, print_report=True, \
                                 print_cm=False, print_top10=False, feature_names=None, clf_name=name))

    # Output the results of the grid search to a text file
    with open('data/{}'.format(filename), 'a') as the_file:
        for r in results:
            for line in r:
                the_file.write(line)
                the_file.write('\n')

def keyword_score(merge, label, pred):
    # Create a confusion matrix based on the predictions
    TP = merge[(merge[label] == 1) & (merge[pred] == 1)].shape[0]
    FP = merge[(merge[label] == 0) & (merge[pred] == 1)].shape[0]
    TN = merge[(merge[label] == 0) & (merge[pred] == 0)].shape[0]
    FN = merge[(merge[label] == 1) & (merge[pred] == 0)].shape[0]
    Base_Recall = TP/(TP+FN)
    Base_Acc = (TP +TN)/merge.shape[0]
    Base_Precision = TP/(TP+FP)

    # Base_Acc = 70%, Base_Prec = 52%, Base_Recall = 23.5%
    print('KeyWord_Recall: ', Base_Recall)
    print('KeyWord_Accuracy: ', Base_Acc)
    print('KeyWord_Precision: ', Base_Precision)

def dive2(X_train, X_test, y_train, y_test, df, data, print_report=False, print_cm=False,\
          print_top10=False, feature_names=False, target_names=False):
    # clf = KNeighborsClassifier(n_neighbors=10)
    # clf = RandomForestClassifier(n_estimators=100)
    clf = PassiveAggressiveClassifier(n_iter=50, random_state = 1)
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)
    score = metrics.accuracy_score(y_test, pred)
    recall = metrics.recall_score(y_test, pred)
    pre_score = metrics.precision_score(y_test, pred)
    print(df.columns)
    # df['Integ_Issue_Probability'] = clf.predict_proba(data)[:,1]
    df['Integ_Issue_Prediction'] = clf.predict(data)

    free_text_columns = ['All_Comments', 'Activity_Memo', 'Comment_Summary']
    # df_copy = df.copy()
    # for column in free_text_columns:
    #     df[column]= df_copy[column].apply(clean_string)

    return df, clf, score, pred, recall, pre_score


if __name__ == '__main__':
    '''
    When necessary packages have been downloaded, running "python nlp.py" within
    a Python console will follow the following steps:

        -pull work over data from SQL
        -group events based on event and API
        -compare these events with a list of know integrity words
        -import root causes from csv
        -create similar grouping for the root causes and merge with work over
            data
        -clean data based on RE/NLP analysis and return necessary variables to
            train and run model
        -run perceptron, returning predictions and scores
    '''
    # Run SQL query to fetch work over reports
    df = fetchdata()
    df.to_csv('data/North_WO_Text.csv')
    # df = pd.read_csv('data/North_WO_Text.csv')

    grouped_event = event_group(df)
    grouped_event.columns = grouped_event.columns.get_level_values(0)

    # If an event has more than one well listed exclude that from the dataset
    # (probably problem with duplicates)
    grouped_event = grouped_event[~grouped_event['API'].str.contains(';')]

    # Find comments which correspond to know integrity words
    hits, classed = search_comments(grouped_event, rep_key = True)

    # Bring in root cause dataset
    root = pd.read_csv('data/North_RootCause.csv')
    root2 = root_cause_label(root)
    root3 = root_group(root2)

    # Merge the root_cause with the text data
    merge_root = pd.merge(classed, root3, on = ['Event_API'])

    # Split into train and test based on RE/NLP analysis
    X_train, X_test, y_train, y_test, feature_name, all_data, allall_data = \
        classifier_process(merge_root, remove=False, y_label='RootCause_Integ', \
                           text_label='Activity_Memo', df2=classed)

    # #Run and return predictions for all_data (data that has been previously labeled both train and test)
    # result, model, score, pred, recall, precision = dive(X_train, X_test, y_train, y_test,merge_root,all_data,  print_report = False, print_cm = False,\
    #     print_top10 = False, feature_names = False, target_names = False)

    # Run and return predicitons for allall_data (all WO collected, even those without root cause predictions)
    result, model, score, pred, recall, precision = \
        dive(X_train, X_test, y_train, y_test, classed, allall_data)

    result.to_csv('data/North_WO_predictions.csv')

    # #look at the distribution of Root_Cause types (total)
    # result_counts_base = pd.DataFrame(result['Root_Cause'].value_counts())
    # result_counts_base.to_csv('Base_result_counts.csv')

    # #Look at the distribution of Root_Cause entries that were predicted to have integrity issues
    # pred = result[result['Integ_Issue_Prediction']==1]
    # result_counts = pd.DataFrame(pred['Root_Cause'].value_counts())
    # result_counts.to_csv('Perceptron_result_counts.csv')
