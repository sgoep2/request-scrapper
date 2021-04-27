import pandas as pd
import requests
import pickle
from bs4 import BeautifulSoup as bsopa  # parse web-page
import datetime
from collections import defaultdict
import re
import numpy as np


def job_descriptions():
    gg = []
    for j in range(0, 10, 10):
        position, location = 'data scientist', 'california'
        y = requests.get('https://www.indeed.com/jobs?q={}&l={}&sort=date='.format(position, location) + str(j))

        # y=requests.get('https://www.indeed.com/jobs?q=data+scientist&l=california&sort=date='+str(i))
        sou = bsopa(y.text, 'lxml')

        #     for ii in sou.find_all('div', {'class': 'row'}):
        for ii in sou.find_all('div', {"class": "jobsearch-SerpJobCard"}):

            # GET JOB TITLE
            job_title = ii.find('a', {'data-tn-element': 'jobTitle'})['title']
            # GET COMPANY NAME
            company_name = ii.find('span', {'class': 'company'}).text.strip()
            # GET LOCATION
            location = ii.find('span', {"class": "location"})
            # GET POST DATE
            post_date = ii.find('span', attrs={'class': 'date'})
            # GET SUMMARY
            summary = ii.find('div', attrs={'class': 'summary'})

            if location:
                location = location.text.strip()
            else:
                location = ii.find('div', {"class": "location"})
                location = location.text.strip()

            k = ii.find('h2', {'class': "title"})
            p = k.find(href=True)
            v = p['href']
            f_ = str(v).replace('&amp;', '&')  # links to iterate for qualification text

            datum = {'job_title': job_title,
                     'company_name': company_name,
                     'location': location,
                     'summary': summary.text.strip(),
                     'post_Date': post_date.text,
                     'Qualification_link': f_}

            gg.append(datum)
    return gg


def get_job_qualifications(gg):
    # get all qualification page text: key=index, value=string of text for qualification
    hoop = []
    for i in range(len(gg)):
        op = requests.get('https://www.indeed.com' + gg[i]['Qualification_link'])
        sou_ = bsopa(op.text, 'html.parser')
        for ii in sou_.find('div', {'class': 'jobsearch-jobDescriptionText'}):
            try:
                hoop.append([i, ''.join(ii.text.strip())])
            except AttributeError:
                hoop.append([i, ''])
    return hoop


def create_dictionary_with_values(hoop):
    # create dictionary with values as lists
    dct_lst = defaultdict(list)
    for i in hoop:
        dct_lst[i[0]].append(i[1])

    u = []
    for i in dct_lst.values():  # string join: lists of lists of strings
        u.append(''.join(i))

    return u
    # one entry of our qualification text:
    # u[2]


def jobs_in_pandas(gg, u):
    jobs_ = pd.concat([pd.DataFrame(gg), pd.DataFrame(u, columns=['Qual_Text'])], axis=1)
    jobs_.head()
    return jobs_


def post_by_date(jobs_):
    v = []
    for i in jobs_['post_Date']:

        if re.findall(r'[0-9]', i):
            # if the string has digits convert each entry to single string: ['3','0']->'30'
            b = ''.join(re.findall(r'[0-9]', i))

            # convert string int to int and subtract from today's date and format
            g = (datetime.datetime.today() - datetime.timedelta(int(b))).strftime('%m-%d-%Y')

            v.append(g)

        else:  # this will contain strings like: 'just posted' or 'today' etc before convert
            v.append(datetime.datetime.today().strftime('%m-%d-%Y'))
    # v[:5]
    return v


def fix_posting_date(jobs_, v):
    # fixed posting date to date format instead of string: last column
    jobs_['posting_date_fixed'] = v
    jobs_.head()
    return jobs_


def create_list_of_skills(jobs_):
    # Create a list of skills that you may have or general list
    buzz_words = ['Python', 'SQL', 'AWS', 'Machine learning', 'Deep learning', 'Text mining',
                  'NLP', 'SAS', 'Tableau', 'Sagemaker', 'Tensorflow', 'Spark', 'numpy', 'MongDB', 'PSQL',
                  "Postgres", 'Pandas', 'RESTFUL', 'NLP', 'Statistics', 'Algorithms', 'Visualization',
                  'GCP', 'Google Cloud', 'Naive Bayes', 'Random Forest', 'Bachelors degree', 'Masters degree'
                                                                                             'Java', 'Pyspark',
                  'Postgres',
                  'MySQL', 'Github', 'Docker', 'Machine Learning', 'C+',
                  'C++', 'Pytorch', 'Jupyter Notebook', 'R Studio', 'R-Studio', 'Forecasting', 'Hive',
                  'PhD', 'GCP', 'Numpy', 'NoSQL', 'Neo4j', 'Neural Network', 'Clustering', 'Linear Algebra',
                  'Google Colab', 'Data Mining', 'Regression', 'Time Series', 'ETL', 'Data Wrangling',
                  'Web Scraping', 'Feature Extraction', 'Featuring Engineering', 'Scipy', 'ML', 'DL']
    buzz_words_list = [x.lower() for x in buzz_words]  # convert list to lowercase to parse

    yo = []
    for i in range(len(jobs_.Qual_Text)):
        a = buzz_words_list
        dd = [x for x in a if x in jobs_.Qual_Text[i].lower()]
        yo.append(dd)
    jobs_['skill_matches'] = yo
    jobs_.head(7)
    return jobs_


def create_file_with_data(jobs_):
    np.savetxt('np.txt', jobs_.values, fmt='%s')
    filename = 'Indeed_scrape_Oct2020.txt'
    file = open(filename, 'wb')
    pickle.dump(jobs_, file)

    file_ = open(filename, 'rb')
    new_file_ = pickle.load(file_)
    new_file_.head(10)


gg = job_descriptions()
hoop = get_job_qualifications(gg)
u = create_dictionary_with_values(hoop)
jobs_ = jobs_in_pandas(gg, u)
v = post_by_date(jobs_)
jobs_ = fix_posting_date(jobs_, v)
jobs_ = create_list_of_skills(jobs_)
create_file_with_data(jobs_)
