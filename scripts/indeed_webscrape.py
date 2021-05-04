import pandas as pd
import requests
import pickle
from bs4 import BeautifulSoup as bsopa  # parse web-page
import datetime
from collections import defaultdict
import re
import numpy as np


def job_descriptions():
    datum_array_dict = []
    for j in range(0, 30, 10):
        position, location = 'data scientist', 'california'
        #y=requests.get('https://www.indeed.com/jobs?q=data+scientist&l=california&sort=date='+str(i))
        #y=requests.get(f'https://www.indeed.com/jobs?q={position}&l={location}&sort=date='+str(j))
        
        y = requests.get('https://www.indeed.com/jobs?q={}&l={}&sort=date='.format(position, location) + str(j))
        #print(y.content)
        sou = bsopa(y.text, 'lxml')
        #print(sou)
        
      # for ii in sou.find_all('div', {'class': 'row'}):
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

            print("GET LOCATION")
            print(location)

        k = ii.find('h2', {'class': "title"})
        print('title content')
        print(k)
        p = k.find(href=True)
        v = p['href']

        # links to iterate for qualification text
        f_ = str(v).replace('&amp;', '&')

        datum = {'job_title': job_title,
                     'company_name': company_name,
                     'location': location,
                     'summary': summary.text.strip(),
                     'post_Date': post_date.text,
                     'Qualification_link': f_}

        datum_array_dict.append(datum)

    return datum_array_dict


def get_job_qualifications(datum_array_dict):
    # get all qualification page text: key=index, value=string of text for qualification
    job_qualifications = []
    for i in range(len(datum_array_dict)):
        op = requests.get(f"https://www.indeed.com{datum_array_dict[i]['Qualification_link']}")
        sou_ = bsopa(op.text, 'html.parser')
        for ii in sou_.find('div', {'class': 'jobsearch-jobDescriptionText'}):
            try:
                job_qualifications.append([i, ''.join(ii.text.strip())])
            except AttributeError:
                job_qualifications.append([i, ''])
    return job_qualifications


def create_dictionary_with_values(job_qualifications):
    # create dictionary with values as lists
    dct_lst = defaultdict(list)
    for i in job_qualifications:
        dct_lst[i[0]].append(i[1])

    list_with_index = []
    for i in dct_lst.values():  # string join: lists of lists of strings
        list_with_index.append(''.join(i))

    # one entry of our qualification text:
    # u[2]
    return list_with_index


def jobs_in_pandas(datum_array_dict, list_with_index):
    """
     Concatenate pandas objects along a particular axis with optional set logic
    along the other axes.

    Can also add a layer of hierarchical indexing on the concatenation axis,
    which may be useful if the labels are the same (or overlapping) on
    the passed axis number.

    Place the DataFrames side by side axis=1
    """
    job_positions = pd.concat([pd.DataFrame(datum_array_dict),
                               pd.DataFrame(list_with_index, columns=['Qual_Text'])], axis=1)
    # returns the first n rows for the object based on position.
    #jobs_.head()
    return job_positions


def post_by_date(job_positions):
    positions_sorted_by_date = []
    for i in job_positions['post_Date']:

        if re.findall(r'[0-9]', i):
            # if the string has digits convert each entry to single string: ['3','0']->'30'
            b = ''.join(re.findall(r'[0-9]', i))

            # convert string int to int and subtract from today's date and format
            g = (datetime.datetime.today() - datetime.timedelta(int(b))).strftime('%m-%d-%Y')

            positions_sorted_by_date.append(g)

        else:
            # this will contain strings like: 'just posted' or 'today' etc before convert
            positions_sorted_by_date.append(datetime.datetime.today().strftime('%m-%d-%Y'))
    # v[:5]
    return positions_sorted_by_date


def fix_posting_date(job_positions, positions_sorted_by_date):
    # fixed posting date to date format instead of string: last column
    job_positions['posting_date_fixed'] = v
    job_positions.head()
    return job_positions


def create_list_of_skills(job_positions):
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
    for i in range(len(job_positions.Qual_Text)):
        a = buzz_words_list
        dd = [x for x in a if x in job_positions.Qual_Text[i].lower()]
        yo.append(dd)

    job_positions['skill_matches'] = yo
    # job_positions.head(7)
    return job_positions


def create_file_with_data(job_positions):
    np.savetxt('np.txt', job_positions.values, fmt='%s')
    filename = 'indeed_scrape.txt'
    file = open(filename, 'wb')
    pickle.dump(job_positions, file)

    file_ = open(filename, 'rb')
    new_file_ = pickle.load(file_)
    new_file_.head(10)


gg = job_descriptions()
#print(gg)

hoop = get_job_qualifications(gg)
u = create_dictionary_with_values(hoop)
print(u)
jobs_ = jobs_in_pandas(gg, u)
print("##################")
print(jobs_)

v = post_by_date(jobs_)
print(v)
jobs_ = fix_posting_date(jobs_, v)
print(jobs_)
jobs_ = create_list_of_skills(jobs_)
print(jobs_)
# create_file_with_data(jobs_)
