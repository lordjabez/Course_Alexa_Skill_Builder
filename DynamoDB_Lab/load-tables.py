#!/usr/bin/env python3


import csv

import boto3


dynamodb = boto3.client('dynamodb')


def make_countries_item(row):
    return {
        'CountryId': {'S': row[0]},
        'Name': {'S': row[1]}
    }


def make_fun_facts_item(row):
    return {
        'RecordNumber': {'S': row[0]},
        'CountryId': {'S': row[1]},
        'Text': {'S': row[2]},
    }


def make_stories_item(row):
    return {
        'CountryId': {'S': row[0]},
        'QuestionNumber': {'N': row[1]},
        'QuestionText': {'S': row[2]},
    }


def make_story_details_item(row):
    return {
        'CountryId': {'S': row[0]},
        'QuestionNumber': {'N': row[1]},
        'NoEnergyImpact': {'N': row[2]},
        'NoResponseText': {'S': row[3]},
        'NoWealthImpact': {'N': row[4]},
        'Tip': {'S': row[5]},
        'YesEnergyImpact': {'N': row[6]},
        'YesResponseText': {'S': row[7]},
        'YesWealthImpact': {'N': row[8]},
    }


make_item = {
    'AdvgCountries': make_countries_item,
    'AdvgFunFacts': make_fun_facts_item,
    'AdvgStories': make_stories_item,
    'AdvgStoryDetails': make_story_details_item,
}


def load_table_items(table):
    with open(f'{table}.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
        next(csv_reader)
        return [{'PutRequest': {'Item': make_item[table](r)}} for r in csv_reader]


tables = ('AdvgCountries', 'AdvgFunFacts', 'AdvgStories', 'AdvgStoryDetails')
request_items = {t: load_table_items(t) for t in tables}

response = dynamodb.batch_write_item(RequestItems=request_items)
print(response)
