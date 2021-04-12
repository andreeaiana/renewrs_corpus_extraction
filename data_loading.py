# -*- coding: utf-8 -*-
""" Data loading and preprocessing utilities """

import os
import pickle
import argparse
import pandas as pd
import pycld2 as cld2

class Loader:

    def __init__(self, topic: str) -> None:
        self.raw_files_dir = os.path.join('data', topic)
        self.dataset_filepath = os.path.join('data', 'final_dataset', topic + '_dataset.p')

    def _load_corpus(self) -> None:
        """ Loads the raw corpus for the desired topic into memory. """
        print('Loading the raw corpus.')

        dataframes = list()
        outlets = os.listdir(self.raw_files_dir)
        for idx, outlet in enumerate(outlets):
            outlet_files_dir = os.path.join(self.raw_files_dir, outlet, 'json')
            if os.path.isdir(outlet_files_dir):
                outlet_files = os.listdir(outlet_files_dir)
                for json_file in outlet_files:
                    filepath = os.path.join(outlet_files_dir, json_file)
                    if not os.path.isfile(filepath):
                        raise FileNotFoundError('Could not find file for path "{}"!'.format(filepath))
                    df = pd.read_json(filepath, orient='index')
                    dataframes.append(df.T)
            else:
                print(f'\tOutlet {outlet} has no articles.')
            print(f'\tRead files for {idx} of {len(outlets)} outlets.')

        # Concatenate all datframes into one
        self.dataset = pd.concat(dataframes, ignore_index=True)
        self.dataset.reset_index(inplace=True, drop=True)
        print(f'Raw corpus with {len(self.dataset)} news articles loaded into memory.\n')

    def _preprocess_corpus(self) -> None:
        """ Preprocesses the raw corpus of news articles to create the cleaned dataset. """
        print('Preprocessing the raw corpus.')
        
        # Split the body column into different columns for title, description and content
        self.dataset = pd.concat([self.dataset.drop(['content'], axis =1), self.dataset['content'].apply(pd.Series)], axis=1)

        # Preprocess content body by merging headlines with corresponding paragraphs
        self.dataset['body'] = self.dataset['body'].apply(lambda x: sum(x.values(), []))
        self.dataset['body'] = self.dataset['body'].apply(lambda x: ' '.join([para for para in x if para != '' and para != ' ']))

        # Remove empty spaces from title and description
        self.dataset['title'] = self.dataset['title'].str.strip()
        self.dataset['description'] = self.dataset['description'].str.strip()

        # Replace None values in columns
        self.dataset['description'].fillna('', inplace=True)
        self.dataset['author_person'] = self.dataset['author_person'].apply(lambda l: [x for x in l if x is not None])

        # Fix values in recommendations column
        self.dataset['recommendations']  =self.dataset['recommendations'].apply(lambda val: list() if type(val)==float else val)

        # Drop duplicates based on article's content
        print('\tRemoving duplicates.')
        self.dataset.drop_duplicates(subset=['title', 'description', 'body'], inplace=True)
        print(f'\tDataset contains {len(self.dataset)} news articles after duplicates removal.')

        # Keep only the latest version for articles with multiple updates
        print('\tRemoving older versions of articles with multiple updates.')
        self._drop_outdated_articles()

        # Keep only articles in German
        self.dataset = self.dataset[self.dataset['body'].map(lambda x: self._detect_language(x))=='de']

        print(f'The preprocessed dataset has {len(self.dataset)} news articles.\n')
        

    def _drop_outdated_articles(self) -> None:
        """ Drops outdated articles (i.e. articles with more recent updates). """
        updated_articles = self.dataset.loc[self.dataset.duplicated(subset=['title'], keep=False)==True]
        grouped_updated_articles = updated_articles.groupby('title').apply(lambda x: x.index.to_list())
        updated_articles_idx = grouped_updated_articles.values.tolist()

        for idx_pair in updated_articles_idx:
            sample = updated_articles.loc[idx_pair]

            if sample.loc[idx_pair[0]]['creation_date'] != sample.loc[idx_pair[1]]['creation_date']:
                # If publishing dates differ, drop least recent article
                least_recent_date = sample['creation_date'].min()
                older_article_idx = sample.loc[sample['creation_date']==least_recent_date].index
                self.dataset.drop(older_article_idx, inplace=True)
            elif sample.loc[idx_pair[0]]['last_modified'] != sample.loc[idx_pair[1]]['last_modified']:
                # If the dates when the articles where last modified differ, drop least recently modified article
                least_recent_date = sample['last_modified'].min()
                older_article_idx = sample.loc[sample['last_modified']==least_recent_date].index
                self.dataset.drop(older_article_idx, inplace=True)
            else:
                # Drop article version with shotest body (i.e. assumed least recent)
                shortest_article = sample.loc[sample['body'].str.len().idxmin()].name
                self.dataset.drop(shortest_article, inplace=True)

    def _detect_language(self, text: str) -> str:
        """ Detects the languag of a given string. """
        _, _, details = cld2.detect(text)
        return details[0][1]

    def _cache_data(self) -> None:
        """ Caches the data to disk as a pickle file. """
        print('Caching dataset to disk.')
        with open(self.dataset_filepath, 'wb') as f:
            pickle.dump(self.dataset, f)
        print('Dataset cached.\n')

    def _load_cache(self) -> bool:
        """ Loads the cached dataset if it exists. """
        if os.path.isfile(self.dataset_filepath):
            print('Loading the cached dataset.')
            with open(self.dataset_filepath, 'rb') as f:
                self.dataset = pickle.load(f)
            print(f'Loaded dataset with {len(self.dataset)} articles.\n')
            return True
        return False

    def load_dataset(self) -> pd.DataFrame:
        """ Loads the prepocessed dataset if cached to disk. If not, loads the raw corpus and preprocesses it before returning the resulting dataset. """
        if not self._load_cache():
            print('Data not cached.\n')
            # Load the raw corpus into memory 
            self._load_corpus()
            
            # Preprocess the raw dataset
            self._preprocess_corpus()

            # Cache the processed dataset to disk
            self._cache_data()

        return self.dataset

    if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Arguments for data loading and preprocessing.')
        parser.add_argument('topic', 
                choices=['refugees_migration', 'legalization_soft_drugs'],
                help='the topic for which the dataset should be loaded'
                )
        args = parser.parse_args()

        from data_loading import Loader 
        loader = Loader(args.topic)
        loader.load_dataset()
