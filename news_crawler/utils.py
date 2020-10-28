# -*- coding: utf-8 -*-
# Utils for news_crawler project

import os
import pandas as pd

def save_as_html(response, domain):
    """
    Saves the scraped webpage of a news article in HTML format.

    Parameters
    ----------
    response: object
        The response returned by the spider
    domain: str
        The media outlet domain
    """
    filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'data', 'scraped', 'corpus_html', domain)
    if not os.path.isdir(filepath):
        os.makedirs(filepath)
    filename = response.url.split('/')[-1] + '.html'
    
    print('Saving page as html.')
    with open(os.path.join(filepath, filename), 'wb') as f:
        f.write(response.body)
    print('Saved.')
