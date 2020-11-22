# -*- coding: utf-8 -*-
# Utils for news_crawler project


def remove_empty_paragraphs(paragraphs):
    """ Removes empty paragraphs from a list of paragraphs """
    return [para for para in paragraphs if para != ' ' and para != '']
