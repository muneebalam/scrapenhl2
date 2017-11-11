#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from scrapenhl2.scrape import autoupdate


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--season", type=int, default=None)
    arguments = parser.parse_args()

    if 2017 < arguments.season < 2010:
        print("Invalid season")

    autoupdate.autoupdate(season=arguments.season)

