"""
At import, this module creates folders for data storage if need be.

It also creates a team ID mapping and schedule files from 2005 through the current season (if the files do not exist).
"""

import scrapenhl2.scrape.scrape_setup
import scrapenhl2.scrape.scrape_game
import scrapenhl2.scrape.scrape_season