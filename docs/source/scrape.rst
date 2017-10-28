.. _scrape:

Scrape
=======

The scrapenhl2.scrape module contains methods useful for scraping.

Useful examples
----------------

Updating data::

   from scrapenhl2.scrape import autoupdate
   autoupdate.autoupdate()

Get the season schedule::

   from scrapenhl2.scrape import schedules
   schedules.get_season_schedule(2017)

Convert between player ID and player name::

   from scrapenhl2.scrape import players
   pname = 'Alex Ovechkin'
   players.player_as_id(pname)

   pid = 8471214
   players.player_as_str(pid)

aa

Methods
--------

The functions in these modules are organized pretty logically under the module names.

.. automodule:: scrapenhl2.scrape.autoupdate
.. automodule:: scrapenhl2.scrape.events
.. automodule:: scrapenhl2.scrape.games
.. automodule:: scrapenhl2.scrape.general_helpers
.. automodule:: scrapenhl2.scrape.manipulate_schedules
.. automodule:: scrapenhl2.scrape.organization
.. automodule:: scrapenhl2.scrape.parse_pbp
.. automodule:: scrapenhl2.scrape.parse_toi
.. automodule:: scrapenhl2.scrape.players
.. automodule:: scrapenhl2.scrape.schedules
.. automodule:: scrapenhl2.scrape.scrape_pbp
.. automodule:: scrapenhl2.scrape.scrape_toi
.. automodule:: scrapenhl2.scrape.team_info
.. automodule:: scrapenhl2.scrape.teams

