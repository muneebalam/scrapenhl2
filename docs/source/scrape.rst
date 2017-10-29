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

There's much more, and feel free to submit pull requests with whatever you find useful.

Methods
--------

The functions in these modules are organized pretty logically under the module names.

Autoupdate
~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.autoupdate
   :members:

Events
~~~~~~~
.. automodule:: scrapenhl2.scrape.events
   :members:

Games
~~~~~~
.. automodule:: scrapenhl2.scrape.games
   :members:

General helpers
~~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.general_helpers
   :members:

Organization
~~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.organization
   :members:

Players
~~~~~~~~
.. automodule:: scrapenhl2.scrape.players
   :members:

Schedules
~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.schedules
   :members:

Manipulate schedules
~~~~~~~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.manipulate_schedules
   :members:

Scrape play by play
~~~~~~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.scrape_pbp
   :members:

Parse play by play
~~~~~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.parse_pbp
   :members:

Scrape TOI
~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.scrape_toi
   :members:

Parse TOI
~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.parse_toi
   :members:

Team information
~~~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.scrape.team_info
   :members:

Teams
~~~~~
.. automodule:: scrapenhl2.scrape.teams
   :members:

