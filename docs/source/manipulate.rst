.. _manipulate:

Manipulate
===========

The scrapenhl2.manipulate module contains methods useful for scraping.

Useful examples
----------------

Add on-ice players to a file::

   from scrapenhl.manipulate import add_onice_players as onice
   add_players_to_file('/Users/muneebalam/Downloads/zone_entries.csv', 'WSH', time_format='elapsed')
   # Will output zone_entries_on-ice.csv in Downloads, with WSH players and opp players on-ice listed.

See documentation below for more information and additional arguments to add_players_to_file.


Methods
--------

.. automodule:: scrapenhl2.manipulate.manipulate
   :members:

.. automodule:: scrapenhl2.manipulate.add_onice_players
   :members:
