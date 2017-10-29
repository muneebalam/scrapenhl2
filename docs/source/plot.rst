.. _plot:

Plot
====

The scrapenhl2.plot module contains methods useful for plotting.

Useful examples
----------------

First, import::

   from scrapenhl2.plot import *

Get the H2H for an in-progress game::

   live_h2h('WSH', 'EDM')

.. image:: _static/example_h2h.png

Get the Corsi timeline as well, but don't update data this time::

   live_timeline('WSH', 'EDM', update=False)

.. image:: _static/example_timeline.png

Save the timeline of a memorable game to file::

   game_timeline(2016, 30136, save_file='/Users/muneebalam/Desktop/WSH_TOR_G6.png')

More methods being added regularly.

Methods
--------

Game H2H
~~~~~~~~~~~
.. automodule:: scrapenhl2.plot.game_h2h
   :members:

Corsi timeline
~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.plot.game_timeline
   :members:

Player rolling CF
~~~~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.plot.rolling_cf
   :members:

Helper methods
~~~~~~~~~~~~~~~
.. automodule:: scrapenhl2.plot.visualization_helper
   :members:
