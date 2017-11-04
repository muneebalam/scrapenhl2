"""
This module creates static and animated usage charts.
"""

from scrapenhl2.plot import visualization_helper as vhelper

def static_usage_chart(**kwargs):
    """

    :param kwargs: Defaults to take last month of games for all teams.

    :return: nothing, or figure
    """
    if 'startdate' not in kwargs and 'enddate' not in kwargs and \
                    'startseason' not in kwargs and 'endseason' not in kwargs:
        kwargs['last_n_days'] = 30

    qocqot = vhelper.get_and_filter_5v5_log(**kwargs)
    return vhelper.savefilehelper(**kwargs)

if __name__ == '__main__':
    static_usage_chart(team='WSH')