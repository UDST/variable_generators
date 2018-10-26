from __future__ import print_function

import numpy as np
import pandas as pd

import orca

from urbansim.utils import misc
from urbansim.models import util

try:
    import pandana
except ImportError:
    pass


def make_agg_var(agent, geog, geog_id, var_to_aggregate, agg_function):
    """
    Generator function for aggregation variables. Registers with orca.
    """
    var_name = agg_function + '_' + var_to_aggregate

    @orca.column(geog, var_name, cache=True, cache_scope='iteration')
    def func():
        agents = orca.get_table(agent)
        print('Calculating {} of {} for {}'
              .format(var_name, agent, geog))

        groupby = agents[var_to_aggregate].groupby(agents[geog_id])
        if agg_function == 'mean':
            values = groupby.mean().fillna(0)
        if agg_function == 'median':
            values = groupby.median().fillna(0)
        if agg_function == 'std':
            values = groupby.std().fillna(0)
        if agg_function == 'sum':
            values = groupby.sum().fillna(0)

        locations_index = orca.get_table(geog).index
        series = pd.Series(data=values, index=locations_index)

        # Fillna.
        # For certain functions, must add other options,
        # like puma value or neighboring value
        if agg_function == 'sum':
            series = series.fillna(0)
        else:
            series = series.fillna(method='ffill')
            series = series.fillna(method='bfill')

        return series

    return func


def make_disagg_var(from_geog_name, to_geog_name, var_to_disaggregate,
                    from_geog_id_name, name_based_on_geography=True):
    """
    Generator function for disaggregating variables. Registers with orca.
    """
    if name_based_on_geography:
        var_name = from_geog_name + '_' + var_to_disaggregate
    else:
        var_name = var_to_disaggregate

    @orca.column(to_geog_name, var_name, cache=True, cache_scope='iteration')
    def func():
        print('Disaggregating {} to {} from {}'
              .format(var_to_disaggregate, to_geog_name, from_geog_name))

        from_geog = orca.get_table(from_geog_name)
        to_geog = orca.get_table(to_geog_name)
        return misc.reindex(from_geog[var_to_disaggregate],
                            to_geog[from_geog_id_name]).fillna(0)

    return func


def make_size_var(agent, geog, geog_id, cache_scope='step'):
    """
    Generator function for size variables. Registers with orca.
    """
    var_name = 'total_' + agent

    @orca.column(geog, var_name, cache=True, cache_scope=cache_scope)
    def func():
        agents = orca.get_table(agent)
        print('Calculating number of {} for {}'.format(agent, geog))

        size = agents[geog_id].value_counts()

        locations_index = orca.get_table(geog).index
        series = pd.Series(data=size, index=locations_index)
        series = series.fillna(0)

        return series

    return func


def make_proportion_var(agent, geog, geog_id, target_variable, target_value):
    """
    Generator function for proportion variables. Registers with orca.
    """
    var_name = 'prop_%s_%s' % (target_variable, int(target_value))

    @orca.column(geog, var_name, cache=True, cache_scope='iteration')
    def func():
        agents = orca.get_table(agent).to_frame(
            columns=[target_variable, geog_id])
        locations = orca.get_table(geog)
        print('Calculating proportion {} {} for {}'
              .format(target_variable, target_value, geog))

        agent_subset = agents[agents[target_variable] == target_value]
        series = (agent_subset.groupby(geog_id).size()
                  * 1.0
                  / locations['total_' + agent])
        series = series.fillna(0)
        return series

    return func


def make_dummy_variable(agent, geog_var, geog_id):
    """
    Generator function for spatial dummy. Registers with orca.
    """
    # cache_scope
    try:
        var_name = geog_var + '_is_' + str(geog_id)
    except Exception:
        var_name = geog_var + '_is_' + str(int(geog_id))

    @orca.column(agent, var_name, cache=True, cache_scope='iteration')
    def func():
        agents = orca.get_table(agent)
        return (agents[geog_var] == geog_id).astype('int32')

    return func


def make_ratio_var(agent1, agent2, geog):
    """
    Generator function for ratio variables. Registers with orca.
    """
    var_name = 'ratio_%s_to_%s' % (agent1, agent2)

    @orca.column(geog, var_name, cache=True, cache_scope='iteration')
    def func():
        locations = orca.get_table(geog)
        print('Calculating ratio of {} to {} for {}'
              .format(agent1, agent2, geog))

        series = (locations['total_' + agent1]
                  * 1.0
                  / (locations['total_' + agent2] + 1.0))
        series = series.fillna(0)
        return series

    return func


def make_density_var(agent, geog):
    """
    Generator function for density variables. Registers with orca.
    """
    var_name = 'density_%s' % (agent)

    @orca.column(geog, var_name, cache=True, cache_scope='iteration')
    def func():
        locations = orca.get_table(geog)

        print('Calculating density of {} for {}'.format(agent, geog))

        series = locations['total_' + agent] * 1.0 / (
            locations['sum_acres'] + 1.0)

        series = series.fillna(0)
        return series

    return func


def make_access_var(name, agent, target_variable=False, target_value=False,
                    radius=1000, agg_function='sum', decay='flat', log=True,
                    filters=False):
    """
    Generator function for accessibility variables. Registers with orca.
    """

    @orca.column('nodes', name, cache=True, cache_scope='iteration')
    def func(net):
        print('Calculating {}'.format(name))

        nodes = pd.DataFrame(index=net.node_ids)
        flds = [target_variable] if target_variable else []

        if target_value:
            flds += util.columns_in_filters(
                ["{} == {}".format(target_variable, target_value)])

        if filters:
            flds += util.columns_in_filters(filters)
        flds.append('node_id')

        df = orca.get_table(agent).to_frame(flds)

        if target_value:
            df = util.apply_filter_query(df, [
                "{} == {}".format(target_variable, target_value)])
        if filters:
            df = util.apply_filter_query(df, filters)

        net.set(df['node_id'],
                variable=df[target_variable] if target_variable else None)
        nodes[name] = net.aggregate(radius, type=agg_function, decay=decay)

        if log:
            nodes[name] = nodes[name].apply(eval('np.log1p'))
        return nodes[name]

    return func
