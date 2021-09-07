from collections import OrderedDict, namedtuple
import copy
import datetime as dt
import logging
import matplotlib
import matplotlib.pyplot as plt
import pytz
import re
import textwrap

import numpy as np
import pandas as pd

from dsgrid.dataformat.enumeration import EndUseEnumerationBase

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# User input
# ------------------------------------------------------------------------------

class OptionPresenter(object):

    def __init__(self, alist):
        self._list = alist
        self._n = len(self._list)
        self._fmt_str = '{: ' + str(len(str(self._n))+1) + '}: {}'

    def present_options(self, name_func = None):
        for i, val in enumerate(self._list, 1):
            print(self._fmt_str.format(i, val if name_func is None else name_func(val)))

    def get_option(self, choice):
        return self._list[int(choice)-1]

    def get_options(self, choices):
        return [self._list[int(choice) - 1] for choice in choices]
    
# ------------------------------------------------------------------------------
# Present data
# ------------------------------------------------------------------------------
    
def show_enum(an_enum):
    if isinstance(an_enum, EndUseEnumerationBase):
        return show_enduse_enum(an_enum)
    return show_basic_enum(an_enum)
    
def show_basic_enum(an_enum):
    print(an_enum.name)
    return pd.DataFrame(zip(an_enum.ids, an_enum.names), columns = ["id", "name"])

def show_enduse_enum(an_enum):
    print(an_enum.name)
    return pd.DataFrame([[_id, an_enum.get_name(_id), an_enum.fuel(_id), an_enum.units(_id)] for _id in an_enum.ids],
                        columns = ['id', 'name', 'fuel', 'units'])

def show_elements_with_data(an_enum, ids_with_data):
    print(f"Subset of {an_enum.name} with data:")
    df = pd.DataFrame(zip(an_enum.ids, an_enum.names), columns = ["id", "name"])
    return df[df.id.isin(ids_with_data)]

# ------------------------------------------------------------------------------
# Time handling for plots
# ------------------------------------------------------------------------------

model_tz = pytz.timezone('Etc/GMT+5')

def add_temporal_category(data,temporal_map,category_name):
    data = data.merge(temporal_map,how='left',left_on='time',right_on='from_id')
    data[category_name] = data['to_id']; del data['from_id']; del data['to_id']
    return data

# ------------------------------------------------------------------------------
# Units handling for plots
# ------------------------------------------------------------------------------

unit_factors = {'MW': 1.0,
                'GW': 1.0E-3,
                'TW': 1.0E-6}
unit_cutoffs = OrderedDict([('MW', 1.0E3),
                            ('GW', 1.0E6),
                            ('TW', None)])

# ------------------------------------------------------------------------------
# Dimension handling for plots
# ------------------------------------------------------------------------------

replacements = [('Commercial: ',''),
                ('Residential: ',''),
                ('Appartment','Apartment'),
                ('In','in'),
                ('2to4',' 2 to 4 '),
                ('Apartment in Building 2 to 4 Units','Small Apartment Building'),
                ('Outpatient Treatment Facility','Outpatient Treatment'),
                ('interior','Interior'),
                ('Electro Chemical','Electrochemical'),
                ('Hvac','HVAC')]

enduse_color_map = {'interior_equipment': '#008000',
                    'cooling': '#0000ff',
                    'interior_lights': '#ffff00',
                    'heating': '#db0000',
                    'fans': '#bf008f',
                    'water_systems': '#3399ff', 
                    'exterior_lights': '#ff9933',
                    'pumps': '#996633',
                    'heat_rejection': '#00ffcc',
                    'machine_drive': '#996633', 
                    'process_heating': '#db0000',
                    'facility_hvac': '#bf008f',
                    'electro_chemical_processes': '#888888',
                    'process_cooling_and_refrigeration': '#00ffcc', 
                    'facility_lighting': '#ffff00',
                    'other_process_use': '#008000',
                    'other_facility_support': '#944dff',
                    'conventional_boiler_use': '#990000',
                    'end_use_not_reported': '#ff3399',
                    'onsite_transportation': '#0079C2',
                    'other_nonprocess_use': '#333333',
                    'chp': '#99004D', 
                    'thermal_dg': '#ff1a8c', 
                    'dpv': '#CD9B1D'}

def get_dim_order(datatable,dim):
    return datatable.groupby(dim).sum().sort_values(ascending=False).reset_index()[dim].tolist()

def clean_name(name):
    name = re.sub(r'([a-z])([A-Z])',r'\1 \2',name)
    for repl in replacements:
        name = name.replace(repl[0],repl[1])
    return name

def transform_label(label,enum):
    if not label in transform_label.cache:
        result = enum.get_name(label)
        result = clean_name(result)
        result = textwrap.fill(result, width=18)
        transform_label.cache[label] = result
    return transform_label.cache[label]
transform_label.cache = {'Other': 'Other'}

# ------------------------------------------------------------------------------
# Plot functions
# ------------------------------------------------------------------------------

def component_plot(df, colors, enum, area_filepath, line_filepath, show=False, **kwargs): 
    """
    Used for seasonal_diurnal_profiles
    """
    figsize = kwargs["figsize"]
    font_size = kwargs["font_size"]
    leg_pos = kwargs["leg_pos"]
    legend_kwargs = kwargs["legend_kwargs"]
    subplots_adjust_kwargs = kwargs["subplots_adjust_kwargs"]
    axis_position_kwargs = kwargs["axis_position_kwargs"]

    original_figsize = tuple(matplotlib.rcParams['figure.figsize'])
    matplotlib.rcParams['figure.figsize'] = figsize
    
    if df.min(axis=0).min() < 0.0:
        # truncate at 0.0. from visual inspection, these seem to be rounding errors not real data
        assert df.min(axis=0).min() > -1.0, "Large negative value found: {}".format(df.min(axis=0).min())
        df = df.applymap(lambda x: x if x >= 0.0 else 0.0)
    
    # determine units
    max_val = df.sum(axis=1).max()
    unit_name = None
    for unit_name, cutoff in unit_cutoffs.items():
        if cutoff is not None and max_val < cutoff:
            break
    assert unit_name
    unit_factor = unit_factors[unit_name]
    
    df = df * unit_factor

    axis_position_styler = MatplotlibAxisPosition()
    
    fig_area, axx_area = plt.subplots(4,2,sharex=True,sharey=True)
    fig_line, axx_line = plt.subplots(4,2,sharex=True,sharey=True)
    max_ylim_area = 0.0; max_ylim_line = 0.0
    for i, season in enumerate(['winter','spring','summer','autumn']):
        for j, day_type in enumerate(['weekday','weekend']):
            subplot_df = copy.deepcopy(df).reset_index()
            subplot_df = subplot_df[(subplot_df.season == season) & (subplot_df.day_type == day_type)]
            subplot_df = subplot_df.set_index('hour')
            
            subplot_df.plot(ax=axx_area[i][j],
                            kind='area',
                            color=colors,
                            legend=False)
            subplot_df.plot(ax=axx_line[i][j],
                            kind='line',
                            color=colors,
                            legend=False)
            
            axx_area[i][j].set_xlim((1,24))
            axx_line[i][j].set_xlim((1,24))
            axx_area[i][j].set_xticks([6,12,18,24])
            axx_line[i][j].set_xticks([6,12,18,24])
            
            if i == 0:
                axx_area[i][j].set_title(day_type.title(),fontsize=font_size,color='k')
                axx_line[i][j].set_title(day_type.title(),fontsize=font_size,color='k')
            
            if j == 0:
                axx_area[i][j].set_ylabel('{} ({})'.format(season.title(),unit_name),fontsize=font_size,color='k')
                axx_line[i][j].set_ylabel('{} ({})'.format(season.title(),unit_name),fontsize=font_size,color='k')
                axx_area[i][j].tick_params(axis='y',labelsize=font_size,colors='k')
                axx_line[i][j].tick_params(axis='y',labelsize=font_size,colors='k')
            else:
                axx_area[i][j].yaxis.set_visible(False)
                axx_line[i][j].yaxis.set_visible(False)
                
            if i == 3:
                axx_area[i][j].set_xlabel('Hour',fontsize=font_size,color='k')
                axx_area[i][j].tick_params(axis='x',labelsize=font_size,colors='k')
                axx_line[i][j].set_xlabel('Hour',fontsize=font_size,color='k')
                axx_line[i][j].tick_params(axis='x',labelsize=font_size,colors='k')
                            
            if axx_area[i][j].get_ylim()[1] > max_ylim_area:
                max_ylim_area = axx_area[i][j].get_ylim()[1]
            if axx_line[i][j].get_ylim()[1] > max_ylim_line:
                max_ylim_line = axx_line[i][j].get_ylim()[1]
            if subplot_df.sum(axis=1).max() > max_ylim_area:
                max_ylim_area = subplot_df.sum(axis=1).max()
            if subplot_df.max(axis=1).max() > max_ylim_line:
                max_ylim_line = subplot_df.max(axis=1).max()
                                
    axx_area[0][0].set_ylim((0.0,max_ylim_area*1.05))
    axx_line[0][0].set_ylim((0.0,max_ylim_line*1.01))
            
    handles, labels = axx_area[1][0].get_legend_handles_labels()
    legend = fig_area.legend(handles[::-1],[transform_label(label,enum) for label in labels[::-1]],leg_pos,**legend_kwargs)
    legend.draw_frame(False)
    
    handles, labels = axx_line[1][0].get_legend_handles_labels()
    legend = fig_line.legend(handles[::-1],[transform_label(label,enum) for label in labels[::-1]],leg_pos,**legend_kwargs)
    legend.draw_frame(False)
    
    fig_area.tight_layout()
    fig_line.tight_layout()
    fig_area.subplots_adjust(**subplots_adjust_kwargs)
    fig_line.subplots_adjust(**subplots_adjust_kwargs)
    
    axis_position_styler.postprocess(fig=fig_area,ax=axx_area,**axis_position_kwargs)
    axis_position_styler.postprocess(fig=fig_line,ax=axx_line,**axis_position_kwargs)
    
    if area_filepath is not None: 
        fig_area.savefig(area_filepath,dpi=1200)
        if show:
            fig_area.display()
    if line_filepath is not None:
        fig_line.savefig(line_filepath,dpi=1200)
        if show:
            fig_line.display()
    plt.close(fig_area); plt.close(fig_line)
    matplotlib.rcParams['figure.figsize'] = original_figsize

# ------------------------------------------------------------------------------
# Plot styling helper classes
# ------------------------------------------------------------------------------

class MatplotlibAxisPosition:
    Position = namedtuple('Position', ['x0','y0','x1','y1'])
    FIG_CHILDREN_TO_ADJUST = [matplotlib.text.Text]

    def reset_axis_position(self, a):
        with self._lowleft.hold_trait_notifications():
            self._lowleft.value = ''
        self._upright.value = ''
        return

    def postprocess(self, fig=None, ax=None, **kwargs):
        if fig is None or ax is None: 
            # Only run this in final finishing step
            return
        cur_pos = self._current_position(ax)
        new_pos = self._new_position(cur_pos, **kwargs)
        self._adjust_position(ax, cur_pos, new_pos)
        if fig is not None:
            self._adjust_fig_child_positions(fig, cur_pos, new_pos)
        return

    def _current_position(self, ax):
        if isinstance(ax, np.ndarray):
            result = self.Position(1.0,1.0,0.0,0.0)
            for axx in ax.flatten():
                pos = plt.getp(axx,'position')
                if pos.x0 < result.x0:
                    result = result._replace(x0 = pos.x0)
                if pos.y0 < result.y0:
                    result = result._replace(y0 = pos.y0)
                if pos.x1 > result.x1:
                    result = result._replace(x1 = pos.x1)
                if pos.y1 > result.y1:
                    result = result._replace(y1 = pos.y1)
            return result                    
        return plt.getp(ax,'position')

    def _new_position(self, cur_pos, **kwargs):
        new_pos = self.Position(cur_pos.x0, cur_pos.y0, cur_pos.x1, cur_pos.y1)
        if 'ax_lowleft_pos' in kwargs:
            if kwargs['ax_lowleft_pos']:
                try:
                    lim = str_to_limits(kwargs['ax_lowleft_pos'])
                    new_pos = new_pos._replace(x0=lim[0],y0=lim[1])
                except: pass
            else:
                with self._lowleft.hold_trait_notifications():
                    logger.debug("Setting ax_lowleft_pos")
                    self._lowleft.value = print_float_tuple((cur_pos.x0, cur_pos.y0))
        if 'ax_upright_pos' in kwargs:
            if kwargs['ax_upright_pos']:
                try:
                    lim = str_to_limits(kwargs['ax_upright_pos'])
                    new_pos = new_pos._replace(x1=lim[0],y1=lim[1])
                except: pass
            else:
                with self._upright.hold_trait_notifications():
                    logger.debug("Setting ax_upright_pos")
                    self._upright.value = print_float_tuple((cur_pos.x1, cur_pos.y1))
        return new_pos

    def _adjust_position(self, ax, cur_pos, new_pos):
        logger.debug("Adjusting axis position from {} to {}.".format(cur_pos, new_pos))

        if isinstance(ax, np.ndarray):
            for axx in ax.flatten():
                self._adjust_one_position(axx, cur_pos, new_pos)
            return
        self._adjust_one_position(ax, cur_pos, new_pos)
        return

    def _adjust_fig_child_positions(self, fig, cur_pos, new_pos):
        for child in fig.get_children():
            if any([isinstance(child, child_type) for child_type in self.FIG_CHILDREN_TO_ADJUST]):
                self._adjust_one_position(child, cur_pos, new_pos)

    def _adjust_one_position(self, item, cur_pos, new_pos):
        pos = plt.getp(item, 'position')
        if isinstance(pos, tuple):
            assert len(pos) == 2
            x0 = new_pos.x0 + (new_pos.x1 - new_pos.x0)*(pos[0] - cur_pos.x0)/(cur_pos.x1 - cur_pos.x0)
            y0 = new_pos.y0 + (new_pos.y1 - new_pos.y0)*(pos[1] - cur_pos.y0)/(cur_pos.y1 - cur_pos.y0)
            pos = (x0,y0)
        else:
            pos.x0 = new_pos.x0 + (new_pos.x1 - new_pos.x0)*(pos.x0 - cur_pos.x0)/(cur_pos.x1 - cur_pos.x0)
            pos.y0 = new_pos.y0 + (new_pos.y1 - new_pos.y0)*(pos.y0 - cur_pos.y0)/(cur_pos.y1 - cur_pos.y0)
            pos.x1 = new_pos.x0 + (new_pos.x1 - new_pos.x0)*(pos.x1 - cur_pos.x0)/(cur_pos.x1 - cur_pos.x0)
            pos.y1 = new_pos.y0 + (new_pos.y1 - new_pos.y0)*(pos.y1 - cur_pos.y0)/(cur_pos.y1 - cur_pos.y0)
        # logger.debug("Setting {} position to {}.".format(type(item), pos))
        plt.setp(item,position=pos)
        return                

def str_to_limits(value, default=None):
    if value:
        try:
            m = re.match(r'\(?([0-9\.eE\-+]*), ?([0-9\.eE\-+]*)\)?',value)       
            val1 = float(m.group(1))
            val2 = float(m.group(2))
            return (val1, val2)
        except:
            logger.warning("Unable to interpret {} as a set of limits.".format(value))
    return default  


def print_float_tuple(t):
    return '(' + ",".join(["{:.4f}".format(tt) for tt in t]) + ")" 