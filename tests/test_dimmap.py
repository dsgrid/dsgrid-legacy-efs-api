import logging
import os

import pandas as pds

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.datatable import Datatable
from dsgrid.dataformat.enumeration import enumdata_folder
from .temppaths import TempDir

logger = logging.getLogger(__name__)

here = os.path.dirname(__file__)

def test_aggregation():

    with TempDir() as wdir:
        # pull population data and use pandas to calculate state and 
        # national-level aggregations
        population = Datafile.load(os.path.join(here,'data','v0.2.0','population.dsg'),
                                   new_filepath=os.path.join(wdir,'population.dsg'))
        df = Datatable(population,sort=False,verify_integrity=False).data
        df.name = 'population'
        df = df.reset_index()

        counties_to_states = pds.read_csv(os.path.join(enumdata_folder,'counties_to_states.csv'), dtype=str)
        conus_to_states = pds.read_csv(os.path.join(enumdata_folder,'conus_to_states.csv'), dtype=str)
        df = df.merge(counties_to_states,how='left',left_on='geography',right_on='from_id')
        df['state'] = df['to_id']
        for col in ['from_id','to_id']:
            del df[col]
        df['conus'] = df['state'].apply(lambda x: True if x in conus_to_states['to_id'].tolist() else False)

        logger.debug("Head of DataFrame:\n{}".format(df.head()))
        logger.debug("Head of non-CONUS part of DataFrame:\n{}".format(df[df.conus == False].head()))

        # HERE -- Started writing this test then found that my problem was 
        # sloppy CONUS filtering.

        # create fake timeseries by timezone and store population data that way
        # population_2.dsg


        # aggregate population_2.dsg along time dimension and compare to 
        # population.dsg



        # aggregate population.dsg to state level and compare to pandas result


        # aggregate population_2.dsg to state level and compare to 
        # aggregated population.dsg result


        # aggregate population.dsg-state level to CONUS and compare to pandas 
        # result


        # aggregate population_2.dsg-state level to CONUS and compare to 
        # aggregated population.dsg result
