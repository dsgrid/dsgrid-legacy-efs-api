import numpy as np
import pandas as pd
from dsgrid.dataformat.datatable import Datatable
from .temppaths import TempFilepath
from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.enumeration import (
    sectors_subsectors, states, enduses, hourly2012
)

def make_data(enduses, times):
    return pd.DataFrame({
        enduse: np.random.rand(len(times))
        for enduse in enduses
        },
       index=pd.CategoricalIndex(times),
       dtype="float32")


def test_datatable_read():

    eus1 = ["heating", "cooling"]
    data1 = make_data(eus1, hourly2012.ids)
    data2 = make_data(eus1, hourly2012.ids)

    eus2 = ["heating", "cooling", "fans", "pumps"]
    data3 = make_data(eus2, hourly2012.ids)
    data4 = make_data(eus2, hourly2012.ids)

    with TempFilepath() as filepath:

        datafile = Datafile(filepath, sectors_subsectors, states, enduses, hourly2012)

        sector = datafile.add_sector("com__Laboratory", eus1)
        sector.add_data(data1, ["CO", "TN", "CA"], [1.0, 2.3, 6.7])
        sector["IL"] = data2

        sector = datafile.add_sector("ind__11", eus2)
        sector.add_data(data3, ["KS", "MO"], [1.3, 8.6])
        sector["WA"] = data4

        dt = Datatable(datafile)

        assert(len(dt.data) == 8784*(2*4 + 4*3))

        pd.testing.assert_series_equal(
            dt.data.xs(("com__Laboratory", "CO", "heating")),
            data1["heating"], check_names=False)

        co_lab_heating = dt["com__Laboratory", "CO", "heating", :]
        np.testing.assert_allclose(
            np.array(co_lab_heating),
            np.array(data1["heating"]),
            rtol=1e-6)

        pd.testing.assert_series_equal(
            dt.data.xs(("com__Laboratory", "CA", "cooling")),
            data1["cooling"]*6.7, check_names=False)

        ca_lab_cooling = dt["com__Laboratory", "CA", "cooling", :]
        np.testing.assert_allclose(
            np.array(ca_lab_cooling),
            np.array(data1["cooling"])*6.7,
            rtol=1e-6)

        pd.testing.assert_series_equal(
            dt.data.xs(("com__Laboratory", "IL", "cooling")),
            data2["cooling"], check_names=False)

        pd.testing.assert_series_equal(
            dt.data.xs(("ind__11", "KS", "pumps")),
            data3["pumps"]*1.3, check_names=False)

        pd.testing.assert_series_equal(
            dt.data.xs(("ind__11", "WA", "fans")),
            data4["fans"], check_names=False)

        dt.data.groupby(level=["geography", "enduse"]).sum()
