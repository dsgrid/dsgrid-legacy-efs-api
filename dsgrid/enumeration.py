import numpy as np

ENCODING = "utf-8"

class Enumeration(object):

    enum_dtype = np.dtype([
        ("id", "S64"),
        ("name", "S64")
    ])

    dimension = None

    @staticmethod
    def checkvalues(ids, names):

        n_ids = len(ids)
        n_names = len(names)

        if n_ids != n_names:
            raise ValueError("Number of ids (" + str(n_ids) +
                             ") must match number of names (" + str(n_names) + ")")

        if n_ids > 65535:
            raise ValueError("Enumeration cannot contain more than 65535 values: " +
                             str(n_ids) + " provided")
        # 0 to 2^16-2 for indices (2^16-1 slots)
        # 2^16-1 for zero-sentinel

        if len(set(ids)) != n_ids:
            raise ValueError("Enumeration ids must be unique")

        if max(len(value) for value in ids) > 64:
            raise ValueError("Enumeration ids must be less than 64 characters")

        if max(len(value) for value in names) > 64:
            raise ValueError("Enumeration names must be less than 64 characters")


    def __init__(self, name, ids, names):

        Enumeration.checkvalues(ids, names)

        self.name = name
        self.ids = ids
        self.names = names

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.__dict__ == other.__dict__
        )

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()


    def persist(self, h5group):

        dset = h5group.create_dataset(
            self.dimension,
            dtype=self.enum_dtype,
            shape=(len(self.ids),))

        dset.attrs["name"] = self.name

        dset["id"] = np.array(self.ids)
        dset["name"] = np.array(self.names)

        return dset

    @classmethod
    def load(cls, h5group):
        h5dset = h5group[cls.dimension]
        return cls(
            h5dset.attrs["name"],
            [vid.decode(ENCODING) for vid in h5dset["id"]],
            [vname.decode(ENCODING) for vname in h5dset["name"]]
        )

class SectorEnumeration(Enumeration):
    dimension = "sector"

class GeographyEnumeration(Enumeration):
    dimension = "geography"

class EndUseEnumeration(Enumeration):
    dimension = "enduse"

class TimeEnumeration(Enumeration):
    dimension = "time"

sectors = SectorEnumeration("sector_subsectors",
                            ["res__sfd"],
                            ["Residential: Single Family Detached"])
counties = GeographyEnumeration("counties",
                                ["01001"],
                                ["A county"])
enduses = EndUseEnumeration("enduses",
                            ["water_heating"],
                            ["Water Heating"])
hourly2012 = TimeEnumeration("2012 Hourly",
                             ["hour1"],
                             ["hour1"])
