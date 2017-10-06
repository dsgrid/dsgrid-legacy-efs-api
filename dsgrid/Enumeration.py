import numpy as np
import h5py

class Enumeration(object):

    enum_dtype = np.dtype([
        ("id", "S64"),
        ("name", "S64")
    ])

    @staticmethod
    def checkvalues(ids, names):

        n_ids = len(ids)
        n_names = len(names)

        if n_ids != n_names:
            raise ValueError("Number of ids (" + n_ids +
                             ") must match number of names (" + n_names + ")")

        if n_ids > 65535:
            raise ValueError("Enumeration cannot contain more than 65535 values: " +
                             n_ids + " provided")
        # 0 to 2^16-2 for indices (2^16-1 slots)
        # 2^16-1 for zero-sentinel

        if len(set(ids)) != n_ids:
            raise ValueError("Enumeration ids must be unique")

        if max(len(value) for value in ids) > 64:
            raise ValueError("Enumeration ids must be less than 64 characters")

        if max(len(value) for value in names) > 64:
            raise ValueError("Enumeration names must be less than 64 characters")


    def __init__(self, dimension, name, ids, names):

        Enumeration.checkvalues(ids, names)

        self.dimension = dimension
        self.name = name
        self.ids = ids
        self.names = names

    def persist(self, h5group):

        dset = h5group.create_dataset(
            self.dimension,
            dtype=enum_dtype)

        dset.attrs.name = self.name

        dset["id"] = np.array(self.ids)
        dset["name"] = np.array(self.names)

        return dset

class SectorEnumeration(Enumeration):

    def __init__(self, name, values):
        super().__init__("time", name, values)


class GeographyEnumeration(Enumeration):

    def __init__(self, name, values):
        super().__init__("geography", name, values)


class EndUseEnumeration(Enumeration):

    def __init__(self, name, values):
        super().__init__("enduse", name, values)


class TimeEnumeration(Enumeration):

    def __init__(self, name, values):
        super().__init__("time", name, values)

enums = {
    "geography": GeographyEnumeration,
    "sector": SectorEnumeration,
    "enduse": EndUseEnumeration,
    "time": TimeEnumeration
}
