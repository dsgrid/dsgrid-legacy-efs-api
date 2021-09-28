import h5py
import numpy as np
import pandas as pds
import webcolors


def multi_index(df, cols):
    result = df.copy()
    if len(cols) == 1:
        result.index = result[cols[0]]
    else:
        result.index = pds.MultiIndex.from_tuples(list(zip(*[result[col].tolist() for col in cols])),
                                                  names = cols)
    for col in cols:
        del result[col]
    return result  


def ensure_enum(cls, val):
    """
    Returns the instance of cls that corresponds to val. cls is expected to be 
    an enum-type class.

    Parameters
    ----------
    cls : an Enum class
    val : str or cls object
    
    Returns
    -------
    cls object
    """
    if isinstance(val, str):
        return cls[val]
    return cls(val)


def lighten_color(hex_color,fraction_to_white):
    rgb_color = np.array(webcolors.hex_to_rgb(hex_color))
    white = np.array([255,255,255])
    direction = white - rgb_color
    result = [int(round(x)) for x in list(rgb_color + direction * fraction_to_white)]
    return webcolors.rgb_to_hex(tuple(result))
    

def palette(hex_color,n,max_fraction=0.75):
    result = []; step = max_fraction / float(n)
    for frac in [i * step for i in range(n)]:
        result.append(lighten_color(hex_color,frac))
    assert len(result) == n
    return result


class h5Reader(object):
    def __init__(self, filepath):
        self.filepath = filepath
        if self.is_hsds:
            import h5pyd
            self._f = h5pyd.File(filepath, mode="r", use_cache=False)
        elif self.is_s3:
            import s3fs
            self._s3p = s3fs.S3FileSystem().open(filepath, 'rb')
            self._f = h5py.File(self._s3p, mode="r")
        else:
            self._f = h5py.File(filepath, mode="r")

    @property
    def is_hsds(self):
        return str(self.filepath).startswith("/nrel/")

    @property
    def is_s3(self):
        return str(self.filepath).startswith("s3://")

    def __enter__(self):
        return self._f.__enter__()

    def __exit__(self, exc, value, tb):
        result = self._f.__exit__(exc, value, tb)
        if self.is_s3:
            self._s3p.__exit__(exc, value, tb)
        return result