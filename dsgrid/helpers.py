import pandas as pds

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