import numpy as np
from .taq_file import TaqType

def get_bin_dtype(type: TaqType, record_size: int) -> np.dtype:

    if type == TaqType.TRADE:
        match record_size:
            case 22:
                return T_BIN_DTYPE_22
            case _:
                raise ValueError(f"Unexpected record size: {record_size}")
    elif type == TaqType.QUOTE:
        match record_size:
            case 23:
                return Q_BIN_DTYPE_23
            case 27:
                return Q_BIN_DTYPE_27
            case _:
                raise ValueError(f"Unexpected record size: {record_size}")
    else:
        raise ValueError(f"Unexpected TaqType: {type}")


# 1. DEFINITIVE LITTLE-ENDIAN INDEX (22 bytes)
IDX_DTYPE = np.dtype([
    ('ticker',  'S10'),
    ('date',    '<i4'),
    ('start_idx',   '<i4'),
    ('end_idx', '<i4')
])

# 2. TRADE RECORD (22 bytes) -- 1993 TAQ consolidated trade file
T_BIN_DTYPE_22 = np.dtype([
    ('time',    '<i4'),
    ('price',   '<i4'),
    ('volume',  '<i4'),
    ('seq',     '<i4'),
    ('cond',    '<i4'),
    ('sale',    'S1'),
    ('ex',      'S1'),
])

Q_BIN_DTYPE_23 = np.dtype([
    ('time',     '<i4'),
    ('bid',      '<i4'),
    ('ask',      '<i4'),
    ('seq',      '<i4'),
    ('bid_size', '<i2'),
    ('ask_size', '<i2'),
    ('mode',     '<i2'),
    ('ex',       'S1'),
])

Q_BIN_DTYPE_27 = np.dtype([
    ('time',     '<i4'),
    ('bid',      '<i4'),
    ('ask',      '<i4'),
    ('seq',      '<i4'),
    ('bid_size', '<i2'),
    ('ask_size', '<i2'),
    ('mode',     '<i2'),
    ('ex',       'S1'),
    ('extra',    'S4'),
])