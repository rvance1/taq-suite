import numpy as np

# 1. DEFINITIVE LITTLE-ENDIAN INDEX (22 bytes)
idx_dtype = np.dtype([
    ('ticker',  'S10'),
    ('date',    '<i4'), 
    ('start',   '<i4'), 
    ('end_idx', '<i4')  
])

# 2. DEFINITIVE LITTLE-ENDIAN BIN SCHEMA (23 bytes)
bin_dtype = np.dtype([
    ('time',     '<i4'),
    ('bid',      '<i4'),
    ('ask',      '<i4'),
    ('seq',      '<i4'),
    ('bid_size', '<i2'), # Fixed: Sizes are 2 bytes!
    ('ask_size', '<i2'), # Fixed: Sizes are 2 bytes!
    ('mode',     '<i2'), # Stored as a 2-byte integer condition code
    ('ex',       'S1')
])