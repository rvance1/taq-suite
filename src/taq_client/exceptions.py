class DataMissingError(FileNotFoundError):
    """Raised when requested TAQ data (quotes/trades) is missing."""
    pass

class CrspMappingMissingError(FileNotFoundError):
    """Raised when the CRSP mapping files are missing from the database infrastructure."""
    pass