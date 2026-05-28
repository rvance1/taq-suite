import datetime as dt
from pathlib import Path
from typing import List, Union

def get_file_paths(
    root_dir: Union[str, Path], 
    start_date: dt.date, 
    end_date: dt.date, 
    taq_type: str
) -> List[Path]:
    """
    Returns all parquet files for a given date range, handling both 
    single daily files and partitioned daily folders.
    """
    files_to_scan = []
    current_date = start_date
    root_path = Path(root_dir)

    while current_date <= end_date:
        base_dir = root_path / "interim" / "taq" / str(taq_type) / str(current_date.year) / f"{current_date.month:02d}"
        date_str = current_date.strftime("%Y-%m-%d")

        single_file = base_dir / f"{date_str}.parquet"
        if single_file.is_file():
            files_to_scan.append(single_file)
            
        else:
            folder = base_dir / date_str
            if folder.is_dir():
                files_to_scan.extend(folder.glob("*.parquet"))

        current_date += dt.timedelta(days=1)

    return files_to_scan