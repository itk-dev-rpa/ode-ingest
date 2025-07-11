import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional


def _extract_date_from_filename(filename: str) -> Optional[datetime]:
    """
    Ekstraherer datostempel fra filnavn på formatet: YYYY-MM-DD

    Args:
        filename: Filnavn som '0751_ODE_2025-06-17_002_01-Bilag-master_Delta_001af2.csv'

    Returns:
        datetime object eller None hvis ikke fundet
    """
    # Match datostempel i format YYYY-MM-DD
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    match = re.search(date_pattern, filename)

    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return None
    return None


def _extract_sequence_number(filename: str) -> int:
    """
    Ekstraherer sekvensnummer fra filnavn.

    Args:
        filename: Filnavn som '0751_ODE_2025-06-17_002_01-Bilag-master_Delta_001af2.csv'

    Returns:
        Sekvensnummer (002 i eksemplet) eller 0 hvis ikke fundet
    """
    # Match sekvensnummer efter dato - antager format _XXX_
    seq_pattern = r'_(\d{3})_'
    match = re.search(seq_pattern, filename)

    if match:
        return int(match.group(1))
    return 0


def get_file_sort_key(filepath: str) -> Tuple[datetime, int, str]:
    """
    Skaber en sort-key for et filnavn baseret på:
    1. Datostempel (ældste først)
    2. Sekvensnummer (laveste først)
    3. Filnavn (alfabetisk som tiebreaker)

    Args:
        filepath: Fuld sti til fil

    Returns:
        Tuple der kan bruges til sortering
    """
    filename = Path(filepath).name

    # Få dato og sekvensnummer
    file_date = _extract_date_from_filename(filename)
    seq_number = _extract_sequence_number(filename)

    # Brug en meget tidlig dato som fallback hvis ikke fundet
    if file_date is None:
        file_date = datetime(1900, 1, 1)

    return (file_date, seq_number, filename)


def sort_files(file_paths: List[str]) -> List[str]:
    """
    Sorterer en liste af filer baseret på datostempel og sekvensnummer.
    
    Args:
        file_paths: Liste af filstier
    
    Returns:
        Sorteret liste med ældste filer først
    """
    return sorted(file_paths, key=get_file_sort_key)
