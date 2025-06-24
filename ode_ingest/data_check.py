from pathlib import Path
import config
from csv_cleaner import CSVCleaner
import ode_ingest as ode


tables = [
    "BO-aftale-haendelse",
    "BO-aftale",
    "Bilag-aaben",
    "Bilag-master",
    "Bilag-lukket",
    "FP-aftale",
    "Forretningspartner",
    "Indbetalinger",
    "Opsaetning-Aftalekontotype",
    "Opsaetning-Rykkerniveau",
    "RIM-aftale-rater",
    "RIM-aftale-renter",
    "RIM-aftale",
    "Rykker",
    "UU-aftale-haefter",
    "UU-aftale",
]


def check_table(name):
    ode.unique_columns(config.FILE_DIRECTORY, [f"{name}_"])


if __name__ == "__main__":
    cleaner = CSVCleaner()
    files = ode.find_files(config.FILE_DIRECTORY, ["BO-aftale"])
    df = ode.dataframe_from_csv(files[0])
    csv_file = Path(files[0])
    if csv_file.exists():
        analysis = cleaner.analyze_csv(csv_file)

        # Definer kolonnetyper baseret på analyse (tilpas til dine data)
        date_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'date']
        int_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'integer']
        float_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'float']

        # Indlæs med korrekte datatyper
        df = cleaner.read_csv_with_types(
            csv_file,
            date_columns=date_cols,
            integer_columns=int_cols,
            float_columns=float_cols
        )
    print("done")
