import ode_ingest as ode
import config


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
    for table in tables:
        check_table(table)
