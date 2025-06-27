import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DateColumn:
    """Dataclass for setting the date range for a data upload.
    """
    column: str | list[str]
    start_date: str
    end_date: str


class CSVCleaner:
    """
    En klasse til konsistent håndtering af CSV-filer med automatisk datatypekonvertering
    """

    def __init__(self, encodings: List[str] = None):
        # Standard konfiguration for læsning af CSV
        self.csv_config = {
            'sep': ';',  # Dansk standard separator
            'decimal': ',',  # Dansk decimal separator
            'thousands': '.',  # Dansk tusinde separator
            'na_values': ['', ' ', 'nan', 'NaN', 'NULL', 'null', '-', 'N/A'],
            'keep_default_na': True,
            'skipinitialspace': True
        }

        # Encoding fallback liste
        self.encodings = encodings or ['utf-8', 'latin-1', 'cp1252']

        # Dato formater til at prøve (dansk standard først)
        self.date_formats = [
            '%d-%m-%Y',  # 01-12-2024
            '%d/%m/%Y',  # 01/12/2024
            '%Y%m%d',    # 20241201
            '%Y-%m-%d',  # 2024-12-01
            '%d.%m.%Y',  # 01.12.2024
            '%d-%m-%y',  # 01-12-24
            '%d/%m/%y',  # 01/12/24
        ]

    def read_csv_with_types(self, filepath: Path,
                            date_columns: Optional[List[str]] = None,
                            integer_columns: Optional[List[str]] = None,
                            float_columns: Optional[List[str]] = None,
                            date_filter: Optional[DateColumn] = None) -> pd.DataFrame:
        """
        Læser CSV med automatisk datatypekonvertering

        Args:
            filepath: Sti til CSV-fil
            date_columns: Liste over kolonner der skal konverteres til datoer
            integer_columns: Liste over kolonner der skal være heltal
            float_columns: Liste over kolonner der skal være decimaltal
            date_filter: Dict med 'column', 'start_date', 'end_date' for filtrering
        """

        print(f"Læser: {filepath}")

        # Prøv forskellige encodings
        df = None

        for encoding in self.encodings:
            try:
                df = pd.read_csv(filepath, dtype=str, encoding=encoding, **self.csv_config)
                print(f"Succesfuld læsning med encoding: {encoding}")
                break
            except UnicodeDecodeError:
                print(f"Encoding {encoding} fejlede, prøver næste...")
                continue

        if df is None:
            raise ValueError(f"Kunne ikke læse {filepath} med nogen af disse encodings: {self.encodings}")

        print(f"Indlæst {len(df)} rækker og {len(df.columns)} kolonner")
        # print(f"Kolonner: {list(df.columns)}")

        # Rens data
        df = self._clean_basic_data(df)

        # Konverter datatyper
        if date_columns:
            df = self._convert_dates(df, date_columns)

        if integer_columns:
            df = self._convert_integers(df, integer_columns)

        if float_columns:
            df = self._convert_floats(df, float_columns)

        # Anvend dato-filtrering hvis specificeret
        if date_filter:
            df = self._apply_date_filter(df, date_filter)

        return df

    def _clean_basic_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Grundlæggende datarensning"""

        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip().replace(".", "")

        # Konverter tomme strenge til NULL
        df = df.replace(['', ' ', 'nan', 'NaN'], pd.NA).convert_dtypes()

        # Fjern "Unnamed" kolonner som pandas nogle gange tilføjer
        unnamed_cols = df.columns[df.columns.str.contains('^Unnamed', case=False, na=False)]
        if len(unnamed_cols) > 0:
            print(f"Fjerner unnamed kolonner: {list(unnamed_cols)}")
            df = df.drop(columns=unnamed_cols)

        # Trim whitespace fra alle tekstkolonner og erstat mellemrum med underscore i kolonnenavne
        df.columns = df.columns.str.replace(' ', '_').str.strip()
        return df

    def _convert_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """Konverter kolonner til datoer med flere formater"""

        for col in date_columns:
            if col not in df.columns:
                print(f"Advarsel: Kolonne '{col}' findes ikke")
                continue

            print(f"Konverterer {col} til dato...")

            # Prøv forskellige datoformater
            converted = False
            for date_format in self.date_formats:
                try:
                    df[col] = pd.to_datetime(df[col], format=date_format, errors='coerce')
                    successful_conversions = df[col].notna().sum()
                    print(f"  Bruger format {date_format}: {successful_conversions} datoer konverteret")
                    converted = True
                    break
                except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
                    continue

            if not converted:
                # Sidste forsøg med automatisk parsing
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                    print(f"  Bruger automatisk parsing for {col}")
                    converted = True
                except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
                    print(f"  Kunne ikke konvertere {col} til dato")

            # Konverter til standardformat ddmmyyyy (som string)
            if converted:
                df[col] = df[col].dt.strftime('%d%m%Y')
                print(f"  Standardiseret {col} til format: ddmmyyyy")

        return df

    def _convert_integers(self, df: pd.DataFrame, integer_columns: List[str]) -> pd.DataFrame:
        """Konverter til heltal kun hvis der ikke er komma i originaldata"""

        for col in integer_columns:
            if col not in df.columns:
                print(f"Advarsel: Kolonne '{col}' findes ikke")
                continue

            print(f"Konverterer {col} til heltal...")

            # Check om der er komma i dataene (dansk decimal)
            has_decimals = df[col].astype(str).str.contains(',', na=False).any()

            if has_decimals:
                print(f"  {col} indeholder komma - konverterer til float i stedet")
                df[col] = self._safe_float_conversion(df[col])
            else:
                # Konverter til heltal
                df[col] = np.floor(pd.to_numeric(df[col].str.replace(".", ""), errors='coerce')).astype('Int64')

        return df

    def _convert_floats(self, df: pd.DataFrame, float_columns: List[str]) -> pd.DataFrame:
        """Konverter til float med håndtering af dansk decimal format"""

        for col in float_columns:
            if col not in df.columns:
                print(f"Advarsel: Kolonne '{col}' findes ikke")
                continue

            print(f"Konverterer {col} til decimal...")
            df[col] = self._safe_float_conversion(df[col])

        return df

    def _safe_float_conversion(self, series: pd.Series) -> pd.Series:
        """Sikker konvertering til float med dansk format"""

        # Håndter dansk format (komma som decimal, punktum som tusinde)
        def convert_danish_number(value):
            if pd.isna(value) or value == '':
                return np.nan

            value_str = str(value).strip()

            # Hvis både punktum og komma, punktum er tusinde separator
            if '.' in value_str and ',' in value_str:
                value_str = value_str.replace('.', '').replace(',', '.')
            # Hvis kun komma, decimal separator
            elif ',' in value_str:
                value_str = value_str.replace(',', '.')

            try:
                return float(value_str)
            except (ValueError, TypeError):
                return np.nan

        converted = series.apply(convert_danish_number)
        return converted

    def analyze_csv(self, filepath: Path) -> Dict:
        """Analysér CSV-fil for at foreslå datatyper"""

        # Læs først få rækker for at inspicere
        sample_df = pd.read_csv(filepath, dtype=str, nrows=100, **self.csv_config)
        sample_df = self._clean_basic_data(sample_df)

        analysis = {
            'total_columns': len(sample_df.columns),
            'columns': list(sample_df.columns),
            'suggested_types': {},
            'sample_data': {}
        }

        for col in sample_df.columns:
            # Tag ikke-null samples
            samples = sample_df[col].dropna().head(5).tolist()
            analysis['sample_data'][col] = samples

            # Foreslå datatype
            suggested_type = self._suggest_column_type(sample_df[col])
            analysis['suggested_types'][col] = suggested_type

        return analysis

    def _suggest_column_type(self, series: pd.Series) -> str:
        """Foreslå datatype baseret på indhold"""

        # Tag ikke-null værdier
        non_null = series.dropna().astype(str)

        if len(non_null) == 0:
            return 'text'

        # Check for datoer
        date_patterns = [
            r'\d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4}',  # dd-mm-yyyy varianter
            r'\d{4}-\d{1,2}-\d{1,2}'  # yyyy-mm-dd
        ]

        for pattern in date_patterns:
            if non_null.str.match(pattern).any():
                return 'date'

        # Check for tal
        has_comma = non_null.str.contains(',').any()
        is_numeric = non_null.str.replace('[,.]', '', regex=True).str.isdigit().all()

        if is_numeric:
            if has_comma:
                return 'float'
            else:
                return 'integer'

        return 'text'

    def _apply_date_filter(self, df: pd.DataFrame, date_filter: DateColumn) -> pd.DataFrame:
        """Filtrer DataFrame baseret på dato-interval"""

        if not all([date_filter.column, date_filter.start_date, date_filter.end_date]):
            print("Advarsel: Ufuldstændig dato-filter konfiguration")
            return df

        if date_filter.column not in df.columns:
            print(f"Advarsel: Filter-kolonne '{date_filter.column}' findes ikke")
            return df

        print(f"Anvender dato-filter på {date_filter.column}: {date_filter.start_date} til {date_filter.end_date}")

        # Konverter filter-datoer til pandas datetime
        start_dt = pd.to_datetime(date_filter.start_date, format='%Y%m%d')
        end_dt = pd.to_datetime(date_filter.end_date, format='%Y%m%d')

        if isinstance(date_filter.column, list):
            mask = pd.Series(False, index=df.index)
            for col in date_filter.column:
                temp_date_col = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                mask |= (temp_date_col >= start_dt) & (temp_date_col <= end_dt)
        else:
            # Konverter kolonne tilbage til datetime midlertidigt for filtrering
            temp_date_col = pd.to_datetime(df[date_filter.column], format='%Y%m%d', errors='coerce')

            # Anvend filter
            mask = (temp_date_col >= start_dt) & (temp_date_col <= end_dt)

        filtered_df = df.loc[mask]

        print(f"Filtreret fra {len(df)} til {len(filtered_df)} rækker")

        if len(filtered_df) == 0:
            print("Advarsel: Ingen rækker matchede dato-filteret")
            return df

        return filtered_df


def _test_date(self, series: pd.Series) -> float:
    """Test om værdier kan parses som datoer (inkl. kompakte formater)"""
    success_count = 0

    # Specifikke kompakte dato-formater at teste
    compact_formats = [
        '%Y%m%d',    # YYYYMMDD
        '%d%m%Y',    # DDMMYYYY  
        '%m%d%Y',    # MMDDYYYY
        '%y%m%d',    # YYMMDD
        '%d%m%y',    # DDMMYY
    ]

    for value in series:
        try:
            str_val = str(value).strip()

            # Skip hvis det ligner timestamp
            if str_val.isdigit() and len(str_val) in [10, 13]:
                continue

            # Skip hvis det indeholder tid
            if any(indicator in str_val for indicator in [' ', 'T']) and ':' in str_val:
                continue

            parsed = None

            # Først: prøv kompakte formater hvis det er kun cifre
            if str_val.isdigit() and len(str_val) in [6, 8]:
                for fmt in compact_formats:
                    try:
                        parsed = pd.to_datetime(str_val, format=fmt, errors='raise')
                        break
                    except:
                        continue

            # Hvis ikke parsed endnu, prøv automatisk parsing
            if parsed is None:
                parsed = pd.to_datetime(str_val, errors='raise', dayfirst=True)

            # Verificer at det er en rimelig dato
            if pd.Timestamp('1900-01-01') <= parsed <= pd.Timestamp('2100-12-31'):
                success_count += 1

        except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
            continue

    return success_count / len(series) if len(series) > 0 else 0

# Eksempel på brug
def main():
    cleaner = CSVCleaner()

    # Analysér CSV først
    csv_file = Path('data.csv')  # Erstat med din fil

    if csv_file.exists():
        print("=== ANALYSE AF CSV ===")
        analysis = cleaner.analyze_csv(csv_file)

        print(f"Kolonner fundet: {analysis['total_columns']}")
        for col, suggested_type in analysis['suggested_types'].items():
            samples = analysis['sample_data'][col]
            print(f"  {col}: {suggested_type} (eksempel: {samples[:2]})")

        print("\n=== INDLÆSNING MED DATATYPER ===")

        # Definer kolonnetyper baseret på analyse (tilpas til dine data)
        date_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'date']
        int_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'integer']
        float_cols = [col for col, type_ in analysis['suggested_types'].items() if type_ == 'float']

        # Indlæs med korrekte datatyper og dato-filtrering
        date_filter = {
            'column': 'dato_kolonne',  # Erstat med din dato-kolonne
            'start_date': '20231130',   # YYYYMMDD format
            'end_date': '20231231'      # YYYYMMDD format
        }

        df = cleaner.read_csv_with_types(
            csv_file,
            date_columns=date_cols,
            integer_columns=int_cols,
            float_columns=float_cols,
            date_filter=date_filter  # Valgfrit
        )

        print("\n=== RESULTAT ===")
        print("Datatypes efter konvertering:")
        print(df.dtypes)
        print("\nNull værdier pr. kolonne:")
        print(df.isnull().sum())
        print("\nFørste 3 rækker:")
        print(df.head(3))

    else:
        print("CSV-fil ikke fundet. Opret 'data.csv' eller tilpas stien.")


if __name__ == "__main__":
    main()
