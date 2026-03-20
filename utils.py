import os
import re
import time
import uuid
from pathlib import Path
from datetime import datetime, date

import pandas as pd
from sqlalchemy import text


def extract_file_period(filename):
    stem = Path(filename).stem
    pattern = re.search(r"(\d{2}\.\d{2}\.\d{4})\s*[-–]\s*(\d{2}\.\d{2}\.\d{4})", stem)
    pattern_with_letters = re.search(
        pattern=r"[cс]\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})",
        string=stem,
        flags=re.IGNORECASE,
    )

    if pattern:
        start_str, end_str = pattern.groups()
    elif pattern_with_letters:
        start_str, end_str = pattern_with_letters.groups()
    else:
        print(f"Имя файла {filename} не подходит под необходимый формат")
        return None

    start_str, end_str = (pattern or pattern_with_letters).groups()
    start_date = datetime.strptime(start_str, "%d.%m.%Y").date()
    end_date = datetime.strptime(end_str, "%d.%m.%Y").date()
    return start_date, end_date


def find_date_range_files(folder_path, rc_name, target_start, target_end):
    valid_extensions = {".csv", ".xls", ".xlsx"}
    matching_files = []

    for dirpath, _, filenames in os.walk(folder_path):
        for file in filenames:
            if Path(file).suffix.lower() not in valid_extensions:
                print(
                    f"Function find_date_range_files: File {file} has invalid extension"
                )
                continue

            file_path = os.path.join(dirpath, file)
            if rc_name == "ПАО ТНС ЭНЕРГО ЯРОСЛАВЛЬ":
                parent_folder = Path(dirpath).name
                try:
                    end_date = datetime.strptime(parent_folder, "%d.%m.%Y").date()
                    start_date = end_date.replace(day=1)
                except ValueError:
                    print(f"  Пропуск папки с некорректной датой: {parent_folder}")
                    continue
            elif rc_name in ["ЭСВ", "Т+", "ЮП РКЦ"]:
                stem = Path(file).stem
                if rc_name not in stem:
                    print(f"  Пропуск файла (не содержит '{rc_name}'): {file}")
                    continue
                period = extract_file_period(file)
                if period is None:
                    continue
                start_date, end_date = period
            else:
                period = extract_file_period(file)
                if period is None:
                    continue
                start_date, end_date = period

            if start_date <= target_end and target_start <= end_date:
                matching_files.append(file_path)

    return matching_files


def find_end_date_in_table(sheet):
    end_date = None
    for row in sheet.iter_rows(
        min_row=1, max_row=14, min_col=1, max_col=12, values_only=True
    ):
        if "Дата окончания" in row:
            end_date = row[3]
    return end_date


def find_end_date_by_name(filename_str):
    template_str = r"^(\d{2}\.\d{2}\.\d{4})-(\d{2}\.\d{2}\.\d{4}).*$"
    match = re.search(template_str, filename_str)
    return f"{match.group(2)}"


def get_last_second_timestamp_from_date_str(date_str):
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        last_sec = dt.replace(hour=23, minute=59, second=59, microsecond=0)
        return int(last_sec.timestamp())
    except (ValueError, TypeError):
        print(
            f"Exception in get_last_second_timestamp_from_date_str: Cant' cast date to timestamp"
        )
        return float("nan")


def convert_and_rename_columns(df, original_columns):
    target_columns = ("ls_str", "date_ts", "sum_float")
    columns = {
        orig_name: target_name
        for orig_name, target_name in zip(original_columns, target_columns)
    }
    renamed_df = df.rename(columns=columns)

    def safe_convert_ls_to_str(value):
        if pd.isna(value):
            return ""

        if isinstance(value, str):
            return value.strip()

        try:
            float_val = float(value)
            if float_val.is_integer():
                if abs(float_val) > 2**31 - 1:
                    return (
                        str(int(float_val))
                        if float_val <= 2**63 - 1
                        else str(int(float_val))
                    )
                return str(int(float_val))
            else:
                return (
                    str(float_val).rstrip("0").rstrip(".")
                    if "." in str(float_val)
                    else str(float_val)
                )
        except (ValueError, TypeError):
            return str(value)

    renamed_df["ls_str"] = renamed_df["ls_str"].apply(safe_convert_ls_to_str)
    renamed_df["ls_str"] = renamed_df["ls_str"].str.replace(r"\.0$", "", regex=True)
    renamed_df["date_ts"] = renamed_df["date_ts"].apply(
        get_last_second_timestamp_from_date_str
    )
    renamed_df["sum_float"] = pd.to_numeric(
        renamed_df["sum_float"], errors="coerce"
    ).round(2)
    return renamed_df[["ls_str", "date_ts", "sum_float"]]


def extract_final_date(raw_date):
    pattern = r"с (\d{2}\.\d{2}\.\d{4}) по (\d{2}\.\d{2}\.\d{4})"
    try:
        match = re.search(pattern, raw_date)
    except ValueError:
        print(f"For raw_date {raw_date} pattern was not found")
    return match.group(2)


def find_header_by_columns(file_path, required_cols, max_header=5):
    for header in range(max_header):
        try:
            df = pd.read_excel(file_path, header=header, nrows=1)
            if all(col in df.columns for col in required_cols):
                return header
        except Exception:
            continue
    raise ValueError(f"Не найден заголовок с колонками {required_cols} в {file_path}")


def load_payment_fl_to_sql(
    df, db_engine, rc_name, start_period, end_period, user_str="DWH/PAY_FL_IMPORT"
):
    start_time = datetime.now()
    print("=" * 80)
    print(f"НАЧАЛО ЗАГРУЗКИ В SQL. Период: {start_period} – {end_period}")
    print("=" * 80)

    if df.empty:
        return "Нет данных для загрузки"

    try:
        finance_map = {"sum_float": "sum_float"}
        numeric_cols = list(finance_map.keys())

        df = df.copy()
        df["ls_str"] = df["ls_str"].astype(str)

        for col in numeric_cols:
            if col in df.columns:
                series = df[col].astype(str)
                series = series.str.replace(r"\s|\xa0", "", regex=True)
                series = series.str.replace(",", ".", regex=False)
                df[col] = pd.to_numeric(series, errors="coerce")

        print("Очистка финансовых колонок завершена")

        def date_to_end_of_day_ts(d: date) -> int:
            dt = datetime(d.year, d.month, d.day, 23, 59, 59)
            return int(dt.timestamp())

        start_ts = int(datetime.combine(start_period, datetime.min.time()).timestamp())
        end_ts = date_to_end_of_day_ts(end_period)

        with db_engine.connect() as conn:
            with conn.begin():
                delete_query = text("""
                    DELETE payment_fl
                    FROM payment_fl
                    JOIN entity_ls AS ls ON payment_fl.entity_ls_uuid = ls.uuid
                    WHERE payment_fl.date_ts >= :start_ts
                    AND payment_fl.date_ts <= :end_ts
                    AND ls.rc_str = :rc_name
                """)

                result = conn.execute(
                    delete_query,
                    {"start_ts": start_ts, "end_ts": end_ts, "rc_name": rc_name},
                )
                print(f"Удалено записей: {result.rowcount}")

                if not df.empty:
                    temp_table = f"#{uuid.uuid4().hex[:12]}"
                    col_list = ["ls_str", "date_ts"] + [
                        c for c in numeric_cols if c in df.columns
                    ]
                    df[col_list].to_sql(temp_table, conn, if_exists="fail", index=False)
                    print(f"Данные загружены во временную таблицу {temp_table}")

                    modified_ts = int(time.time())
                    enriched_cols = ", ".join(
                        [f'r."{c}"' for c in col_list if c != "ls_str"]
                    )

                    insert_query = text(f"""
                        INSERT INTO payment_fl(
                            uuid, entity_ls_uuid, date_ts, sum_float,
                            created_user_str, modified_user_str, created_ts, modified_ts
                        )
                        SELECT 
                            CONVERT(varchar(36), NEWID()),
                            ls.uuid AS entity_ls_uuid,
                            {enriched_cols},
                            '{user_str}',
                            '{user_str}',
                            {modified_ts},
                            {modified_ts}
                        FROM {temp_table} AS r
                        JOIN entity_ls AS ls ON r.ls_str = ls.ls_str
                    """)

                    result = conn.execute(insert_query)
                    print(f"Вставлено новых записей: {result.rowcount}")
                else:
                    print("Нет данных для вставки")

    except Exception as e:
        error_msg = f"КРИТИЧЕСКАЯ ОШИБКА: {e}"
        print(error_msg)
        return error_msg

    duration = datetime.now() - start_time

    return (
        f"\n{'='*80}\n"
        f"ЗАГРУЗКА ЗАВЕРШЕНА\n"
        f"Период: {start_period} – {end_period}\n"
        f"Время исполнения: {str(duration).split('.')[0]}\n"
        f"{'='*80}"
    )
