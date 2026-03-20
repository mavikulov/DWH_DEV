import os
from pathlib import Path
from collections import defaultdict
from datetime import date, datetime

import sqlalchemy
import pandas as pd

from dotenv import load_dotenv

from parsers import (
    parse_vladimir_esv,
    parse_vladimir_tplus,
    parse_vladimir_up_rkc,
    parse_mosobl_eirc,
    parse_mosoble_mosenergo,
    parse_tula,
    parse_garant_invest,
    parse_yaroslavl_irc,
    parse_yaroslavl_tns,
)

from utils import (
    find_date_range_files,
    find_date_range_files,
    extract_file_period,
    load_payment_fl_to_sql,
)

RC_DIRS_MAPPER = {
    "ЭСВ": r"Владимирский",
    "Т+": r"Владимирский",
    "ЮП РКЦ": r"Владимирский",
    "МОСОБЛЕИРЦ": r"Подмосковный\ЕИРЦ",
    "МОСЭНЕРГОСБЫТ": r"Подмосковный\Мосэнергосбыт",
    "ТУЛЬСКИЙ": r"Тульский",
    "ГАРАНТИНВЕСТ": r"Ярославский\ГарантИнвест",
    "МУП ИРЦ": r"Ярославский\ИРЦ",
    "НАО РКЦ": r"Ярославский\НАО",
    "ПАО ТНС ЭНЕРГО ЯРОСЛАВЛЬ": r"Ярославский\ТНС",
    "ЯРОБЛ ЕИРЦ": r"Ярославский\ЯрОбл",
}


CONFIG_LIST = [
    {
        "rc_names": RC_DIRS_MAPPER.keys(),
        "start_period": (1, 12, 2025),
        "end_period": (31, 1, 2026),
    },
]


PARSER_MAP = {
    "ЭСВ": parse_vladimir_esv,
    "Т+": parse_vladimir_tplus,
    "ЮП РКЦ": parse_vladimir_up_rkc,
    "МОСОБЛЕИРЦ": parse_mosobl_eirc,
    "МОСЭНЕРГОСБЫТ": parse_mosoble_mosenergo,
    "ТУЛЬСКИЙ": parse_tula,
    "ГАРАНТИНВЕСТ": parse_garant_invest,
    "МУП ИРЦ": parse_yaroslavl_irc,
    "НАО РКЦ": None,
    "ПАО ТНС ЭНЕРГО ЯРОСЛАВЛЬ": parse_yaroslavl_tns,
    "ЯРОБЛ ЕИРЦ": None,
}


dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

if __name__ == "__main__":
    base_path = r"C:\DWH_DEV\PAY_FL_IMPORT\Платежи"
    load_to_sql = False
    all_dataframes = []
    db_connection = os.getenv("DATABASE_URL")
    db_engine = sqlalchemy.create_engine(db_connection)

    for config in CONFIG_LIST:
        vladimir_rc_names = ["ЭСВ", "Т+", "ЮП РКЦ"]
        dataframes_per_config = []
        rc_names = config.get("rc_names")
        start_period = config.get("start_period")
        end_period = config.get("end_period")
        s_day, s_month, s_year = start_period
        e_day, e_month, e_year = end_period
        target_start = date(s_year, s_month, s_day)
        target_end = date(e_year, e_month, e_day)
        rc_files = defaultdict(list)
        print(f"Анализ периода: {target_start} – {target_end}")

        for rc_name in rc_names:
            print(f"\nОбработка РЦ: {rc_name}")
            rc_folder = RC_DIRS_MAPPER.get(rc_name)
            if rc_folder is None:
                print(f"Не найден путь для РЦ: {rc_name}")
                continue

            folder_path = os.path.join(base_path, rc_folder)
            if not os.path.exists(folder_path):
                print(f"Директория не найдена: {folder_path}")
                continue

            matched_files = find_date_range_files(
                folder_path, rc_name, target_start, target_end
            )
            print(f"  Найдено подходящих файлов: {len(matched_files)}")
            for file in matched_files:
                print(f"    - {file}")

            dataframes_per_rc = []
            parser_func = PARSER_MAP.get(rc_name)

            if parser_func is None:
                print(f"Пропуск {rc_name}: нет парсера")
                continue

            all_filtered_dfs = []
            report_rows = []
            for file_path in matched_files:
                print(f"Парсинг: {os.path.basename(file_path)}")
                df = parser_func(file_path)
                target_start_ts = int(
                    datetime.combine(target_start, datetime.min.time()).timestamp()
                )
                target_end_ts = int(
                    datetime.combine(
                        target_end, datetime.max.time().replace(microsecond=0)
                    ).timestamp()
                )

                mask = (df["date_ts"] >= target_start_ts) & (
                    df["date_ts"] <= target_end_ts
                )

                filtered_df = df[mask].copy()
                if not filtered_df.empty:
                    all_filtered_dfs.append(filtered_df)
                    row_count = len(filtered_df)
                    total_sum = filtered_df["sum_float"].sum().round(2)

                    if rc_name == "ПАО ТНС ЭНЕРГО ЯРОСЛАВЛЬ":
                        parent_dir = Path(file_path).parent.name
                        try:
                            end_date = datetime.strptime(parent_dir, "%d.%m.%Y").date()
                            start_date = end_date.replace(day=1)
                            start_date_str = start_date.strftime("%d.%m.%Y")
                            end_date_str = end_date.strftime("%d.%m.%Y")
                        except:
                            start_date_str = end_date_str = "unknown"
                    else:
                        period = extract_file_period(os.path.basename(file_path))
                        if period:
                            start_date_str = period[0].strftime("%d.%m.%Y")
                            end_date_str = period[1].strftime("%d.%m.%Y")
                        else:
                            start_date_str = end_date_str = "unknown"

                    report_rows.append(
                        {
                            "start_date": start_date_str,
                            "end_date": end_date_str,
                            "row_count": row_count,
                            "source_file_path_str": os.path.abspath(file_path),
                            "parser_str": parser_func.__name__,
                            "total_sum": total_sum,
                        }
                    )

            if all_filtered_dfs:
                final_df_for_sql = pd.concat(all_filtered_dfs, ignore_index=True)
                print(f"  Итого для загрузки: {len(final_df_for_sql)} записей")

                if load_to_sql:
                    status = load_payment_fl_to_sql(
                        df=final_df_for_sql,
                        rc_name=rc_name,
                        db_engine=db_engine,
                        user_str="DWH/PAY_FL_IMPORT",
                        start_period=target_start,
                        end_period=target_end,
                    )
                    print(f"{status}")

                if report_rows:
                    stat_df = pd.DataFrame(report_rows)
                    verbose_folder = "payment_verbose"
                    if not os.path.exists(verbose_folder):
                        os.makedirs(verbose_folder, exist_ok=True)

                    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    stat_df.to_excel(
                        os.path.join(
                            verbose_folder,
                            f"payment_df_stat_total_{rc_name}-{now}.xlsx",
                        )
                    )
                    print(
                        f"\n Успешно сохранено payment_df_stat_total_{rc_name}-{now}.xlsx"
                    )
            else:
                print(f"    Нет данных в периоде для {rc_name}")
