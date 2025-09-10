import os
import csv
import sys
import json
import time
import logging
import argparse
import traceback
import subprocess

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
    level=logging.INFO,
)
csv.register_dialect(
    "csv", delimiter=",", quoting=csv.QUOTE_MINIMAL, quotechar='"', doublequote=True,
    escapechar=None, lineterminator="\n", skipinitialspace=False,
)
csv.register_dialect(
    "tsv", delimiter="\t", quoting=csv.QUOTE_NONE, quotechar=None, doublequote=False,
    escapechar=None, lineterminator="\n", skipinitialspace=False,
)

import pdf_utils  # file from v2l


def read_lines(file, write_log=True):
    if write_log:
        logger.info(f"Reading {file}")

    with open(file, "r", encoding="utf8") as f:
        line_list = f.read().splitlines()

    if write_log:
        lines = len(line_list)
        logger.info(f"Read {lines:,} lines")
    return line_list


def write_lines(file, line_list, write_log=True):
    if write_log:
        lines = len(line_list)
        logger.info(f"Writing {lines:,} lines")

    with open(file, "w", encoding="utf8") as f:
        for line in line_list:
            f.write(f"{line}\n")

    if write_log:
        logger.info(f"Written to {file}")
    return


def read_json(file, write_log=True):
    if write_log:
        logger.info(f"Reading {file}")

    with open(file, "r", encoding="utf8") as f:
        data = json.load(f)

    if write_log:
        objects = len(data)
        logger.info(f"Read {objects:,} objects")
    return data


def write_json(file, data, indent=None, write_log=True):
    if write_log:
        objects = len(data)
        logger.info(f"Writing {objects:,} objects")

    with open(file, "w", encoding="utf8") as f:
        json.dump(data, f, indent=indent)

    if write_log:
        logger.info(f"Written to {file}")
    return data


def read_csv(file, dialect, write_log=True):
    if write_log:
        logger.info(f"Reading {file}")

    with open(file, "r", encoding="utf8", newline="") as f:
        reader = csv.reader(f, dialect=dialect)
        row_list = [row for row in reader]

    if write_log:
        rows = len(row_list)
        logger.info(f"Read {rows:,} rows")
    return row_list


def write_csv(file, dialect, row_list, write_log=True):
    if write_log:
        rows = len(row_list)
        logger.info(f"Writing {rows:,} rows")

    with open(file, "w", encoding="utf8", newline="") as f:
        writer = csv.writer(f, dialect=dialect)
        for row in row_list:
            writer.writerow(row)

    if write_log:
        logger.info(f"Written to {file}")
    return


def extract_text_from_pdf(pdf_list_file, pdf_dir, text_dir, start, end):
    pdf_list = read_lines(pdf_list_file)
    pmid_list = []
    for pdf in pdf_list:
        assert pdf.endswith(".pdf")
        int(pdf[:-4])
        pmid_list.append(pdf[:-4])
    del pdf_list
    pmid_list = sorted(pmid_list, key=lambda pmid: int(pmid))

    pmid_to_line_list = {}
    lines = 0
    fails = 0

    for pi in range(start, end + 1):
        pmid = pmid_list[pi - 1]
        pdf = os.path.join(pdf_dir, f"{pmid}.pdf")
        try:
            line_list, _ = pdf_utils.get_pdf_objects(pdf, False)
        except:
            logger.info(f"pmid {pmid}: pdf-to-text failed")
            traceback.print_exc()
            fails += 1
            continue
        pmid_to_line_list[pmid] = line_list
        lines += len(line_list)
        if pi % 100 == 0 or pi == end:
            logger.info(f"{pi:,}/[{start},{end}]: {lines:,} lines; {fails:,} fails")

    text_file = os.path.join(text_dir, f"{start}_{end}.json")
    write_json(text_file, pmid_to_line_list)
    return


def collect_pmid_to_text(text_dir, pmid_to_text_file):
    file_list = os.listdir(text_dir)
    file_list = sorted(file_list, key=lambda x: int(x.split("_")[0]))
    pmid_to_text = {}

    for file in file_list:
        file = os.path.join(text_dir, file)
        sub_pmid_to_text = read_json(file)
        for pmid, text in sub_pmid_to_text.items():
            pmid_to_text[pmid] = text
    write_json(pmid_to_text_file, pmid_to_text)
    return


def get_modulo_pmid_list(all_pdf_list_file, pmid_skip_file, divide, remain):
    pmid_skip_list = read_lines(pmid_skip_file)
    pmid_skip_set = set(pmid_skip_list)
    del pmid_skip_list

    pdf_list = read_lines(all_pdf_list_file)
    all_pmids = len(pdf_list)
    pmid_list = []

    for pdf in pdf_list:
        assert pdf.endswith(".pdf")
        int(pdf[:-4])
        pmid = pdf[:-4]
        if pmid not in pmid_skip_set:
            pmid_list.append(pmid)
    del pmid_skip_set
    del pdf_list
    all_todos = len(pmid_list)
    pmid_list = [
        pmid
        for pmid in pmid_list
        if int(pmid) % divide == remain
    ]
    mod_todos = len(pmid_list)
    logger.info(f"{all_pmids:,} all_pmids; {all_todos:,} all_todos; {mod_todos:,} mod_todos")
    pmid_list = sorted(pmid_list, key=lambda pmid: int(pmid))
    return pmid_list


def extract_text_from_pdf_by_subprocess(
        all_pdf_list_file, pdf_dir,
        pmid_skip_file, mod_dir,
        divide, remain,
        patience=60,
):
    pmid_list = get_modulo_pmid_list(all_pdf_list_file, pmid_skip_file, divide, remain)
    target_dir = os.path.join(mod_dir, f"{divide}_{remain}")
    buffer_dir = os.path.join(target_dir, "buffer")
    os.makedirs(buffer_dir, exist_ok=True)

    pmids = len(pmid_list)
    pmid_to_text = {}

    for pi, pmid in enumerate(pmid_list):
        source_file = os.path.join(pdf_dir, f"{pmid}.pdf")
        target_file = os.path.join(buffer_dir, f"{pmid}.txt")

        if not os.path.exists(target_file):
            arg = [
                "python", "main.py", "--extract_one_file",
                "--source", source_file,
                "--target", target_file,
            ]
            logger.info(f"{pi + 1:,}/{pmids:,} [{pmid}] arg={arg}")
            process = subprocess.Popen(arg)
            time.sleep(1)
            for _ in range(patience):
                if process.poll() is not None:
                    break
                time.sleep(1)
            else:
                process.terminate()

        if os.path.exists(target_file):
            line_list = read_lines(target_file)
            pmid_to_text[pmid] = line_list

    pmid_to_text_file = os.path.join(target_dir, "pmid_to_text.json")
    write_json(pmid_to_text_file, pmid_to_text)
    return


def extract_one_file(source, target):
    line_list, _ = pdf_utils.get_pdf_objects(source, False)
    write_lines(target, line_list)
    return


def tmp():
    data_dir = os.path.join("/", "volume", "pubmedkb-covid", "chester", "v2lwork")
    pmid_to_text_old_file = os.path.join(data_dir, "work", "pmid_to_text_old.json")
    pmid_to_text_file = os.path.join(data_dir, "work", "pmid_to_text.json")
    pmid_list_file = os.path.join(data_dir, "work", "pmid_list.txt")

    pmid_to_text = read_json(pmid_to_text_old_file)
    divide = 27

    for i in range(divide):
        mod_pmid_to_text_file = os.path.join(data_dir, "mod", f"{divide}_{i}", "pmid_to_text.json")
        mod_pmid_to_text = read_json(mod_pmid_to_text_file)
        for pmid, text in mod_pmid_to_text.items():
            pmid_to_text[pmid] = text

    pmid_list = sorted(pmid_to_text.keys(), key=lambda x: int(x))
    write_lines(pmid_list_file, pmid_list)
    pmid_to_text = {
        pmid: pmid_to_text[pmid]
        for pmid in pmid_list
    }
    write_json(pmid_to_text_file, pmid_to_text)
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--end", type=int, default=253390)
    parser.add_argument("--divide", type=int, default=20)
    parser.add_argument("--remain", type=int, default=0)

    parser.add_argument("--extract_one_file", action="store_true")
    parser.add_argument("--source", type=str)
    parser.add_argument("--target", type=str)

    arg = parser.parse_args()

    # if arg.extract_one_file:
    #     extract_one_file(arg.source, arg.target)
    #     return

    data_dir = os.path.join("/", "volume", "pubmedkb-covid", "chester")

    plant_pdf_list_file = os.path.join(data_dir, "chester", "download_paper_list.txt")
    plant_pdf_dir = os.path.join(data_dir, "chester", "pdf")

    plant_text_dir = os.path.join(data_dir, "v2lwork", "text")
    plant_pmid_to_text_file = os.path.join(data_dir, "v2lwork", "pmid_to_text.json")

    plant_pmid_skip_file = os.path.join(data_dir, "v2lwork", "pmid_to_skip.txt")
    plant_mod_dir = os.path.join(data_dir, "v2lwork", "mod")

    # extract_text_from_pdf(plant_pdf_list_file, plant_pdf_dir, plant_text_dir, arg.start, arg.end)
    # collect_pmid_to_text(plant_text_dir, plant_pmid_to_text_file)
    # extract_text_from_pdf_by_subprocess(
    #     plant_pdf_list_file, plant_pdf_dir,
    #     plant_pmid_skip_file, plant_mod_dir,
    #     arg.divide, arg.remain,
    # )

    tmp()
    return


if __name__ == "__main__":
    main()
    sys.exit()
