import os
import csv
import sys
import json
import random
import logging
import argparse

from nltk.tokenize.destructive import NLTKWordTokenizer

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


def read_jsonl(file, dtype, write_log=True):
    if write_log:
        logger.info(f"Reading {file}")

    if dtype == "list":
        data = []
        with open(file, "r", encoding="utf8") as f:
            for line in f:
                datum = json.loads(line)
                data.append(datum)

    elif dtype == "dict":
        data = {}
        with open(file, "r", encoding="utf8") as f:
            for line in f:
                key, value = json.loads(line)
                data[key] = value

    else:
        assert False

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


def extract_sentence_data(pmid_to_text_file, sentence_dir, batch_size=500000):
    os.makedirs(sentence_dir, exist_ok=True)
    
    if pmid_to_text_file.endswith(".json"):
        pmid_to_text = read_json(pmid_to_text_file)
    elif pmid_to_text_file.endswith(".jsonl"):
        pmid_to_text = read_jsonl(pmid_to_text_file, "dict")
    else:
        assert False
    pmid_list = list(pmid_to_text.keys())
    random.shuffle(pmid_list)

    tokenizer = NLTKWordTokenizer()
    total_pmids = len(pmid_list)
    pmids = 0
    sentences = 0
    tokens = 0

    batch = []
    bi = 1

    for pmid in pmid_list:
        for si, sentence in enumerate(pmid_to_text[pmid]):
            try:
                span_sequence = list(tokenizer.span_tokenize(sentence))
                token_sequence = [sentence[i:j] for i, j in span_sequence]
            except ValueError:
                span_sequence = None
                token_sequence = tokenizer.tokenize(sentence)
            batch.append({
                "pmid": pmid,
                "sent_id": si,
                "sentence": sentence,
                "span_list": span_sequence,
                "token_list": token_sequence,
            })
            tokens += len(token_sequence)
            sentences += 1
        pmids += 1
        batch_sentences = len(batch)

        if batch_sentences >= batch_size:
            batch_file = os.path.join(sentence_dir, f"batch_{bi}.json")
            write_json(batch_file, batch, write_log=False)
            logger.info(
                f"batch 1-{bi:,} cumulates:"
                f" {pmids:,}/{total_pmids:,} pmids;"
                f" {sentences:,} sentences;"
                f" {tokens:,} tokens"
            )
            batch = []
            bi += 1
    if batch:
        batch_file = os.path.join(sentence_dir, f"batch_{bi}.json")
        write_json(batch_file, batch, write_log=False)
        logger.info(
            f"batch 1-{bi:,} cumulates:"
            f" {pmids:,}/{total_pmids:,} pmids;"
            f" {sentences:,} sentences;"
            f" {tokens:,} tokens"
        )
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int)
    parser.add_argument("--end", type=int)
    arg = parser.parse_args()
    for key, value in vars(arg).items():
        if value is not None:
            logger.info(f"[arg.{key}] {value}")

    # raw_dir = os.path.join("/", "volume", "pubmedkb-covid", "chester", "v2lwork", "work")
    # pmid_to_text_file = os.path.join(raw_dir, "pmid_to_text.json")
    # data_dir = os.path.join("/", "volume", "penghsuanli-genome2-nas2", "plant")
    # sentence_dir = os.path.join(data_dir, "core_data", "source")

    data_dir = os.path.join("/", "volume", "penghsuanli-genome2-nas2", "plant", "dataset_20230926")
    pmid_to_text_file = os.path.join(data_dir, "pmid_text.jsonl")
    sentence_dir = os.path.join(data_dir, "sentence")

    extract_sentence_data(pmid_to_text_file, sentence_dir)
    return


if __name__ == "__main__":
    main()
    sys.exit()
