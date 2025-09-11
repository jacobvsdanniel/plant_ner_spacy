import os
import csv
import sys
import json
import logging
import argparse
from collections import defaultdict

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


"""
NER
"""


def collect_ner_data(sentence_ner_dir, ner_file, start, end):
    data = []
    type_to_count = defaultdict(lambda: 0)
    mentions = 0
    sentences = 0
    ner_sentences = 0

    for bi in range(start, end + 1):
        batch_file = os.path.join(sentence_ner_dir, f"batch_{bi}", "target.json")
        batch_data = read_json(batch_file, write_log=False)
        for datum in batch_data:
            sentences += 1

            mention_list = datum["mention_list"]
            if not mention_list:
                continue
            ner_sentences += 1
            mentions += len(mention_list)
            for mention in mention_list:
                type_to_count[mention["type"]] += 1

            data.append(datum)

        logger.info(
            f"batch {bi}/[{start},{end}] cumulated:"
            f" {ner_sentences:,}/{sentences:,} sentences;"
            f" {mentions:,} mentions;"
            f" {dict(type_to_count)}"
        )

    write_json(ner_file, data)
    return


def split_batch(single_file, batch_dir, batch_size):
    data = read_json(single_file)

    pmid_set = set()
    batch = []
    bi = 1

    for datum in data:
        pmid = datum["pmid"]

        if pmid not in pmid_set and len(batch) >= batch_size:
            batch_file = os.path.join(batch_dir, f"batch_{bi}.json")
            write_json(batch_file, batch)
            pmid_set = set()
            batch = []
            bi += 1

        pmid_set.add(pmid)
        batch.append(datum)

    batch_file = os.path.join(batch_dir, f"batch_{bi}.json")
    write_json(batch_file, batch)
    return


"""
GeneID
"""


def tag_gene_id_by_sentence(sentence, length_name_type_id):
    mention_list = []

    for length, name_type_id in length_name_type_id.items():

        for ci in range(len(sentence) - length + 1):
            name = sentence[ci:ci+length]
            type_to_id = name_type_id.get(name.lower(), {})

            for _type, id_set in type_to_id.items():
                assert id_set
                mention = {
                    "name": name,
                    "real_pos": (ci, ci + len(name)),
                    "type": _type,
                    "id": sorted(id_set),
                }
                mention_list.append(mention)

    return mention_list


def tag_gene_id_for_directory(gene_id_dir, source_dir, target_dir, start, end):
    gene_id_type_list = [
        "Arabidopsis thaliana",
        "Populus trichocarpa",
    ]

    # Read gene ids and aliases
    length_name_type_id = {}

    for _type in gene_id_type_list:
        escape_type = _type.replace(" ", "_")
        gene_id_file = os.path.join(gene_id_dir, f"{escape_type}.csv")
        gene_id_data = read_csv(gene_id_file, "csv")
        ids = 0
        names = 0

        for row in gene_id_data:
            name_list = []
            for name in row:
                name = name.strip().lower()
                if name:
                    name_list.append(name)
            if not name_list:
                continue
            ids += 1
            _id = name_list[0]

            for name in name_list:
                names += 1
                length = len(name)
                if length not in length_name_type_id:
                    length_name_type_id[length] = defaultdict(lambda: defaultdict(lambda: set()))
                length_name_type_id[length][name][_type].add(_id)

        lengths = len(length_name_type_id.keys())
        logger.info(f"{ids:,} ids; {names:,} names; {lengths:,} lengths")

    # Add exact match tags to data
    sentences = 0
    id_set = set()
    mentions = 0
    si = 0

    for bi in range(start, end + 1):
        source_file = os.path.join(source_dir, f"batch_{bi}.json")
        target_file = os.path.join(target_dir, f"batch_{bi}.json")

        data = read_json(source_file)
        sentences += len(data)

        for di, datum in enumerate(data):
            sentence = datum["sentence"]
            mention_list = tag_gene_id_by_sentence(sentence, length_name_type_id)
            for mention in mention_list:
                for _id in mention["id"]:
                    id_set.add(_id)
            mentions += len(mention_list)
            datum["gene_id_mention_list"] = mention_list

            di += 1
            si += 1
            if di % 10000 == 0 or di == len(data):
                ids = len(id_set)
                logger.info(
                    f"batch [{start:,}-{bi:,}]/[{start:,}-{end:,}]"
                    f" sentence {si:,}/{sentences:,}:"
                    f" {ids:,} unique ids;"
                    f" {mentions:,} mentions"
                )

        write_json(target_file, data)
    return


"""
misc
"""


def extract_result(source_dir, target_dir, start, end):
    os.makedirs(target_dir, exist_ok=True)

    gene_gene_file = os.path.join(target_dir, "geneid_commonname.jsonl")
    gene_other_file = os.path.join(target_dir, "geneid_location_compound_process.jsonl")
    spacy_file = os.path.join(target_dir, "spacy_triplet.jsonl")
    ner_file = os.path.join(target_dir, "ner.jsonl")

    f_gene_gene = open(gene_gene_file, "w", encoding="utf8")
    f_gene_other = open(gene_other_file, "w", encoding="utf8")
    f_spacy = open(spacy_file, "w", encoding="utf8")
    f_ner = open(ner_file, "w", encoding="utf8")

    ggs = 0
    gos = 0
    spacys = 0
    ners = 0

    for bi in range(start, end + 1):
        source_file = os.path.join(source_dir, f"batch_{bi}.json")
        data = read_json(source_file)

        for datum in data:
            pmid = datum["pmid"]
            sentence = datum["sentence"]
            mention_list = datum["mention_list"]
            gene_id_mention_list = datum["gene_id_mention_list"]
            triplet_list = datum["triplet_list"]

            # clean datum
            mention_list = [
                {
                    "name": mention["name"],
                    "type": mention["type"],
                }
                for mention in mention_list
            ]
            gene_id_mention_list = [
                {
                    "name": mention["name"],
                    "type": mention["type"],
                    "id": mention["id"],
                }
                for mention in gene_id_mention_list
            ]
            triplet_list = [
                {
                    "head_ner_mention_index": triplet["h_mention"],
                    "tail_ner_mention_index": triplet["t_mention"],
                    "triplet": triplet["triplet"],
                }
                for triplet in triplet_list
            ]
            datum_string = json.dumps({
                "pmid": pmid,
                "sentence": sentence,
                "ner_mention_list": mention_list,
                "gene_id_mention_list": gene_id_mention_list,
                "triplet_list": triplet_list,
            }) + "\n"

            # ner data
            f_ner.write(datum_string)
            ners += 1

            # geneid to commonname data
            geneidtuple_set = {
                tuple(mention["id"])
                for mention in gene_id_mention_list
            }
            commonname_set = {
                mention["name"]
                for mention in mention_list
                if mention["type"] == "CommonName"
            }
            geneidtuples = len(geneidtuple_set)
            commonnames = len(commonname_set)
            if 1 <= geneidtuples <= 2 and 1 <= commonnames <= 2:
                f_gene_gene.write(datum_string)
                ggs += 1

            # geneid to location/compound/process data
            if 1 <= geneidtuples <= 2:
                has_other_entity = False
                for mention in mention_list:
                    if mention["type"] in ["Location", "Compound", "Process"]:
                        has_other_entity = True
                        break
                if has_other_entity:
                    f_gene_other.write(datum_string)
                    gos += 1

            # spacy_ore data
            if triplet_list:
                f_spacy.write(datum_string)
                spacys += 1

        logger.info(
            f"batch {start}-{bi} cumulates:"
            f" {ggs:,} GeneID-CommonName sentences;"
            f" {gos:,} GeneID-Location/Compound/Process sentences;"
            f" {spacys:,} SpacyORE sentences;"
            f" {ners:,} NER sentences"
        )

    f_gene_gene.close()
    f_gene_other.close()
    f_spacy.close()
    f_ner.close()
    return


def extract_website_data(geneid_relation_dir, website_dir, start, end):
    plant_geneidlist_commonname_to_pmid_sentence_aliaslist = defaultdict(lambda: [])
    commonname_to_relation_list = defaultdict(lambda: [])
    type_to_set = defaultdict(lambda: set())
    triplets = 0

    for bi in range(start, end + 1):
        data_file = os.path.join(geneid_relation_dir, f"batch_{bi}.json")
        data = read_json(data_file, write_log=False)

        for datum in data:
            pmid = datum["pmid"]
            sentence = datum["sentence"]
            mention_list = datum["mention_list"]
            triplet_list = datum["triplet_list"]
            gene_mention_list = datum["gene_id_mention_list"]

            # GeneIDs in the sentence
            plant_geneidlist_to_alias_set = defaultdict(lambda: set())
            for gene_mention in gene_mention_list:
                geneidlist = tuple(gene_mention["id"])
                alias = gene_mention["name"]
                plant = gene_mention["type"]
                plant_geneidlist_to_alias_set[(plant, geneidlist)].add(alias)

            # CommonNames in the sentence
            commonname_set = set()
            for mention in mention_list:
                if mention["type"] == "CommonName":
                    commonname = mention["name"]
                    commonname_set.add(commonname)

            # (plant, GeneID, CommonName) -> (pmid, sentence, GeneIDs in the sentence, Alias GeneIDs in sentence) list
            g_matches = len(plant_geneidlist_to_alias_set)
            c_matches = len(commonname_set)
            if (g_matches, c_matches) in [(1, 1), (1, 2), (2, 1), (2, 2)]:
                type_to_set["pmid"].add(pmid)

                for (plant, geneid_list), alias_set in plant_geneidlist_to_alias_set.items():
                    type_to_set["plant"].add(plant)
                    alias_list = sorted(alias_set)

                    for geneid in geneid_list:
                        type_to_set["geneid"].add(geneid)

                    for commonname in commonname_set:
                        type_to_set["commonname"].add(commonname)

                        plant_geneidlist_commonname_to_pmid_sentence_aliaslist[
                            (plant, geneid_list, commonname)
                        ].append(
                            (pmid, sentence, alias_list)
                        )

            # relation data among (CommonName, Species, Location, Compound, Process)
            for triplet_datum in triplet_list:
                head, relation, tail = triplet_datum["triplet"]
                hi = triplet_datum["h_mention"]
                ti = triplet_datum["t_mention"]
                h_mention = mention_list[hi]
                t_mention = mention_list[ti]
                h_name = h_mention["name"]
                t_name = t_mention["name"]
                h_type = h_mention["type"]
                t_type = t_mention["type"]
                triplets += 1

                relation_datum = [
                    head, relation, tail,
                    h_name, h_type,
                    t_name, t_type,
                    pmid, sentence,
                ]

                # only collect CommonName relations
                if h_type == "CommonName":
                    commonname_to_relation_list[h_name].append(relation_datum)
                if t_type == "CommonName" and (h_type, h_name) != (t_type, t_name):
                    commonname_to_relation_list[t_name].append(relation_datum)

        logger.info(
            f"batch [{start:,}-{bi:,}]/[{start:,}-{end:,}]"
            f" {len(type_to_set['plant']):,} plants;"
            f" {len(type_to_set['geneid']):,} GeneIDs;"
            f" {len(type_to_set['commonname']):,} CommonNames;"
            f" {len(type_to_set['pmid']):,} pmids;"
            f" {triplets:,} triplets"
        )

    # format gene id data csv
    geneid_header = ["plant", "GeneID", "CommonName", "alias_GeneIDs_in_the_sentence", "pmid", "sentence"]
    geneid_data = [geneid_header]
    tag_list = sorted(plant_geneidlist_commonname_to_pmid_sentence_aliaslist.keys())

    for tag in tag_list:
        plant, geneid_list, commonname = tag
        evidence_list = plant_geneidlist_commonname_to_pmid_sentence_aliaslist[tag]
        evidence_list = sorted(evidence_list, key=lambda x: int(x[0]))

        for pmid, sentence, alias_list in evidence_list:
            geneid_string = ", ".join(geneid_list)
            alias_string = ", ".join(alias_list)
            geneid_data.append([plant, geneid_string, commonname, alias_string, pmid, sentence])

    # format relation data csv
    relation_header = [
        "head", "relation", "tail",
        "head_entity", "head_type",
        "tail_entity", "tail_type",
        "pmid", "sentence",
    ]
    relation_data = [relation_header]
    commonname_list = sorted(commonname_to_relation_list.keys())
    statistics_type_to_set = defaultdict(lambda: set())

    for commonname in commonname_list:
        # only collect relations for GeneID-CommonName-Entity
        if commonname not in type_to_set["commonname"]:
            continue
        for relation in commonname_to_relation_list[commonname]:
            relation_data.append(relation)

            # statistics
            (
                head, relation, tail,
                h_name, h_type,
                t_name, t_type,
                pmid, sentence,
            ) = relation

            statistics_type_to_set["triplet"].add((head, relation, tail))
            statistics_type_to_set[h_type].add(h_name)
            statistics_type_to_set[t_type].add(t_name)

    type_count_list = [
        (_type, len(_set))
        for _type, _set in statistics_type_to_set.items()
        if "triplet" not in _type
    ]
    triplets = len(statistics_type_to_set["triplet"])
    type_count_list = sorted(type_count_list, key=lambda tc: tc[1], reverse=True)
    logger.info(f"In the GeneID-CommonName-Entity graph:")
    logger.info(f" {triplets:,} unique triplets")
    for _type, count in type_count_list:
        logger.info(f" {count:,} unique {_type}s have relations")

    geneid_data_file = os.path.join(website_dir, "geneid_commonname.csv")
    write_csv(geneid_data_file, "csv", geneid_data)

    relation_data_file = os.path.join(website_dir, "commonname_relation.csv")
    write_csv(relation_data_file, "csv", relation_data)
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int)
    parser.add_argument("--end", type=int)
    arg = parser.parse_args()
    for key, value in vars(arg).items():
        if value is not None:
            logger.info(f"[arg.{key}] {value}")

    data_dir = os.path.join("/", "volume", "penghsuanli-genome2-nas2", "plant", "dataset_20230926")

    sentence_ner_dir = os.path.join(data_dir, "sentence_ner")
    ner_file = os.path.join(data_dir, "ner.json")
    ner_dir = os.path.join(data_dir, "ner")
    gene_id_dir = os.path.join(data_dir, "gene_id")
    ner_geneid_dir = os.path.join(data_dir, "ner_geneid")
    ner_geneid_spacy_dir = os.path.join(data_dir, "ner_geneid_spacy")

    # collect_ner_data(sentence_ner_dir, ner_file, arg.start, arg.end)
    # split_batch(ner_file, ner_dir, 313607)
    # tag_gene_id_for_directory(gene_id_dir, ner_dir, ner_geneid_dir, arg.start, arg.end)

    result_dir = os.path.join(data_dir, "result")
    # extract_result(ner_geneid_spacy_dir, result_dir, arg.start, arg.end)
    extract_website_data(ner_geneid_spacy_dir, result_dir, arg.start, arg.end)
    return


if __name__ == "__main__":
    main()
    sys.exit()
