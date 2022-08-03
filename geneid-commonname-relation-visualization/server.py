import csv
import sys
import json
import logging
import argparse
from collections import defaultdict

from flask import Flask, render_template, request
app = Flask(__name__)
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


def get_global_data():
    geneid_data_file = "geneid_commonname.csv"
    relation_data_file = "commonname_relation.csv"

    geneid_data = read_csv(geneid_data_file, "csv")
    relation_data = read_csv(relation_data_file, "csv")

    geneid_header, geneid_data = geneid_data[0], geneid_data[1:]
    relation_header, relation_data = relation_data[0], relation_data[1:]
    assert geneid_header == [
        "plant", "GeneID", "CommonName",
        "alias_GeneIDs_in_the_sentence", "pmid", "sentence",
    ]
    assert relation_header == [
        "head", "relation", "tail",
        "head_entity", "head_type",
        "tail_entity", "tail_type",
        "simple", "pmid", "sentence",
    ]

    plant_to_geneid = defaultdict(lambda: set())
    geneid_commonname_pmid = defaultdict(lambda: defaultdict(lambda: set()))
    commonname_to_relation = defaultdict(lambda: set())

    for plant, geneid, commonname, _alias_geneids_in_the_sentence, pmid, _sentence in geneid_data:
        plant_to_geneid[plant].add(geneid)
        geneid_commonname_pmid[geneid][commonname].add(pmid)

    for relation_datum in relation_data:
        relation_datum = tuple(relation_datum)
        (
            head, relation, tail,
            head_entity, head_type,
            tail_entity, tail_type,
            simple, pmid, sentence,
        ) = relation_datum
        if head_type == "CommonName":
            commonname_to_relation[head_entity].add(relation_datum)
        if tail_type == "CommonName":
            commonname_to_relation[tail_entity].add(relation_datum)

    plant_to_geneid = {
        plant: sorted(geneid_set)
        for plant, geneid_set in plant_to_geneid.items()
    }
    return plant_to_geneid, geneid_commonname_pmid, commonname_to_relation


plant_to_geneid, geneid_commonname_pmid, commonname_to_relation = get_global_data()


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/run_load_gene_list", methods=["POST"])
def run_load_gene_list():
    data = json.loads(request.data)
    plant = data["plant"]
    geneid_list = plant_to_geneid[plant]
    response = {
        "geneid_list": geneid_list
    }
    return json.dumps(response)


@app.route("/run_generate_graph", methods=["POST"])
def run_generate_graph():
    data = json.loads(request.data)

    type_to_color = {
        "GeneID": "#d5abff",  # 270°, 33%, 100% violet
        "CommonName": "#abffff",  # 180°, 33%, 100% cyan
        "Compound": "#d5ffab",  # 90°, 33%, 100% yellow-green
        "Species": "#ffffab",  # 60°, 33%, 100% yellow
        "Location": "#ffd5ab",  # 30°, 33%, 100% orange
        "Process": "#ffabab",  # 0°, 33%, 100% red
    }

    node_list = [
        {"id": -1, "label": "GeneID", "color": type_to_color["GeneID"]},
        {"id": -2, "label": "CommonName", "color": type_to_color["CommonName"]},
        {"id": -3, "label": "CommonName", "color": type_to_color["CommonName"]},
        {"id": -4, "label": "Compound", "color": type_to_color["Compound"]},
        {"id": -5, "label": "Species", "color": type_to_color["Species"]},
        {"id": -6, "label": "Location", "color": type_to_color["Location"]},
        {"id": -7, "label": "Process", "color": type_to_color["Process"]},
    ]
    edge_list = [
        {"from": -1, "to": -2},
        {"from": -2, "to": -3},
        {"from": -2, "to": -4},
        {"from": -2, "to": -5},
        {"from": -2, "to": -6},
        {"from": -2, "to": -7},

    ]
    name_to_nid = {}
    pair_to_label = defaultdict(lambda: [])
    pair_to_width = defaultdict(lambda: 0)

    # GeneID
    geneid = data["geneid"]
    name_to_nid[geneid] = 0
    node_list.append({"id": 0, "label": geneid, "color": type_to_color["GeneID"]})
    edge_list.append({"from": 0, "to": -1})

    # CommonName
    commonname_to_pmid_set = geneid_commonname_pmid.get(geneid, {})

    for commonname, pmid_set in commonname_to_pmid_set.items():
        nid = name_to_nid.get(commonname, None)
        if nid is None:
            nid = len(node_list)
            name_to_nid[commonname] = nid
            node_list.append({"id": nid, "label": commonname, "color": type_to_color["CommonName"]})
        for pmid in pmid_set:
            # edge_list.append({"from": 0, "to": nid, "label": f"PMID{pmid}"})
            # edge_list.append({"to": 0, "from": nid})
            pair_to_width[(0, nid)] += 1

    # Entity relations
    for commonname in commonname_to_pmid_set:
        for relation_datum in commonname_to_relation.get(commonname, []):
            (
                head, relation, tail,
                head_entity, head_type,
                tail_entity, tail_type,
                simple, pmid, sentence,
            ) = relation_datum

            # head node
            head_nid = name_to_nid.get(head_entity, None)
            if head_nid is None:
                head_nid = len(node_list)
                name_to_nid[head_entity] = head_nid
                node_list.append({"id": head_nid, "label": head_entity, "color": type_to_color[head_type]})

            # tail node
            tail_nid = name_to_nid.get(tail_entity, None)
            if tail_nid is None:
                tail_nid = len(node_list)
                name_to_nid[tail_entity] = tail_nid
                node_list.append({"id": tail_nid, "label": tail_entity, "color": type_to_color[tail_type]})

            # CommonName -> Entity edge
            if head_type == "CommonName":
                if tail_type == "CommonName":
                    from_nid, to_nid = sorted((head_nid, tail_nid))
                else:
                    from_nid, to_nid = head_nid, tail_nid
            else:
                from_nid, to_nid = tail_nid, head_nid

            if simple == "T":
                # edge_list.append({"from": head_nid, "to": tail_nid, "label": f"PMID{pmid}: {relation}"})
                # edge_list.append({"from": head_nid, "to": tail_nid, "label": relation})
                pair_to_label[(from_nid, to_nid)].append(relation)
                pair_to_width[(from_nid, to_nid)] += 1
            else:
                # edge_list.append({"from": head_nid, "to": tail_nid, "label": f"PMID{pmid}"})
                # edge_list.append({"from": head_nid, "to": tail_nid})
                pair_to_width[(from_nid, to_nid)] += 1

    for pair, width in pair_to_width.items():
        from_nid, to_nid = pair
        label = "\n".join(pair_to_label[pair])
        edge_list.append({"from": from_nid, "to": to_nid, "width": width, "label": label})

    response = {
        "node_list": node_list,
        "edge_list": edge_list,
    }
    return json.dumps(response)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", default="0.0.0.0")
    parser.add_argument("-port", default="12345")
    arg = parser.parse_args()

    app.run(host=arg.host, port=arg.port)
    return


if __name__ == "__main__":
    main()
    sys.exit()
