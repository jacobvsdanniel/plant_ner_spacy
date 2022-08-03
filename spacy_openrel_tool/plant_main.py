import re
import csv
import sys
import json
import logging
import argparse
from collections import defaultdict

import spacy
from nltk.tokenize.destructive import NLTKWordTokenizer
from nltk.tokenize.treebank import TreebankWordDetokenizer

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
    level=logging.INFO,
)
csv.field_size_limit(sys.maxsize)
csv.register_dialect(
    "csv", delimiter=",", quoting=csv.QUOTE_MINIMAL, quotechar='"', doublequote=True,
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


def get_masked_sentence_list(data, start, end, max_tokens=128, max_token_length=500):
    sentence_list = []
    di_list = []
    tokenizer = NLTKWordTokenizer()
    detokenizer = TreebankWordDetokenizer()

    for di in range(start, end):
        datum = data[di]
        mention_list = datum["mention_list"]
        if len(mention_list) < 2:
            continue

        if "token_list" in datum:
            masked_token_list = [token for token in datum["token_list"]]
        else:
            sentence = datum["sentence"]
            span_sequence = tokenizer.span_tokenize(sentence)
            masked_token_list = [sentence[l:r] for l, r in span_sequence]

        for mi, mention in enumerate(datum["mention_list"]):
            ti, tj = mention["pos"]
            masked_token_list[ti] = f"ENTITY{mi}"
            for i in range(ti + 1, tj):
                masked_token_list[i] = None

        masked_token_list = [
            token[:max_token_length]
            for token in masked_token_list[:max_tokens]
            if token
        ]
        masked_sentence = detokenizer.detokenize(masked_token_list)
        sentence_list.append(masked_sentence)
        di_list.append(di)

    return sentence_list, di_list


def get_hyphen_merged_sentence_list(sentence_list, word_set):

    def get_non_hyphen_word(match):
        try:
            match.group(3)
            terms = 3
        except IndexError:
            terms = 2

        raw_text = match.group(0)
        term_list = [match.group(i) for i in range(1, terms + 1)]

        # do nothing if all terms are words
        for term in term_list:
            if term not in word_set:
                break
        else:
            return raw_text

        word = "".join(term_list)

        if len(word) >= 5 and word in word_set:
            return word
        else:
            return raw_text

    hyphen_pattern_list = [
        re.compile(r"([^\W\d_]+)-([^\W\d_]+)"),  # in-fluence
        re.compile(r"([^\W\d_]+)\s-\s([^\W\d_]+)"),  # in - fluence
        re.compile(r"([^\W\d_]+)\s([^\W\d_]+)"),  # in fluence
        re.compile(r"([^\W\d_]+)\s([^\W\d_]+)\s([^\W\d_]+)"),  # in flu ence
    ]

    new_sentence_list = []
    for sentence in sentence_list:
        for hyphen_pattern in hyphen_pattern_list:
            new_sentence = hyphen_pattern.sub(get_non_hyphen_word, sentence)
            if new_sentence != sentence:
                break
        new_sentence_list.append(new_sentence)
    return new_sentence_list


def get_merged_hypenated_verb_sentence_list(sentence_list, verb_set):
    def get_merged_hyphenated_verb(match):
        term = match.group(1) + match.group(2)
        if term in verb_set:
            return term
        return match.group(0)

    hyphenated_verb_pattern = re.compile(r"([^\s]+)\s*-\s*([^\s]+)")

    sentence_list = [
        hyphenated_verb_pattern.sub(get_merged_hyphenated_verb, sentence)
        for sentence in sentence_list
    ]
    return sentence_list


def get_ie_preprocessed_sentence_list(sentence_list):
    ie_pattern = re.compile(r"i\s*[.]\s*e\s*[.]\s*,")
    sentence_list = [
        ie_pattern.sub("i.e.", sentence)
        for sentence in sentence_list
    ]
    return sentence_list


def add_spacy_data(data, start, end, word_set, version):
    logger.info(f"spacy_ore_tool: parse [{start:,}, {end:,}]")

    # mask entity mentions
    sentence_list, di_list = get_masked_sentence_list(data, start, end)

    # merge hyphenated verbs
    if version in ["hyphen", "20220430"]:
        sentence_list = get_hyphen_merged_sentence_list(sentence_list, word_set)
    else:
        sentence_list = get_merged_hypenated_verb_sentence_list(sentence_list, word_set)

    if version in ["ie", "20220430"]:
        sentence_list = get_ie_preprocessed_sentence_list(sentence_list)

    nlp = spacy.load(
        "en_core_web_sm",
        exclude=["lemmatizer", "ner"],
    )

    # make sure VARIANT\d+ DISEASE\d+ GENE\d+ will be PROPN
    ruler = nlp.get_pipe("attribute_ruler")
    patterns = [[{
        "TEXT": {
            "REGEX": r"(VARIANT\d+)|(DISEASE\d+)|(GENE\d+)|(MUTATION\d+)|(DRUG\d+)|(SPECIES\d+)"
                     r"|(PATHWAY\d+)|(MIRNA\d+)"
                     r"|(ENTITY\d+)"
        }
    }]]
    attrs = {"TAG": "NNP", "POS": "PROPN"}
    ruler.add(patterns=patterns, attrs=attrs, index=0)

    # add spacy annotation to data
    offset = 0
    for doc in nlp.pipe(sentence_list):
        di = di_list[offset]
        data[di]["doc"] = doc
        offset += 1
    return


def match_mention(h, r, t, mention_list, mention_expression):
    h_match_list = mention_expression.findall(h)
    r_match_list = mention_expression.findall(r)
    t_match_list = mention_expression.findall(t)

    # Must be exactly one ENTITY in head, zero in relation, one in tail
    if len(h_match_list) != 1 or len(r_match_list) != 0 or len(t_match_list) != 1:
        return None
    h_match = h_match_list[0]
    t_match = t_match_list[0]
    # head entity must not equal tail entity
    if h_match == t_match:
        return None

    perfect_match = h_match == h and t_match == t
    h_mi = int(h_match[6:])
    t_mi = int(t_match[6:])

    # undo ENTITY masking
    h_name = mention_list[h_mi]["name"]
    t_name = mention_list[t_mi]["name"]
    h = mention_expression.sub(lambda _: h_name, h)
    t = mention_expression.sub(lambda _: t_name, t)

    return h_mi, t_mi, h, t, perfect_match


def get_conjunctive_and_appositive_root(token):
    while True:
        if token.dep_ in ["conj", "appos"]:
            token = token.head
        else:
            break
    return token


def get_chunk_text(chunk):
    for ti, token in enumerate(chunk):
        if token.text not in [",", "(", "i.e."]:
            break
    else:
        return ""
    return chunk[ti:].text


def get_negation(token):
    neg_list = []
    for child in token.lefts:
        if child.dep_ == "neg":
            neg = str(child)
            if neg == "n't":
                neg = "not"
            neg_list.append(neg)
    neg_prefix = " ".join(neg_list)
    return neg_prefix


def add_relation_data(data, start, end, version):
    logger.info("spacy_ore_tool: add_relation_data()")

    mention_expression = re.compile(r"ENTITY\d+")
    sentences_with_triplets = 0
    triplets = 0
    perfect_triplets = 0

    for di in range(start, end):
        datum = data[di]
        if "doc" not in datum:
            datum["triplet_list"] = []
            continue
        doc = datum["doc"]
        del datum["doc"]
        possible_relation = defaultdict(lambda: defaultdict(lambda: []))

        root_to_noun_chunk = {
            chunk.root: chunk
            for chunk in doc.noun_chunks
        }

        for chunk in doc.noun_chunks:
            if "ENTITY" not in chunk.text:
                continue
            root = get_conjunctive_and_appositive_root(chunk.root)
            if root.head == root:
                continue
            possible_relation[root.head][root.dep_].append({"chunk": chunk})

            # consider "head_chunk of chunk" as noun chunk
            if version not in ["chunk_of_chunk", "20220430"]:
                continue

            if chunk.root.dep_ != "pobj":  # chunk -> of
                continue
            head = chunk.root.head
            if head.text != "of" or head.pos_ != "ADP" or head.dep_ != "prep":  # of -> head_chunk
                continue
            head = head.head
            if head not in root_to_noun_chunk:  # head_chunk
                continue
            head_chunk = root_to_noun_chunk[head]  # head_chunk
            full_chunk = doc[head_chunk.start:chunk.end]  # head_chunk of chunk

            root = get_conjunctive_and_appositive_root(head_chunk.root)
            if root.head == root:
                continue
            possible_relation[root.head][root.dep_].append({"chunk": full_chunk})

        mention_list = datum["mention_list"]
        triplet_list = []

        for relation in possible_relation:

            if "nsubj" in possible_relation[relation] and "dobj" in possible_relation[relation]:
                for subj in possible_relation[relation]["nsubj"]:
                    for obj in possible_relation[relation]["dobj"]:
                        h = get_chunk_text(subj["chunk"])
                        r = str(relation)
                        t = get_chunk_text(obj["chunk"])
                        n = get_negation(relation)
                        if n:
                            r = f"{n} {r}"

                        match = match_mention(h, r, t, mention_list, mention_expression)
                        if match is None:
                            continue
                        h_mi, t_mi, h, t, perfect_match = match
                        triplet_list.append({
                            "h_mention": h_mi,
                            "t_mention": t_mi,
                            "triplet": (h, r, t),
                            "perfect_match": perfect_match
                        })

            if "pobj" in possible_relation[relation]:
                if relation.head in possible_relation:
                    if "nsubjpass" in possible_relation[relation.head]:
                        for subj in possible_relation[relation.head]["nsubjpass"]:
                            for obj in possible_relation[relation]["pobj"]:
                                h = get_chunk_text(subj["chunk"])
                                r = f"{relation.head} {relation}"
                                t = get_chunk_text(obj["chunk"])
                                n = get_negation(relation.head)
                                if n:
                                    r = f"{n} {r}"

                                match = match_mention(h, r, t, mention_list, mention_expression)
                                if match is None:
                                    continue
                                h_mi, t_mi, h, t, perfect_match = match
                                triplet_list.append({
                                    "h_mention": h_mi,
                                    "t_mention": t_mi,
                                    "triplet": (h, r, t),
                                    "perfect_match": perfect_match
                                })

        datum["triplet_list"] = triplet_list
        if triplet_list:
            sentences_with_triplets += 1
            triplets += len(triplet_list)
            for triplet in triplet_list:
                if triplet["perfect_match"]:
                    perfect_triplets += 1

    return sentences_with_triplets, triplets, perfect_triplets


def run_spacy_relation_extraction(arg):
    data = read_json(arg.source_file)
    sentences = len(data)

    if not arg.use_cpu:
        spacy.require_gpu()

    sentences_with_triplets = 0
    triplets = 0
    perfect_triplets = 0

    if arg.version in ["hyphen", "20220430"]:
        vocab_46k_list = read_csv(arg.vocab_46k_file, "csv")
        vocab_367k_list = read_lines(arg.vocab_367k_file)
        word_set = set(word for word, _count in vocab_46k_list) | set(vocab_367k_list)
    else:
        verb_data = read_csv(arg.verb_file, "csv")
        word_set = set(verb for verb, _count in verb_data[1:])

    for start in range(0, sentences, arg.batch_size):
        end = min(start + arg.batch_size, sentences)
        add_spacy_data(data, start, end, word_set, arg.version)
        ret = add_relation_data(data, start, end, arg.version)
        sentences_with_triplets += ret[0]
        triplets += ret[1]
        perfect_triplets += ret[2]

    logger.info(f"spacy_ore_tool: {sentences_with_triplets:,} sentences with triplets")
    logger.info(f"spacy_ore_tool: {triplets:,} triplets")
    logger.info(f"spacy_ore_tool: {perfect_triplets:,} perfect triplets")

    indent = arg.indent if arg.indent >= 0 else None
    write_json(arg.target_file, data, indent=indent)
    return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_file", type=str, default="source.json")
    parser.add_argument("--target_file", type=str, default="target.json")
    parser.add_argument("--batch_size", type=int, default=50000)
    parser.add_argument("--indent", type=int, default=2)
    parser.add_argument("--use_cpu", action="store_true")

    parser.add_argument("--verb_file", type=str, default="verb_list.csv")
    parser.add_argument("--vocab_46k_file", type=str, default="vocab_46k.csv")
    parser.add_argument("--vocab_367k_file", type=str, default="vocab_367k_4plus.txt")

    parser.add_argument(
        "--version", type=str, default="20220430",
        choices=["20220308", "chunk_of_chunk", "ie", "hyphen", "20220430"],
    )

    arg = parser.parse_args()
    run_spacy_relation_extraction(arg)
    return


if __name__ == "__main__":
    main()
    sys.exit()
