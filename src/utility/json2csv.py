#!/usr/bin/env python
# -*- coding=utf-8 -*-

"""
convert from json to csv file
"""

import sys
import json
import os
import unicodecsv as csv


def get_folder_name_ext(filename):
    """
    return the given file's folder, filename, and ext info
    """
    folder = os.path.dirname(filename)
    name, ext = os.path.splitext(os.path.basename(filename))
    return {"folder": folder, "name": name, "ext": ext}


def format_json(json_data):
    """
    dump the formated json data
    """
    return json.dumps(json_data, sort_keys=True, indent=4,
                      separators=(',', ': '), ensure_ascii=False).encode('utf8')


def write_to_csvdict(rows, csv_file, csv_header):
    """
    write the result to CSV file
    """
    if rows is None:
        print("None to write")
        return

    with open(csv_file, "wb") as csv_output:
        writer = csv.DictWriter(csv_output, fieldnames=csv_header)

        writer.writeheader()
        writer.writerows(rows)
    print("item written: " + str(len(rows)))


def load_json_file(filename):
    try:
        with open(filename) as input_data:
            return json.load(input_data)
    except:
        print("exception... to open ", filename)
        return None


def convert_2_csv(filename):
    """
    convert the give json file to csv file
    """
    print("processing: ", filename)

    json_content = load_json_file(filename)
    header = ["review_type", "review_content"]
    content = []
    for positive_review in json_content["positive_review"]:
        review = positive_review[0]
        item = {
            "review_type": "positive",
            "review_content": review
        }
        # print review
        content.append(item)

    for negative_review in json_content["negative_reviews"]:
        review = negative_review[0]
        item = {
            "review_type": "negative",
            "review_content": review
        }
        # print review
        content.append(item)

    file_info = get_folder_name_ext(filename)
    csv_file = os.path.join(file_info["folder"],
                            file_info["name"] + "_positive_" + str(len(json_content["positive_review"])) +
                            "_negative_" + str(len(json_content["negative_reviews"])) + ".csv")

    # print cvs_format
    write_to_csvdict(content, csv_file, header)


def main(argv):
    """
    the main entry of this file
    """
    usage = os.path.basename(__file__) + " folder_name"
    if len(argv) != 1:
        print("Usage: ", usage)
        return

    folder_name = argv[0]

    for f in os.listdir(folder_name):
        full_filename = os.path.join(folder_name, f)

        if os.path.isfile(full_filename):
            if full_filename.endswith(".json"):
                convert_2_csv(full_filename)


if __name__ == "__main__":
    main(sys.argv[1:])
