import shutil
import concurrent.futures
import json
import tarfile
import time
import os
from fnmatch import fnmatch
from xmltodict import parse
import io


count = 0


def merger(filepath: str):
    global count
    json_file_name = filepath.split("/")[-1].rstrip(".tar.gz")
    temp_dir = filepath.replace("/", "_").replace(".", "_")
    """try:
        os.mkdir("temporary")
    except FileExistsError:
        pass"""

    try:
        os.mkdir("json")
    except FileExistsError:
        pass

    full_json = []

    with open(filepath, "rb") as tar_file:
        # file.extractall(f"temporary/{temp_dir}")
        with tarfile.open(fileobj=io.BytesIO(tar_file.read())) as file:
            for member in file.getmembers():
                file_object = file.extractfile(member)
                if file_object:
                    xml = file_object.read()
                    full_json.append(parse(xml))

    with open(f"json/{json_file_name}.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(full_json))

    """root = f"temporary/{temp_dir}"
    pattern = "*.xml"

    full_json = []

    for path, subdirs, files in os.walk(root):
        for name in files:
            if fnmatch(name, pattern):
                with open(os.path.join(path, name), "r", encoding="utf-8") as file:
                    full_json.append(parse(file.read()))

    try:
        os.mkdir("json")
    except FileExistsError:
        pass

    with open(f"json/{json_file_name}.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(full_json))

    # shutil.rmtree(f"temporary/{temp_dir}")"""
    count += 1
    print(f"Processed file {json_file_name}, File processed: {count}", end="\r")


def process_files(directory="files"):
    files = os.listdir(directory)
    global processed_files
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for file in files:
            json_file_name = file.split("/")[-1].replace(".tar.gz", ".json")
            # print(json_file_name in processed_files)
            # exit()
            if json_file_name in processed_files:
                continue
            print(f"Processing file {file}...")
            executor.submit(merger, filepath=f"{directory}/{file}")


if __name__ == '__main__':
    processed_files = os.listdir("json")
    start = time.time()
    # process_files()
    print("start")
    process_files()
    print(f"Time Taken: {time.time() - start}")
