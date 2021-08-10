import builtins
import os
import concurrent.futures
from google.cloud import storage
from google.oauth2 import service_account


class GoogleCloudStorage(builtins.object):
    def __init__(self, project_name, credentials_path):
        self.project_name = project_name
        self.path_to_credentials = credentials_path

        credentials = service_account.Credentials.from_service_account_file(self.path_to_credentials)
        self.storage_client = storage.Client(project=self.project_name, credentials=credentials)

    def list_blobs(self, bucket_name, *args, **kwargs):
        prefixes = kwargs.get("prefixes")

        filenames = []
        blobs = self.storage_client.list_blobs(bucket_name, prefix=prefixes)

        for blob in blobs:
            filenames.append(blob.name)

        return filenames

    def download_file(self, bucket_name: str, source_filename: str, destination_directory: str = "files",
                      endswith: str = ".txt") -> None:

        bucket = self.storage_client.get_bucket(bucket_name)
        data = bucket.get_blob(source_filename)

        filename = source_filename.split("/")[-1]

        try:
            os.mkdir(destination_directory)
        except FileExistsError:
            pass

        if filename.endswith(endswith):
            print(f"Downloading file {source_filename}...")
            data.download_to_filename(f"{destination_directory}/{filename}")

    def download_files(self, bucket_name: str, source_filenames: list, destination_directory: str = "files",
                       endswith: str = ".txt") -> None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file in source_filenames:
                executor.submit(self.download_file, bucket_name=bucket_name,
                                destination_directory=destination_directory,
                                source_filename=file, endswith=endswith)

    def upload_file(self, bucket_name, filename: str, filepath:str, except_files=None):
        if except_files is None:
            except_files = []

        if filename.split("/")[-1] not in except_files:
            print(f"Uploading file {filename}...")
            bucket = self.storage_client.get_bucket(bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_filename(filepath)
            exit(0)


if __name__ == '__main__':
    gcs = GoogleCloudStorage("peerless-rite-303210", "C:/Users/SHIVAM SINGH/peerless-rite-303210.json")
    files = gcs.list_blobs("xct-tst", prefixes="gz_json/")
    # gcs.download_file("xct-prd-orcid-data-xml",  endswith=".gz")
    # local = os.listdir("files")

    # more_files = []
    # for file in files:
    #    more_files.append(file.lstrip("gz/"))
    # print(more_files)
    # print(local)
    # print((set(more_files) - (set(local))))
    files_left = {'764.tar.gz', '700.tar.gz', '435.tar.gz', '438.tar.gz', '479.tar.gz', '847.tar.gz', '611.tar.gz', '389.tar.gz'}

    files_left = list(files_left)

    # gcs.download_files("xct-prd-orcid-data-xml", files_left, "temporary", endswith=".gz")

    except_files_gcs = []
    for file in files[1:]:
        except_files_gcs.append(file.split("/")[-1])

    local_files = os.listdir("../../xml-merger/gzip_json")
    for file in local_files:
        gcs.upload_file("xct-tst", f"gz_json/{file}", f"gzip_json/{file}", except_files_gcs)

    # gsutil -m mv -r . "gs://xct-tst/gz_json"
