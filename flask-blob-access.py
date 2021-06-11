from datetime import datetime
import logging
import os
import io
import time
import datetime

from flask import Flask
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset
from azure.storage.blob import ContainerClient
import fsspec
import adlfs


BLOB_ACCOUNT_NAME = os.environ.get("BLOB_ACCOUNT_NAME")
BLOB_ACCOUNT_KEY = os.environ.get("BLOB_ACCOUNT_KEY")

if not BLOB_ACCOUNT_NAME or not BLOB_ACCOUNT_KEY:
    raise ValueError("Must set environment vars")



logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s" , datefmt="%H:%M:%S")
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

app = Flask(__name__)


@app.route("/")
def entry_point():
    return f"ALIVE --- {datetime.datetime.now()}"


@app.route("/read_with_ds/<filename>")
def read_with_ds(filename: str):

    file_path = "access_test/" + filename

    app.logger.info(f"executing read_with_ds() against '{file_path}'")
    
    start_s = time.perf_counter()

    blob_path = "abfs://test-container-laget-av-sigurd/" + file_path
    fs = fsspec.filesystem(protocol='abfs', account_name=BLOB_ACCOUNT_NAME, account_key=BLOB_ACCOUNT_KEY)
    ds = pa.dataset.dataset(blob_path, filesystem=fs, format="parquet")

    table = ds.to_table(columns=["REAL", "DATE", "FGIP"])
    df = table.to_pandas()

    elapsed_s = time.perf_counter() - start_s

    app.logger.info(f"finished read_with_ds() in {elapsed_s:.3f}s, table.shape: {table.shape}")

    retstr = f"DONE - {file_path}"
    retstr += "<br>"
    retstr += f"<br>elapsed_s={elapsed_s:.3f}"
    retstr += f"<br>shape={table.shape}"
    retstr += "<br><br>"
    retstr += table.to_pandas().head().to_html()

    return retstr


@app.route("/download_and_read/<filename>")
def download_and_read(filename: str):
    
    file_path = "access_test/" + filename

    app.logger.info(f"executing download_and_read() against '{file_path}'")

    start_s = time.perf_counter()

    connect_str = f"AccountName={BLOB_ACCOUNT_NAME};AccountKey={BLOB_ACCOUNT_KEY};EndpointSuffix=core.windows.net;DefaultEndpointsProtocol=https;"
    container_client = ContainerClient.from_connection_string(connect_str, container_name="test-container-laget-av-sigurd")
    blob_client = container_client.get_blob_client(file_path)
    downloader = blob_client.download_blob()
    byte_stream = io.BytesIO()
    downloader.download_to_stream(byte_stream)

    table = pq.read_table(columns=["REAL", "DATE", "FGIP"], source=byte_stream)
    df = table.to_pandas()

    elapsed_s = time.perf_counter() - start_s

    app.logger.info(f"finished download_and_read() in {elapsed_s:.3f}s, table.shape: {table.shape}")

    retstr = f"DONE - {file_path}"
    retstr += "<br>"
    retstr += f"<br>elapsed_s={elapsed_s:.3f}"
    retstr += f"<br>shape={table.shape}"
    retstr += "<br><br>"
    retstr += table.to_pandas().head().to_html()

    return retstr


if __name__ == '__main__':
    app.run(debug=True)