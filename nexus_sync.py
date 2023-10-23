#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import json
import hashlib
import os
from urllib.parse import urljoin
import urllib
from time import sleep
import requests
import argparse


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

OS_INFO={
    "centos7":{"repo_url": "http://mirrors.aliyun.com/centos/7/os/x86_64/Packages/", "meta_file": "centos7-primary.xml"},
    "centos8":{"repo_url": "http://mirrors.aliyun.com/centos/8/BaseOS/x86_64/os/Packages", "meta_file": "centos8-primary.xml"},
    "openeuler22":{"repo_url": "https://repo.openeuler.org/openEuler-22.03-LTS/OS/x86_64/Packages/", "meta_file":"openeuler22-primary.xml"},
    "kylinv10":{"repo_url": "https://update.cs2c.com.cn/NS/V10/V10SP2/os/adv/lic/base/aarch64/Packages/", "meta_file":"kylinv10-primary.xml"}
}

class NexusSynchronizer:
    def __init__(self, os_type, local_dir):
        self.os_type = os_type
        self.local_dir = local_dir
        self.success_file = os.path.join(SCRIPT_DIR, 'success.json')
        self.failure_file = os.path.join(SCRIPT_DIR, 'failure.json')
        self.retry_limit = 3

    def get_local_pkgs_dir(self):
        pkgs_path = os.path.join(self.local_dir, f"{self.os_type}_pkgs")
        if not os.path.exists(pkgs_path):
            os.mkdir(pkgs_path)
        return  pkgs_path

    def load_json_data(self, filepath):
        if os.path.exists(filepath):
            with open(filepath, 'r') as jsonfile:
                json_data = json.load(jsonfile)
        else:
            json_data = {}
        return json_data

    def write_json_data(self, filepath, json_data):
        with open(filepath, 'w') as jsonfile:
            json.dump(json_data, jsonfile, indent=4)


    def get_meta_json_file_path(self):
        repo_metadata_file=OS_INFO.get(self.os_type).get("meta_file")

        return os.path.join(SCRIPT_DIR, f'{repo_metadata_file}.json')

    def get_packages(self):
        json_data = self.load_json_data(self.get_meta_json_file_path())
        return json_data

    def sha256sum(self, filename):
        h = hashlib.sha256()
        b = bytearray(128 * 1024)
        mv = memoryview(b)
        with open(filename, 'rb', buffering=0) as f:
            for n in iter(lambda: f.readinto(mv), 0):
                h.update(mv[:n])
        return h.hexdigest()

    def validate_md5(self, downloaded_file, md5_hash):
        dh = self.sha256sum(downloaded_file)
        return dh == md5_hash

    def download_file(self, url, local_path):
        try:
            urllib.urlretrieve(url, local_path)
            return True
        except urllib.ContentTooShortError as e:
            print("The download data is less than expected:", e)
        except Exception as e:
            print("Error: {} {}".format(e, url))
        return False

    def download_package(self, package_url, local_filepath, md5_hash, by_stream=True):
        retries = 0
        while retries < self.retry_limit:
            try:
                if by_stream:
                    success, msg = self.download_package_by_stream(package_url, local_filepath, md5_hash)
                else:
                    success, msg = self.download_package_by_urlretrieve(package_url, local_filepath, md5_hash)

                return success, msg
            except Exception as e:
                return False, str(e)
        return False, 'Retry limit exceeded.'

    def download_package_by_stream(self, package_url, local_filepath, md5_hash):
        try:
            response = requests.get(package_url, timeout=10, stream=True)
            if response.status_code == 200:
                with open(local_filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                if self.validate_md5(local_filepath, md5_hash):
                    return True, None
                else:
                    os.remove(local_filepath)
        except Exception as e:
            return False, str(e)
        return False, 'Retry limit exceeded.'

    def download_package_by_urlretrieve(self, package_url, local_filepath, md5_hash):
        try:
            res = self.download_file(package_url, local_filepath)
            if res and self.validate_md5(local_filepath, md5_hash):
                return True, None
            else:
                os.remove(local_filepath)
            sleep(0.5)  # delay to prevent being seen as a bot
        except Exception as e:
            return False, str(e)
        return False, None

    def sync_repository(self):
        success_packages = {}
        failure_packages = {}
        remote_repo_url = OS_INFO.get(self.os_type).get("repo_url")

        repo_packages = self.get_packages()
        for pkg_name, pkg_md5 in repo_packages.items():
            print("scaning  {}".format(pkg_name))
            pkg_url = urljoin(remote_repo_url, pkg_name)
            local_filename = os.path.join(self.get_local_pkgs_dir(), pkg_name)

            if os.path.exists(local_filename) and self.validate_md5(local_filename, pkg_md5):
                success_packages[pkg_name] = True
                print("The {} rpm is already downloaded and hash is consistent".format(pkg_name))
                continue
            else:
                print("The {} rpm is  inconsistent or not exist, re-downloading".format(pkg_name))

                success, msg = self.download_package(pkg_url, local_filename, pkg_md5, by_stream=True)
                print("The {} rpm download success".format(pkg_name))
                if success:
                    success_packages[pkg_name] = True
                    if pkg_name in failure_packages:
                        del failure_packages[pkg_name]
                else:
                    failure_packages[pkg_name] = msg

            self.write_json_data(self.success_file, success_packages)
            self.write_json_data(self.failure_file, failure_packages)


    def generate_pkg_meta(self):
        import xmltodict

        repo_metadata_file  = OS_INFO.get(self.os_type).get("meta_file")

        with open(os.path.join(SCRIPT_DIR, repo_metadata_file)) as fd:
            doc = xmltodict.parse(fd.read())
        rpms={}
        for pinfo in doc["metadata"]["package"]:
            type= pinfo["@type"] #rpm
            name = pinfo["name"]
            arch = pinfo["arch"]
            ver = pinfo["version"]["@ver"]
            rel = pinfo["version"]["@rel"]
            ctype = pinfo["checksum"]["@type"]
            hash = pinfo["checksum"]["#text"]
            if type== "rpm" and (arch == "x86_64" or arch == "noarch"):
                rpm_name = f"{name}-{ver}-{rel}.{arch}.rpm"
                rpms[rpm_name]=hash

        self.write_json_data( self.get_meta_json_file_path(), rpms)


if __name__ == '__main__':
    # Create the parser
    parser = argparse.ArgumentParser(description='Sync packages from a source to a local directory.')

    # Add the arguments
    parser.add_argument('--os_type',
                        metavar='os_type',
                        type=str,
                        required=True,
                        choices=['centos7', 'centos8', 'openeuler22', 'kylinv10'],
                        help='The type of OS for which to sync packages. Options are: "centos7", "centos8", "openeuler22", "kylinv10"')

    parser.add_argument('--data_dir',
                        metavar='data_dir',
                        type=str,
                        default=os.getcwd(),
                        help='The directory to which to sync packages. Default is the current working directory.')

    # Parse the arguments
    args = parser.parse_args()

    # Use the arguments
    os_type = args.os_type
    DATA_DIR = args.data_dir

    synchronizer = NexusSynchronizer(os_type, DATA_DIR)

    #1. Generate repo file by params
    synchronizer.generate_pkg_meta()
    #2. Get packages by os type
    synchronizer.sync_repository()