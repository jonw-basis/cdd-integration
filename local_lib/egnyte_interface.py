import datetime
import logging
import egnyte
import openpyxl
import pathlib
import requests
import time
import uuid

from io import BytesIO, StringIO
from urllib.parse import quote
from local_lib.constants import *
from local_lib.resources import AssayRunFile
root_log = logging.getLogger()


def find_raw_data_sheet(wb, name):
    for name in RAW_DATA_SHEET_NAMES:
        if name in wb:
            return wb[name]
    raise Exception("Cannot find data sheet for: {}".format(name))


def get_metadata_by_key(l, key):
    for item in l:
        if key in item:
            return item[key]


def get_custom_property_by_key(l, namespace, key):
    for item in l:
        if namespace == item['namespace'] and key == item['key']:
            return item


def get_run_group_name(mapping_template_id):
    return mapping_template_id

class EgnyteInterface:

    def __init__(self, egnyte_domain, egnyte_access_token, project_id=None, cdd_interface=None, dry_run=False):
        self.egnyte_domain = egnyte_domain
        self.egnyte_access_token = egnyte_access_token
        self.cdd_interface = cdd_interface
        self.project_id = project_id
        self.assay_runs_to_upload = {}
        self.dry_run = dry_run
        self.lock_file_name = "{}.lock".format(egnyte_domain)
        self.egnyte_client = egnyte.EgnyteClient({'domain': egnyte_domain,
                                                  'access_token': egnyte_access_token})

    def _make_request(self, url, method, params=None, json=None, data=None, files=None, raw=False):
        root_log.debug(url)
        time.sleep(0.2)
        headers = {'Authorization': 'Bearer ' + self.egnyte_access_token}
        if method == GET:
            resp = requests.get(url, headers=headers, params=params)
        elif method == POST:
            resp = requests.post(url, headers=headers, params=params, files=files, data=data, json=json)
        elif method == PUT:
            resp = requests.put(url, headers=headers, params=params, data=data, json=json)
        else:
            raise Exception("Unknown method: {}".format(method))

        if resp.status_code == 200:
            return resp.json() if not raw else resp.content
        elif resp.status_code == 204:
            return True
        elif resp.status_code == 404:
            logging.info("Code: {}, Content: {}".format(resp.status_code, resp.content))
            return None
        else:
            raise Exception(resp.content)

    def set_metadata(self, data, group_id, namespace):
        url = "https://{}/pubapi/v1/fs/ids/file/{}/properties/{}".format(self.egnyte_domain, group_id, namespace)
        resp = self._make_request(url, PUT, json=data)
        return resp

    def get_metadata(self, path):
        url = "https://{}/pubapi/v1/fs{}".format(self.egnyte_domain, quote(path))
        resp = self._make_request(url, GET, params={'list_custom_metadata': True})
        return resp

    def get_file_info_by_id(self, group_id):
        url = "https://{}/pubapi/v1/fs/ids/file/{}".format(self.egnyte_domain, group_id)
        resp = self._make_request(url, GET)
        return resp

    def search_by_metadata(self, key_value_pairs, content_type='FILE'):
        url = "https://{}/pubapi/v1/search".format(self.egnyte_domain)
        data = {
          "type": content_type,
          "key_with_value": key_value_pairs
        }
        resp = self._make_request(url, POST, json=data)
        return resp

    def get_file_by_path(self, path, entry_id=None):
        url = "https://{}/pubapi/v1/fs-content/{}".format(self.egnyte_domain, path)
        params = {}
        if entry_id:
            params['entry_id'] = entry_id
        resp = self._make_request(url, GET, params=params, raw=True)
        return resp

    def get_file(self, group_id, entry_id):
        url = "https://{}/pubapi/v1/fs-content/ids/file/{}".format(self.egnyte_domain, group_id)
        resp = self._make_request(url, GET, params={'entry_id': entry_id}, raw=True)
        fresp = self.get_file_info_by_id(group_id)
        filename = fresp['name']
        return resp, filename

    def get_files_by_integration_id(self, integration_id):
        files = []
        key_value_pairs = [
            {
                "namespace": EGNYTE_CDD_SECTION_KEY,
                "key": EGNYTE_INTEGRATION_ID,
                "value": integration_id
            }
        ]
        resp = self.search_by_metadata(key_value_pairs)
        for result in resp['results']:
            if get_custom_property_by_key(result['file_custom_properties'], EGNYTE_CDD_SECTION_KEY, EGNYTE_FILE_CDD_STATUS)['value'] != EGNYTE_FILE_CDD_STATUS_PROCESSING:
                continue
            file_metadata = self.get_metadata("{}/{}".format(result['path'], result['name']))
            loaded_entry_id = get_custom_property_by_key(result['file_custom_properties'], EGNYTE_CDD_SECTION_KEY, EGNYTE_LOADED_ENTRY_ID)
            if loaded_entry_id:
                file_obj, filename = self.get_file(file_metadata['group_id'], loaded_entry_id['value'])
                files.append((file_obj, result['name'], file_metadata['group_id']))
        return files

    def _check_for_new_events(self, egnyte_path):
        events_to_process = {}

        events_queue = self.egnyte_client.events.filter(folder=egnyte_path, suppress='user')
        final_event_id = events_queue.latest_event_id
        last_event_id = self._get_last_event_id(final_event_id)

        root_log.info("last event id: {} newest event id: {} oldest in queue: {}".format(last_event_id, final_event_id, events_queue.oldest_event_id))
        if last_event_id < events_queue.oldest_event_id:
            event_id = events_queue.oldest_event_id
        else:
            event_id = last_event_id
        while event_id != final_event_id:
            root_log.info("current event id: {}".format(event_id))
            try:
                events = events_queue.list(event_id, count=100)  # get events in batches
            except egnyte.exc.RequestError:
                events = []
            else:
                for event in events:
                    event_id = max(event_id, event.id)
                    if event.action not in ['create', 'move', 'copy'] or event.data['is_folder'] is True:
                        continue
                    events_to_process.setdefault(event.data['target_path'], []).append(event)
                root_log.info("returned: {} events, event id: {}, last: {}".format(len(events), event_id, final_event_id))
            if not len(events):
                event_id = final_event_id

        return events_to_process, event_id

    def _get_last_event_id(self, latest_event_id):
        try:
            f = open(self.lock_file_name, 'r')
        except FileNotFoundError:
            event_id = latest_event_id
        else:
            l = f.readlines()
            event_id = int(l[-1].strip())
        return event_id

    def process_new_assay_files(self, base_path):
        events_by_target_path, max_event_id = self._check_for_new_events(base_path)
        for target_path, events in events_by_target_path.items():
            file_metadata = self.get_metadata(target_path)
            if not file_metadata:
                continue
            folder_cdd_data = self._get_folder_cdd_data(target_path)
            print(folder_cdd_data)
            if folder_cdd_data and folder_cdd_data.get(EGNYTE_MAPPING_TEMPLATE_ID):
                self._process_file(file_metadata, folder_cdd_data)
        if self.assay_runs_to_upload:
            self._upload_assay_runs()
        self._update_lock_file(max_event_id)

    def _update_lock_file(self, last_event_id):
        with open(self.lock_file_name, 'a+') as f:
            f.write("\n{}".format(last_event_id))

    def _get_folder_cdd_data(self, path, depth=0):
        parents = pathlib.Path(path).parents
        try:
            metadata = self.get_metadata(str(parents[0]))
        except Exception:
            return None
        else:
            cdd_data = get_metadata_by_key(metadata['custom_metadata'], EGNYTE_CDD_SECTION_KEY)
            # if not cdd_data and depth <= 4 and len(list(parents)) > 2:
            #     cdd_data = self._get_folder_cdd_data(parents[0], depth + 1)
        return cdd_data

    def _process_file(self, file_metadata, folder_cdd_data):
        file_cdd_data = get_metadata_by_key(file_metadata['custom_metadata'], EGNYTE_CDD_SECTION_KEY)
        loaded_entry_id = file_cdd_data.get(EGNYTE_LOADED_ENTRY_ID) if file_cdd_data else None
        if not loaded_entry_id or loaded_entry_id != file_metadata['entry_id']:
            #TODO Need to delete run/reject slurp if updated file.
            file_cdd_data = get_metadata_by_key(file_metadata['custom_metadata'], EGNYTE_CDD_SECTION_KEY)
            file_obj = self.egnyte_client.file(file_metadata['path'])
            with file_obj.download() as download:
                wb = openpyxl.load_workbook((BytesIO(download.read())))
            file_data_array = []
            raw_data_sheet = find_raw_data_sheet(wb, file_metadata['path'])
            for row in raw_data_sheet:
                file_data_array.append([cell.value for cell in row])
            self.assay_runs_to_upload.setdefault(folder_cdd_data[EGNYTE_MAPPING_TEMPLATE_ID], []).append(AssayRunFile(
                file_data_array, file_metadata['name'], file_metadata['entry_id'], file_metadata['group_id']))

    def _upload_assay_runs(self):
        logging.info(self.assay_runs_to_upload.keys())
        for mapping_template_id, assay_run_file_list in self.assay_runs_to_upload.items():
            integration_uuid = str(uuid.uuid4())
            assay_runs = self.cdd_interface.validate_and_group_file_arrays(assay_run_file_list, mapping_template_id).items()
            for assay_run_group_key, assay_run_list in assay_runs:
                if self.dry_run:
                    continue
                else:
                    assay_run_group_key = "{} - {}".format(datetime.datetime.today().isoformat(), assay_run_group_key)
                    slurp_id = self.cdd_interface.upload_assay_run(assay_run_list, self.project_id,
                                                                   mapping_template_id, integration_uuid, assay_run_group_key + '.csv')
                    logging.info("Slurp: {}".format(slurp_id))
                    for assay_run in assay_run_list:
                        print(assay_run.validation_message)
                        metadata = {'status': EGNYTE_FILE_CDD_STATUS_PROCESSING if assay_run.valid is True else EGNYTE_FILE_CDD_STATUS_FAILED,
                            'slurp id': str(slurp_id) if slurp_id else 0,
                            'loaded entry id': assay_run.entry_id,
                            'integration id': integration_uuid
                        }

                        resp = self.set_metadata(metadata, assay_run.group_id, EGNYTE_CDD_SECTION_KEY)
                        if resp:
                            logging.info("Processed entry id: {}".format(assay_run.entry_id))


