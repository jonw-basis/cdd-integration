import csv
import logging
import egnyte
import openpyxl
import pathlib
import requests
import time

from io import BytesIO, StringIO
from urllib.parse import quote

EGNYTE_CDD_SECTION_KEY = 'cdd'
EGNYTE_LOADED_ENTRY_ID = 'loaded entry id'
EGNYTE_MAPPING_TEMPLATE_ID = 'mapping template id'
RAW_DATA_SHEET = 'format_raw_data_vault'

root_log = logging.getLogger()


def get_metadata_by_key(l, key):
    for item in l:
        if key in item:
            return item[key]


class EgnyteInterface:

    def __init__(self, egnyte_domain, egnyte_access_token, cdd_interface):
        self.egnyte_domain = egnyte_domain
        self.egnyte_access_token = egnyte_access_token
        self.cdd_interface = cdd_interface
        self.egnyte_client = egnyte.EgnyteClient({'domain': egnyte_domain,
                                                  'access_token': egnyte_access_token})

    def _make_request(self, url, method, params):
        root_log.debug(url)
        time.sleep(1)
        headers = {'Authorization': 'Bearer ' + self.egnyte_access_token}
        if method == 'GET':
            resp = requests.get(url, headers=headers, params=params)
        else:
            resp = requests.post(url, headers=headers, params=params)

        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 404:
            return None
        else:
            raise Exception(resp.content)

    def get_metadata(self, path):
        url = "https://{}/pubapi/v1/fs{}".format(self.egnyte_domain, quote(path))
        resp = self._make_request(url, 'GET', params={'list_custom_metadata': True})
        return resp

    def get_file(self, group_id, entry_id):
        url = "https://{}/pubapi/v1/fs-content/ids/file/{}".format(self.egnyte_domain, group_id)
        resp = self._make_request(url, 'GET', params={'entry_id': entry_id})
        return resp


    def _check_for_new_events(self, egnyte_path):
        events_to_process = {}

        events_queue = self.egnyte_client.events.filter(folder=egnyte_path, suppress='user')
        last_event_id = self._get_last_event_id()
        final_event_id = events_queue.latest_event_id

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
                    if event.action not in ['create', 'move'] or event.data['is_folder'] is True:
                        continue
                    events_to_process.setdefault(event.data['target_group_id'], []).append(event)
                root_log.info("returned: {} events, event id: {}, last: {}".format(len(events), event_id, final_event_id))
            if not len(events):
                event_id = final_event_id

        return events_to_process, event_id

    def _get_last_event_id(self):
        return 900
        fname = "{}".format(settings.FILE_LOCK_PATH)
        try:
            f = open(fname, 'r')
        except FileNotFoundError:
            pass
            # event_id = initial_event_id = events_queue.oldest_event_id
        else:
            l = f.readlines()
            event_id = int(l[-1].strip())
        return event_id

    def process_new_assay_files(self, base_path):
        events_by_group_id, max_event_id = self._check_for_new_events(base_path)
        for group_id, events in events_by_group_id.items():
            for event in events:
                target_path = event.data['target_path']
                file_metadata = self.get_metadata(target_path)
                if not file_metadata:
                    continue

                folder_cdd_data = self._get_folder_cdd_data(target_path)
                if folder_cdd_data and folder_cdd_data.get(EGNYTE_MAPPING_TEMPLATE_ID):
                    self._process_file(event, file_metadata, folder_cdd_data)
                break

    def _get_folder_cdd_data(self, path, depth=0):
        parents = pathlib.Path(path).parents
        metadata = self.get_metadata(str(parents[0]))
        cdd_data = get_metadata_by_key(metadata['custom_metadata'], EGNYTE_CDD_SECTION_KEY)
        if not cdd_data and depth <= 4 and len(list(parents)) > 2:
            cdd_data = self._get_folder_cdd_data(parents[0], depth + 1)
        return cdd_data

    def _process_file(self, event, file_metadata, folder_cdd_data):
        print(folder_cdd_data)
        file_cdd_data = get_metadata_by_key(file_metadata['custom_metadata'], EGNYTE_CDD_SECTION_KEY)
        loaded_entry_id = file_cdd_data.get(EGNYTE_LOADED_ENTRY_ID) if file_cdd_data else None
        print(loaded_entry_id)
        if not loaded_entry_id or loaded_entry_id != file_metadata['entry_id']:
            #TODO Need to delete run
            # check if loaded entry id is the current one.
            file_cdd_data = get_metadata_by_key(file_metadata['custom_metadata'], EGNYTE_CDD_SECTION_KEY)
            print(file_cdd_data)
            print(folder_cdd_data)
            print(file_metadata)
            file_obj = self.egnyte_client.file(file_metadata['path'])
            with file_obj.download() as download:
                wb = openpyxl.load_workbook((BytesIO(download.read())))
            f = StringIO()
            c = csv.writer(f)
            for row in wb[RAW_DATA_SHEET]:
                c.writerow([cell.value for cell in row])
            f.seek(0)
            self.cdd_interface.upload_assay_run(f, file_metadata['name'].replace('.xlsx', '.csv'), 'Pluto',
                                                folder_cdd_data[EGNYTE_MAPPING_TEMPLATE_ID], group_entry_id=file_metadata['group_id'] + '|' +file_metadata['entry_id'])