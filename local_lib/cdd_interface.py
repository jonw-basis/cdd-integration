from datetime import datetime, timedelta, date
from local_lib.resources import Protocol
from io import BytesIO, StringIO
from local_lib.constants import *

import csv
import json as json_lib
import logging
import requests
import settings
import time

root_log = logging.getLogger()

SLEEP_INTERVAL = 5

class CddInterface:
    def __init__(self, key, vault_id, egnyte_interface=None, research_projects=None, dry_run=False):
        self.key = key
        self.vault_id = vault_id
        self.egnyte_interface = egnyte_interface
        self.mapping_templates = {}
        self.protocols_by_name = {}
        self.protocols_by_id = {}
        self.max_results_sync = 1000
        self.research_projects = research_projects
        self.dry_run = dry_run
        self.molecules_by_project = None

    def _make_request(self, path, method, params=None, files=None, data=None, json=None, sync_only=False, as_json=True):
        resp = None
        headers = {'X-CDD-Token': self.key}
        url = "{}/{}/{}".format(settings.CDD_BASE_URL, self.vault_id, path)
        if method == GET:
            resp = self._handle_get_request(url, headers, params, json, sync_only)
        elif method == POST:
            resp = requests.post(url, headers=headers, params=params, files=files, data=data)
        elif method == PUT:
            resp = requests.put(url, headers=headers, params=params, data=data, json=json)
        elif method == DELETE:
            resp = requests.delete(url, headers=headers, params=params, data=data, json=json)

        if resp and resp.status_code == 200:
            return resp.json() if as_json else resp
        else:
            raise Exception(resp.content)

    def _handle_get_request(self, url, headers, params, json, sync_only):
        if params:
            params['page_size'] = self.max_results_sync
        if json:
            json['page_size'] = self.max_results_sync
        root_log.info("GET: URL: {} Params: {} JSON: {}".format(url, params, json))
        resp = requests.get(url, headers=headers, params=params, json=json)
        if resp and resp.status_code == 200:
            response_objects = resp.json()
            if not sync_only and response_objects.get('count') and response_objects['count'] >= (self.max_results_sync - 1):
                root_log.info("Making Async Request. Count = {}".format(response_objects['count']))
                json["async"] = True
                resp = requests.get(url, headers=headers, params=params, json=json)
                if resp and resp.status_code == 200:
                    async_id = resp.json()['id']
                    resp = self._handle_async(async_id)
                else:
                    raise Exception(resp.content)
        else:
            raise Exception(resp.content)
        return resp

    def _handle_async(self, async_id):
        try:
            while True:
                response = self._make_request("export_progress/{}".format(async_id), GET)
                root_log.info("Async Status: {}".format(response['status']))
                if response['status'] not in CDD_ASYNC_IN_PROGRESS_STATUS:
                    if response['status'] == FINISHED:
                        break
                    else:
                        raise Exception(response)
                time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt as e:
            self._make_request("exports/{}".format(async_id), DELETE)
            return None

        return self._make_request("exports/{}".format(async_id), GET, sync_only=True, as_json=False)

    def _get_molecules_by_project(self, created_after=None):
        self.molecules_by_project = {}
        for project_name in self.research_projects['research_projects'].keys():
            params = {
                "molecule_fields": ['ID'],
                "batch_fields": [settings.CDD_PROJECT_NAME_LABEL],
                "no_structures": "true",
                "fields_search": [{"name": settings.CDD_PROJECT_NAME_LABEL,
                                  "text_value": project_name}]
            }
            if created_after:
                params['created_after'] = created_after

            batches = self.get_batches(params)
            if batches:
                for batch in batches:
                    self.molecules_by_project.setdefault(batch['batch_fields'][settings.CDD_PROJECT_NAME_LABEL],
                                                         set()).add(batch['molecule']['id'])

    def get_molecules(self, json):
        resp = self._make_request("molecules", GET, json=json)
        if resp:
            return resp['objects']

    def get_batches(self, json):
        resp = self._make_request("batches", GET, json=json)
        if resp:
            return resp['objects']

    def get_readout_rows(self, json):
        resp = self._make_request("readout_rows", GET, json=json)
        if resp:
            return resp['objects']

    def update_batch(self, batch_id, json):
        resp = self._make_request('batches/{}'.format(batch_id), PUT, json=json)
        root_log.info(str(resp))

    def get_mapping_template(self, mapping_template_id):
        if not self.mapping_templates.get(mapping_template_id):
            resp = self._make_request("mapping_templates/{}".format(mapping_template_id), GET)
            self.mapping_templates[mapping_template_id] = resp
        return self.mapping_templates[mapping_template_id]

    def get_protocol_by_name(self, protocol_name):
        if not self.protocols_by_name.get(protocol_name):
            resp = self._make_request("protocols", GET, params={'names': protocol_name})
            if resp['count'] != 1:
                raise Exception("found multiple protocols: Name: {} Count: {}".format(protocol_name, resp['count']))
            self.protocols_by_name[protocol_name] = Protocol(resp['objects'][0])
        return self.protocols_by_name[protocol_name]

    def set_run_fields(self, run_id, data):
        resp = self._make_request("runs/{}".format(run_id), PUT, json=data)
        return resp

    def process_runs(self):
        has_data = {}
        runs_modified_after = (datetime.now() - timedelta(days=1)).isoformat()
        params = {'runs_modified_after': runs_modified_after, 'page_size': 1000}
        response = self._make_request('protocols', GET, params)
        for response_object in response['objects']:
            assay_name = response_object['name']
            for run in response_object['runs']:
                print(run)
                if run.get(CDD_LOAD_PROCESSED_FIELD) == 'Yes' or not run.get(CDD_INTEGRATION_ID_FIELD):
                    continue
                if not self.dry_run:
                    if run.get(CDD_INTEGRATION_ID_FIELD):
                        self.upload_run_attachment(run['id'], run[CDD_INTEGRATION_ID_FIELD], True)
                    self.set_run_fields(run['id'], {CDD_LOAD_PROCESSED_FIELD: 'Yes'})
                if response_object['protocol_fields'].get(settings.CDD_PROJECT_NAME_LABEL) in self.research_projects:
                    has_data.setdefault(response_object['protocol_fields'].get(settings.CDD_PROJECT_NAME_LABEL), dict())[assay_name] = True

                elif not response_object['protocol_fields'].get(settings.CDD_PROJECT_NAME_LABEL):
                    projects = self._check_for_project_molecules_in_run(run['id'])
                    for project in projects:
                        has_data.setdefault(project, dict())[assay_name] = True
        self._post_to_teams(has_data)
        return has_data

    def _check_for_project_molecules_in_run(self, run_id):
        projects = []
        if self.molecules_by_project is None:
            self._get_molecules_by_project(created_after=(datetime.now() - timedelta(days=120)).date().isoformat())
        rows = self.get_readout_rows(json={'runs': str(run_id), 'type': 'detail_row'})
        molecules = {_.get('molecule') for _ in rows}
        for project_name, project_molecules in self.molecules_by_project.items():
            if project_molecules.intersection(molecules):
                projects.append(project_name)
        return projects

    def _upload_data(self, files, data):
        root_log.info("Uploading File: {}, Data: {}".format(files, str(data)))
        resp = self._make_request('slurps', POST, data={'json': json_lib.dumps(data)}, files=files)
        slurp_id = resp["id"]
        state = resp["state"]

        while state not in CDD_SLURP_IN_PROGRESS_STATUS:
            time.sleep(SLEEP_INTERVAL)
            slurp_resp = self._make_request("slurps/{}".format(slurp_id), GET)
            state = slurp_resp["state"]
            root_log.info("Slurp {} State {}".format(slurp_id, state))
        return slurp_id

    def _post_to_teams(self, data):
        for project_id, assays_dict in data.items():
            assay_list = sorted(assays_dict.keys())
            project_data = self.research_projects[project_id]
            description = "Results available for {} assay{}:\r".format(len(assay_list), "" if len(assay_list) == 1 else "s")
            for assay in assay_list[:7]:
                description += "- {} \r".format(assay)
            if len(assay_list) > 7:
                description += "- And More... \r"
            print(project_id)
            card = {
                "type": "message",
                "summary": "New Assay Results Available",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "contentUrl": None,
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.5",
                            "body": [
                                {
                                    "type": "TextBlock",
                                    "size": "Medium",
                                    "weight": "Bolder",
                                    "text": "New Assay Results Available"
                                },
                                {
                                    "type": "TextBlock",
                                    "text": description,
                                    "wrap": True
                                }
                            ],
                            "actions": [
                                {
                                    "type": "Action.OpenUrl",
                                    "title": "Search For Data in CDD",
                                    "url": project_data['data_url']
                                }
                            ]
                        }
                    }
                ]
            }
            if not self.dry_run:
                r = requests.post(project_data['webhook_url'], json=card)
                root_log.info("Teams Post: {}, {}".format(project_id, r.content))

    def upload_assay_run(self, assay_run, project_id, mapping_template_id, integration_uuid, assay_run_csv_name):
        data = {
            'project': project_id,
            'autoreject': False,
            'mapping_template': mapping_template_id,
            'runs': {
                'conditions': integration_uuid,
            }
        }
        files = {}
        assay_file_stream = StringIO()
        c = csv.writer(assay_file_stream)
        has_valid_files = False
        for i, assay_run_file in enumerate(assay_run):
            if assay_run_file.valid:
                processed_assay_run_array = assay_run_file.data_array
                if i != 0:
                    processed_assay_run_array = assay_run_file.data_array[1:]
                c.writerows(processed_assay_run_array)
                has_valid_files = True
        if has_valid_files:
            assay_file_stream.seek(0)
            files['file'] = (assay_run_csv_name, assay_file_stream)
            slurp_id = self._upload_data(files, data)
            return slurp_id

    def upload_run_attachment(self, run_id, integration_id, set_egnyte_status_complete):
        assay_run_files = self.egnyte_interface.get_files_by_integration_id(integration_id)
        if assay_run_files:
            for run_file, filename, group_id in assay_run_files:
                files = {'file': ("Source - " + filename, run_file)}

                data = {'resource_class': 'run',
                        'resource_id': run_id}
                self._make_request('files', POST, data=data, files=files)
                if set_egnyte_status_complete:
                    metadata = {
                        EGNYTE_FILE_CDD_STATUS: 'Success'
                    }
                    self.egnyte_interface.set_metadata(metadata, group_id, EGNYTE_CDD_SECTION_KEY)
        else:
            print("no files for: {}".format(integration_id))

    def validate_and_group_file_arrays(self, assay_run_file_array, mapping_template_id):
        mapping_template = self.get_mapping_template(mapping_template_id)
        for header in mapping_template['header_mappings']:
            protocol_name = header['definition'].get('protocol_name')
            if protocol_name:
                self.get_protocol_by_name(protocol_name)
        assay_run_group = {}
        for assay_run_file in assay_run_file_array:
            assay_run_file.validate_and_parse_run_conditions(mapping_template, self.protocols_by_name)
            assay_run_group.setdefault(assay_run_file.run_key, []).append(assay_run_file)
        return assay_run_group


