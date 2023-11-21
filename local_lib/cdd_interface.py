from datetime import datetime, timedelta
import json
import logging
import requests
import settings

root_log = logging.getLogger()

GET = 'GET'
POST = 'POST'
PUT = 'PUT'

BASE_URL = "https://app.collaborativedrug.com/api/v1/vaults"

CDD_LOAD_PROCESSED_FIELD = 'place'
CDD_EGNYTE_ENTRY_ID_FIELD = 'conditions'


class CddInterface:
    def __init__(self, key, vault_id, egnyte_interface=None):
        self.key = key
        self.vault_id = vault_id
        self.egnyte_interface = egnyte_interface

    def _make_request(self, path, method, params=None, files=None, data=None, json=None):
        resp = None
        headers = {'X-CDD-Token': self.key}
        url = "{}/{}/{}".format(BASE_URL, self.vault_id, path)
        if method == GET:
            resp = requests.get(url, headers=headers, params=params)
        elif method == POST:
            resp = requests.post(url, headers=headers, params=params, files=files, data=data)
        elif method == PUT:
            resp = requests.put(url, headers=headers, params=params, data=data, json=json)
        if resp and resp.status_code == 200:
            return resp.json()
        else:
            raise Exception(resp.content)

    def update_run_fields(self, run_id, data):
        resp = self._make_request("runs/{}".format(run_id), PUT, json=data)
        print(resp)

    def process_runs(self):
        has_data = {}
        runs_modified_after = (datetime.now() - timedelta(days=4)).isoformat()
        params = {'runs_modified_after': runs_modified_after, 'page_size': 1000}
        response = self._make_request('protocols', GET, params)
        for response_object in response['objects']:
            assay_name = response_object['name']
            print(assay_name)
            for run in response_object['runs']:
                if run[CDD_LOAD_PROCESSED_FIELD] == 'Yes':
                    continue
                print(run)
                if run.get(CDD_EGNYTE_ENTRY_ID_FIELD):
                    self.upload_run_attachment(run['id'], run[CDD_EGNYTE_ENTRY_ID_FIELD])
                self.update_run_fields(run['id'], {CDD_LOAD_PROCESSED_FIELD: 'Yes'})
                if run['project']['id'] in settings.CDD_PROJECTS:
                    has_data.setdefault(run['project']['id'], dict())[assay_name] = True
        return has_data

    def _post_to_teams(self, data):
        for project_id, assays_dict in data.items():
            assay_list = sorted(assays_dict.keys())
            project_data = settings.PROJECTS[project_id]
            description = "New results available for {} assay{}:\r".format(len(assay_list), "" if len(assay_list) == 1 else "s")
            for assay in assay_list[:7]:
                description += "- {} \r".format(assay)
            if len(assay_list) > 7:
                description += "- And More... \r"

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
                                    "title": "View Data in CDD",
                                    "url": project_data['data_url']
                                }
                            ]
                        }
                    }
                ]
            }
            r = requests.post(project_data['webhook_url'], json=card)
            print(r.content)

    def upload_assay_run(self, assay_file, filename, project_id, template_id, group_entry_id):
        data = {
            'project': project_id,
            'autoreject': False,
            'mapping_template': template_id,
            'runs': {
                'conditions': group_entry_id,
                # 'egnyte entry id': entry_id
            }
        }
        files = {'file': (filename, assay_file)}
        root_log.info("Uploading File: {}, Data: {}".format(files, str(data)))
        resp = self._make_request('slurps', POST, data={'json': json.dumps(data)}, files=files)
        print(resp)
        return resp['id']

    def upload_run_attachment(self, run_id, egnyte_group_entry_id):
        group_id, entry_id = egnyte_group_entry_id.split('|')
        self.egnyte_interface.get_file()
        files = {'file': (filename, run_file)}

        data = {'resource_class': 'run',
                'resource_id': run_id}
        self._make_request('files', POST, data=data, files=files)

    def get_run_info_for_slurp(self):
        pass
        "/api/v1/vaults/<vault_id>/protocols?slurp=<slurp_id>"