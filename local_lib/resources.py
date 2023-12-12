from local_lib import common
class Protocol:

    def __init__(self, protocol_definition):
        self.protocol_definition = protocol_definition
        self.protocol_name = None
        self.protocol_id = None
        self.readout_protocol_condition_ids = set()
        self._parse_definition()

    def _parse_definition(self):
        self.protocol_id = self.protocol_definition['id']
        self.protocol_name = self.protocol_definition['name']

        for readout_definition in self.protocol_definition['readout_definitions']:
            if readout_definition['protocol_condition']:
                self.readout_protocol_condition_ids.add(readout_definition['id'])


class AssayRunFile:
    def __init__(self, data_array, source_file_name, entry_id, group_id):
        self.data_array = data_array
        self.source_file_name = source_file_name
        self.entry_id = entry_id
        self.group_id = group_id
        self.mapping_template = None
        self.valid = True
        self.validation_message = None
        self.protocol_conditions = {}
        self.run_key = None

    def validate_and_parse_run_conditions(self, mapping_template, protocols_by_name):
        self.mapping_template = mapping_template
        mapping_headers = mapping_template['header_mappings']
        self._fix_typos(self.data_array[0])

        assay_run_columns = {common.strip_value(_): i for i, _ in enumerate(self.data_array[0])}
        #check to see if columns are present
        missing_columns = []
        missing_batch_number = set()
        missing_compound_number = set()
        compound_header_idx = batch_header_idx = well_location_header_idx = concentration_header_idx = None
        protocol_condition_column_idxs = {}
        for column in mapping_headers:
            if column['header']['name'] not in assay_run_columns:
                missing_columns.append(column['header']['name'])

            if column['definition']['type'] == 'InternalFieldDefinition::MoleculeSynonym':
                 compound_header_idx = assay_run_columns.get(column['header']['name'])

            elif column['definition']['type'] == 'InternalFieldDefinition::BatchName':
                batch_header_idx = assay_run_columns.get(column['header']['name'])

            elif column['definition']['type'] == 'InternalFieldDefinition::WellLocation':
                well_location_header_idx = assay_run_columns.get(column['header']['name'])

            elif column['header']['name'].lower() in ['concentration', 'conc', 'conc.'] and assay_run_columns.get(column['header']['name']):
                concentration_header_idx = assay_run_columns[column['header']['name']]
            if column['definition']['type'] == 'ReadoutDefinition':
                if column['header']['name'].lower() in ['concentration', 'conc', 'conc.'] and assay_run_columns.get(column['header']['name']):
                    concentration_header_idx = assay_run_columns[column['header']['name']]
                if column['definition']['id'] in protocols_by_name[column['definition']['protocol_name']].readout_protocol_condition_ids:
                    protocol_condition_column_idxs[assay_run_columns.get(column['header']['name'])] = column['definition']['name']
        if well_location_header_idx:
            #validation of plate files
            pass
        if compound_header_idx is not None and batch_header_idx is not None:
        #check for missing batch values
            for row_num, row in enumerate(self.data_array[1:], start=1):
                compound_value = row[compound_header_idx]
                batch_value = row[batch_header_idx]
                if compound_value and not batch_value:
                    missing_batch_number.add(compound_value)
                if batch_value and not compound_value:
                    missing_compound_number.add(row_num)
                if concentration_header_idx and row[concentration_header_idx] is not None and not compound_value:
                    missing_compound_number.add(row_num)
                for protocol_condition_column_idx, column_name in protocol_condition_column_idxs.items():
                    if compound_value:
                        self.protocol_conditions.setdefault(column_name, set()).add(row[protocol_condition_column_idx])

        if missing_columns or missing_batch_number or missing_compound_number:
            validation_message = "File: {} ".format(self.source_file_name)
            if missing_columns:
                validation_message += " Columns Missing: {}".format(", ".join(missing_columns))
            if missing_batch_number:
                validation_message += " \n Batch Numbers Missing for compounds: {}".format(", ".join(missing_batch_number))
            if missing_compound_number:
                validation_message += " \n Compound Identifier Missing on row: {}".format(", ".join(sorted([str(_) for _ in missing_compound_number])))
            self.valid = False
            self.validation_message = validation_message
        self._make_run_key()

    def _make_run_key(self):
        s = ""
        for column_name, conditions in sorted(self.protocol_conditions.items()):
            if len(conditions) > 1:
                raise Exception("Multiple conditions: File: {} Column: {} Conditions: {}".format(self.source_file_name, column_name, str(conditions)))
            s += "{}-{}|".format(column_name, str(conditions))
        self.run_key = s[:-1]


    def _fix_typos(self, header_array):
        typo_map = {
            'Bacth': 'Batch'
        }
        for idx, header in enumerate(header_array):
            if header in typo_map:
                header_array[idx] = typo_map[header]
