from local_lib.cdd_interface import CddInterface
from local_lib.egnyte_interface import EgnyteInterface
from local_lib import common

import settings


log_stream, root_log = common.set_up_logging()


def sync_egnyte_assay_files():
    cdd_interface = CddInterface(settings.CDD_KEY, settings.CDD_VAULT_ID)
    egnyte_interface = EgnyteInterface(settings.EGNYTE_DOMAIN, settings.EGNYTE_ACCESS_TOKEN, cdd_interface=cdd_interface, project_id='Pluto')
    egnyte_interface.process_new_assay_files(settings.EGNYTE_BASE_PATH)


def process_cdd_assay_runs():
    egnyte_interface = EgnyteInterface(settings.EGNYTE_DOMAIN, settings.EGNYTE_ACCESS_TOKEN, project_id='Pluto')
    cdd_interface = CddInterface(settings.CDD_KEY, settings.CDD_VAULT_ID, egnyte_interface=egnyte_interface)
    cdd_interface.process_runs()


def main():
    sync_egnyte_assay_files()
    # process_cdd_assay_runs()


if __name__ == "__main__":
    main()