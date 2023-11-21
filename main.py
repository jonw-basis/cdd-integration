from local_lib.cdd_interface import CddInterface
from local_lib.egnyte_interface import EgnyteInterface
from local_lib import common

import settings


log_stream, root_log = common.set_up_logging()


def main():
    cdd_interface = CddInterface(settings.CDD_KEY, settings.CDD_VAULT_ID)
    egnyte_interface = EgnyteInterface(settings.EGNYTE_DOMAIN, settings.EGNYTE_ACCESS_TOKEN, cdd_interface)
    egnyte_interface.process_new_assay_files(settings.EGNYTE_BASE_PATH)
    # cdd_interface.process_runs()

if __name__ == "__main__":
    main()