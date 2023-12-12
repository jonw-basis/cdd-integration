import sys

from local_lib.cdd_interface import CddInterface
from local_lib.egnyte_interface import EgnyteInterface
from local_lib import common

import argparse
import settings
import traceback

log_stream, root_log = common.set_up_logging()


def sync_egnyte_assay_files(dry_run):
    for project_name, research_projects in settings.CDD_PROJECTS.items():
        cdd_interface = CddInterface(settings.CDD_KEY, settings.CDD_VAULT_ID, dry_run=dry_run)
        egnyte_interface = EgnyteInterface(settings.EGNYTE_DOMAIN, settings.EGNYTE_ACCESS_TOKEN, cdd_interface=cdd_interface,
                                           project_id=project_name, dry_run=dry_run)
        egnyte_interface.process_new_assay_files(settings.EGNYTE_BASE_PATH)


def process_cdd_assay_runs(dry_run):
    for project_name, info in settings.CDD_PROJECTS.items():
        egnyte_interface = EgnyteInterface(settings.EGNYTE_DOMAIN, settings.EGNYTE_ACCESS_TOKEN,
                                           project_id=project_name, dry_run=dry_run)
        cdd_interface = CddInterface(settings.CDD_KEY, settings.CDD_VAULT_ID, egnyte_interface=egnyte_interface, research_projects=info['research_projects'],
                                     dry_run=dry_run)
        cdd_interface.process_runs()


def main(args):
    dry_run = args.dry
    if args.all or args.egnyte_sync:
        sync_egnyte_assay_files(dry_run)
    if args.all or args.cdd_assay_runs:
        process_cdd_assay_runs(dry_run)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Select Integration Actions.')
    parser.add_argument('--dry', action='store_true', help='Dry run only')
    parser.add_argument('--all', action='store_true', help='Perform all actions')
    parser.add_argument('--cdd-assay-runs', action='store_true', help='Process CDD Assay Runs')
    parser.add_argument('--egnyte-sync', action='store_true', help='Process Egnyte Sync')
    args = parser.parse_args()
    root_log.info(args)
    main(args)
    sys.exit()
    # try:
    #     main(args)
    # except Exception as e:
    #     common.send_email(settings.LOGGING_EMAIL_RECIPIENTS, "CDD Integration Log Error", str(traceback.format_exc()), settings.SMTP)
    # else:
    #     if root_log.error.counter:
    #         stream_value = log_stream.getvalue()
    #         common.send_email(settings.LOGGING_EMAIL_RECIPIENTS, "CDD Integration Log Error", stream_value, settings.SMTP)
