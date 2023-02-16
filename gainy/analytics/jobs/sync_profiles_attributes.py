import argparse
import traceback

from gainy.context_container import ContextContainer


def cli(args=None):
    parser = argparse.ArgumentParser(description='Update profiles\' analytics attributes.')
    parser.add_argument('--profile_id', type=int)
    parser.add_argument('--batch_size', type=int, default=100)
    args = parser.parse_args(args)

    profile_id = args.profile_id
    batch_size = args.batch_size

    try:
        with ContextContainer() as context_container:
            if profile_id:
                context_container.analytics_service.sync_profile_attribution(profile_id)
                return

            repo = context_container.get_repository()
            for profile_ids_batch in repo.read_batch_profile_ids(batch_size):
                for profile_id in profile_ids_batch:
                    context_container.analytics_service.sync_profile_attribution(profile_id)

    except Exception as e:
        traceback.print_exc()
        raise e
