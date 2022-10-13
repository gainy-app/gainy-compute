import argparse
import datetime
import re
import traceback
import os

import github3
from github3.exceptions import NotFoundError
from github3.git import Reference
from github3.pulls import ShortPullRequest
from github3.repos import Repository

from gainy.utils import get_logger

logger = get_logger(__name__)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_APP_ID = os.getenv("GITHUB_APP_ID")
GITHUB_APP_INSTALLATION_ID = os.getenv("GITHUB_APP_INSTALLATION_ID")
GITHUB_APP_PRIVATE_KEY = os.getenv("GITHUB_APP_PRIVATE_KEY")


def execute(repo_owner,
            repo_name,
            src_path,
            dest_path,
            base_branch_name='main',
            branch_name=None,
            title=None,
            message=None,
            reviewers=None,
            team_reviewers=None):
    if not branch_name:
        filename = os.path.basename(src_path)
        filename_escaped = re.sub(r"[^a-z0-9]", "_", filename.lower())
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        now_formatted = now.strftime('%Y%m%d%H%M%S%f')
        branch_name = f'{now_formatted}-update-{filename_escaped}'

    if not title:
        title = f"Update {dest_path}"

    logger_extra = {}
    try:
        with open(src_path, "rb") as f:
            file_content = f.read()

        if GITHUB_TOKEN:
            gh = github3.login(token=GITHUB_TOKEN)
        elif GITHUB_APP_PRIVATE_KEY and GITHUB_APP_ID and GITHUB_APP_INSTALLATION_ID:
            gh = github3.GitHub()
            gh.login_as_app_installation(
                bytes(GITHUB_APP_PRIVATE_KEY, 'utf-8'), int(GITHUB_APP_ID),
                int(GITHUB_APP_INSTALLATION_ID))
        else:
            raise Exception('Failed to authenticate.')

        repo: Repository = gh.repository(repo_owner, repo_name)
        ref: Reference = repo.ref(f'heads/{base_branch_name}')

        branch_ref: Reference = repo.create_branch_ref(branch_name,
                                                       ref.object.sha)
        logger_extra["branch_ref"] = branch_ref.as_dict()

        try:
            contents = repo.file_contents(dest_path)
            resp = contents.update(title, file_content, branch=branch_name)
        except NotFoundError:
            resp = None

        if not resp:
            print(resp)
            resp = repo.create_file(dest_path,
                                    title,
                                    file_content,
                                    branch=branch_name)
            print(resp)

        logger_extra["content"] = resp["content"].as_dict()
        logger_extra["commit"] = resp["commit"].as_dict()

        pull: ShortPullRequest = repo.create_pull(title,
                                                  base_branch_name,
                                                  branch_name,
                                                  body=message)
        logger_extra["pull"] = pull.as_dict()

        if reviewers or team_reviewers:
            pull: ShortPullRequest = pull.create_review_requests(
                reviewers, team_reviewers)
            logger_extra["pull"] = pull.as_dict()

        logger.info('Finished', extra=logger_extra)
    except Exception as e:
        logger.exception(e, extra=logger_extra)
        raise e


def cli(args=None):
    parser = argparse.ArgumentParser(description='Update file in repo.')
    parser.add_argument('-r', '--repo', dest='repo', type=str, required=True)
    parser.add_argument('-s',
                        '--src-path',
                        dest='src_path',
                        type=str,
                        required=True)
    parser.add_argument('-d',
                        '--dest-path',
                        dest='dest_path',
                        type=str,
                        required=True)
    parser.add_argument('--reviewers', dest='reviewers', type=str, nargs='+')
    parser.add_argument('--team-reviewers',
                        dest='team_reviewers',
                        type=str,
                        nargs='+')
    args = parser.parse_args(args)

    repo = args.repo
    repo_owner, repo_name = repo.split("/")
    src_path = args.src_path
    dest_path = args.dest_path
    reviewers = args.reviewers
    team_reviewers = args.team_reviewers

    try:
        execute(repo_owner,
                repo_name,
                src_path,
                dest_path,
                reviewers=reviewers,
                team_reviewers=team_reviewers)

    except Exception as e:
        traceback.print_exc()
        raise e
