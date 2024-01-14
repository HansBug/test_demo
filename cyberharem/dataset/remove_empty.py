import os

from ditk import logging
from tqdm.auto import tqdm

from cyberharem.utils import get_hf_client, get_hf_fs

logging.try_init_root(logging.INFO)

if __name__ == '__main__':
    hf_client = get_hf_client()
    hf_fs = get_hf_fs()
    ch_game = os.environ.get('CH_GAME') or ''
    ch_dry = bool(os.environ.get('CH_DRY') or '')
    if ch_dry:
        logging.info('Dry run mode, no repositories will be removed')

    for item in tqdm(list(hf_client.list_datasets(author='CyberHarem'))):
        if item.id.endswith(ch_game) and hf_fs.exists(f'datasets/{item.id}/.git-empty'):
            if ch_dry:
                logging.info(f'Repository {item.id} will be removed when dry run mode disabled.')
            else:
                logging.info(f'Removing repository {item.id}...')
                hf_client.delete_repo(repo_id=item.id, repo_type='dataset')
