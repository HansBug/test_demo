import os

from tqdm.auto import tqdm

from cyberharem.utils import get_hf_client, get_hf_fs

if __name__ == '__main__':
    hf_client = get_hf_client()
    hf_fs = get_hf_fs()
    ch_game = os.environ.get('CH_GAME') or ''
    for item in tqdm(list(hf_client.list_datasets(author='CyberHarem'))):
        if item.id.endswith(ch_game) and hf_fs.exists(f'datasets/{item.id}/.git-empty'):
            hf_client.delete_repo(repo_id=item.id, repo_type='dataset')
