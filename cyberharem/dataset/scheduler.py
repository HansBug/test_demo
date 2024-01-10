import json
import os
import warnings
from typing import Literal

from ditk import logging
from gchar.games import get_character_class
from gchar.games.base import Character
from gchar.generic import import_generic
from gchar.resources.pixiv import get_pixiv_posts
from hbutils.string import plural_word
from tqdm.auto import tqdm

from .crawler import DATASET_PVERSION
from ..utils import get_ch_name, get_hf_client, get_hf_fs
from ..utils.ghaction import GithubActionClient

logging.try_init_root(logging.INFO)
logger = logging.getLogger("pyrate_limiter")
logger.disabled = True

import_generic()


class Task:
    def __init__(self, ch):
        self.ch = ch

    @property
    def repo_id(self):
        return f'CyberHarem/{get_ch_name(self.ch)}'

    @property
    def character_name(self):
        return str(self.ch.enname)

    @property
    def game_name(self):
        return self.ch.__class__.__game_name__

    def __eq__(self, other):
        return type(self) == type(other) and self.ch == other.ch

    def __hash__(self):
        return hash((self.ch,))

    def __repr__(self):
        return f'<Task character: {self.ch!r}>'


TaskStatusTyping = Literal['not_started', 'on_going', 'completed']


class Scheduler:
    def __init__(self, game_name: str, concurrent: int = 6):
        self.game_name = game_name
        self.game_cls = get_character_class(self.game_name)
        self.concurrent = concurrent

    def list_task_pool(self):
        def _get_pixiv_posts(ch_: Character):
            ret = get_pixiv_posts(ch_)
            if ret:
                return ret[0]
            else:
                return 0

        all_girls = [
            ch
            for ch in sorted(self.game_cls.all(), key=lambda x: (-_get_pixiv_posts(x), x))
            if ch.gender == 'female' and not ch.is_extra
        ]
        return [Task(ch) for ch in all_girls]

    def get_task_status(self, task: Task) -> TaskStatusTyping:
        hf_client = get_hf_client()
        hf_fs = get_hf_fs()

        if not hf_client.repo_exists(repo_id=task.repo_id, repo_type='dataset'):
            return 'not_started'
        if hf_fs.exists(f'datasets/{task.repo_id}/.git-ongoing'):
            return 'on_going'

        if not hf_fs.exists(f'datasets/{task.repo_id}/README.md'):
            return 'not_started'
        md_text = hf_fs.read_text(f'datasets/{task.repo_id}/README.md')
        if 'outfit' not in md_text.lower():
            return 'not_started'

        if not hf_fs.exists(f'datasets/{task.repo_id}/meta.json'):
            return 'not_started'
        meta_text = hf_fs.read_text(f'datasets/{task.repo_id}/meta.json')
        if 'Waifuc-Raw' not in meta_text:
            return 'not_started'
        meta_info = json.loads(meta_text)
        version = meta_info.get('version')
        if version == DATASET_PVERSION:
            return 'completed'
        else:
            return 'not_started'

    def go_up(self):
        client = GithubActionClient()

        on_goings = []
        not_started = []
        completed = []
        for task in tqdm(self.list_task_pool()):
            try:
                status = self.get_task_status(task)
            except (ValueError,) as err:
                warnings.warn(f'Error: {err!r} for task {task!r}, skipped.')
                continue

            logging.info(f'Task {task!r}, status: {status!r}')
            if status == 'on_going':
                on_goings.append(task)
            elif status == 'not_started':
                not_started.append(task)
            elif status == 'completed':
                completed.append(task)
            else:
                assert False, 'Should not reach this line.'

        logging.info(f'{plural_word(len(completed), "completed task")}, '
                     f'{plural_word(len(on_goings), "on-going task")}, '
                     f'{plural_word(len(not_started), "not started task")}.')

        x = len(on_goings)
        i = 0
        while x < self.concurrent and i < len(not_started):
            task: Task = not_started[i]
            logging.info(f'Scheduling for {task!r} ...')
            client.create_workflow_run(
                'HansBug/test_demo',
                'Test Script',
                data={
                    'character_name': task.character_name,
                    'game_name': task.game_name,
                    'drop_multi': False,
                }
            )

            x += 1
            i += 1


_DEFAULT_CONCURRENCY = 18

if __name__ == '__main__':
    concurrency = int(os.environ.get('CH_CONCURRENCY') or _DEFAULT_CONCURRENCY)
    logging.info(f'Concurrency: {concurrency!r}')
    s = Scheduler('arknights', concurrent=concurrency)
    s.go_up()
