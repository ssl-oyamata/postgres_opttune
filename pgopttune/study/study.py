import optuna
import logging

logger = logging.getLogger(__name__)


def create_study(study_name, sampler, save_study_history=False, load_study_history=False, direction='minimize',
                 history_database_url='postgresql://postgres@localhost:5432/study_history'):
    """
    create study.
    If a study with the same name already exists, Load past history.
    """
    db_storage = None
    if save_study_history:  # save history in database
        db_storage = optuna.storages.RDBStorage(history_database_url)

    try:
        study = optuna.create_study(study_name=study_name, sampler=sampler, direction=direction,
                                    storage=db_storage)
    except optuna.exceptions.OptunaError:
        # If a study with the same name already exists, Load past history.
        if load_study_history:
            study = optuna.study.load_study(study_name=study_name, sampler=sampler, storage=db_storage)
        else:
            raise ValueError(
                'Another study with name {} already exists. Please specify a different name.\
                or set load_study_history = True in postgres_opttune.conf'.format(study_name))
    return study
