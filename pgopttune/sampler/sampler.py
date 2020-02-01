import optuna
import logging

logger = logging.getLogger(__name__)


def get_sampler(sampling_mode):
    if sampling_mode == 'TPE':
        sampler = optuna.samplers.TPESampler()
    elif sampling_mode == 'RandomSampler':
        sampler = optuna.samplers.RandomSampler()
    elif sampling_mode == 'SkoptSampler':
        sampler = optuna.integration.SkoptSampler()
    elif sampling_mode == 'CmaEsSampler':
        sampler = optuna.integration.CmaEsSampler()
    else:
        raise NotImplementedError('The specified sampling mode {} is not supported.'.format(sampling_mode))
    return sampler
