import optuna
import pytest

from pgopttune.sampler.sampler import get_sampler


class TestSampler:
    def test_get_sampler(self):
        expects = {
            'TPE': optuna.samplers.TPESampler,
            'RandomSampler': optuna.samplers.RandomSampler,
            'SkoptSampler': optuna.integration.SkoptSampler,
            'CmaEsSampler': optuna.integration.CmaEsSampler,
        }
        for n, expect in expects.items():
            result = get_sampler(n)
            assert isinstance(result, expect)

    def test_get_sampler_not_support_value(self):
        not_support_values = 'test'
        with pytest.raises(NotImplementedError):
            get_sampler(not_support_values)
