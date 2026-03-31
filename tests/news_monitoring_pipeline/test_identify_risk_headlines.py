import pytest
from types import SimpleNamespace
import pandas as pd
from newsmonitor.identify_risk_headlines import (
    number_headlines, 
    batch_headlines,
    extract_index_numbers
)


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def dummy_config():
    return SimpleNamespace(LLM_HEADLINE_BATCH_SIZE=2)



# ----------------------------------------------------------------------
# TESTS 
# ----------------------------------------------------------------------

class TestNumberHeadlines:
    def test_numbers_multiple_headlines(self):
        df = pd.DataFrame({'headline': ['First story', 'Second story', 'Third story']})
        assert number_headlines(df) == ['1. First story', '2. Second story', '3. Third story']

    def test_returns_blank_for_none_or_empty(self):
        df = pd.DataFrame({'headline': ['Valid', None, '']})
        assert number_headlines(df) == ['1. Valid', '2. ', '3. ']

    def test_returns_empty_list_for_empty_dataframe(self):
        df = pd.DataFrame({'headline': []})
        assert number_headlines(df) == []


class TestBatchHeadlines:
    def test_splits_into_correct_batches(self, dummy_config):
        headlines = ['1. A', '2. B', '3. C', '4. D']
        assert batch_headlines(headlines, dummy_config) == ['1. A\n2. B', '3. C\n4. D']

    def test_handles_remainder_batch(self, dummy_config):
        headlines = ['1. A', '2. B', '3. C']
        assert batch_headlines(headlines, dummy_config) == ['1. A\n2. B', '3. C']

    def test_returns_empty_list_for_empty_input(self, dummy_config):
        assert batch_headlines([], dummy_config) == []

    def test_batch_size_larger_than_input(self, dummy_config):
        headlines = ['1. A', '2. B']
        assert batch_headlines(headlines, dummy_config) == ['1. A\n2. B']


class TestExtractIndexNumbers:
    def test_extracts_zero_based_indices_from_list(self):
        indices, status = extract_index_numbers(SimpleNamespace(text='[1, 3, 5]'), 6)
        assert indices == [0, 2, 4]
        assert status == 'ok'

    def test_extracts_list_when_extra_text_surrounds_it(self):
        indices, status = extract_index_numbers(SimpleNamespace(text='These [1, 2, 4] match.'), 5)
        assert indices == [0, 1, 3]
        assert status == 'ok'

    def test_filters_out_of_range_indices(self):
        indices, status = extract_index_numbers(SimpleNamespace(text='[1, 3, 9]'), 4)
        assert indices == [0, 2]
        assert status == 'ok'

    def test_returns_empty_response_when_response_is_none(self):
        indices, status = extract_index_numbers(None, 5)
        assert indices == []
        assert status == 'empty_response'

    def test_returns_empty_response_when_text_attribute_is_missing(self):
        indices, status = extract_index_numbers(SimpleNamespace(), 5)
        assert indices == []
        assert status == 'empty_response'

    def test_returns_empty_response_when_text_is_empty(self):
        indices, status = extract_index_numbers(SimpleNamespace(text=''), 5)
        assert indices == []
        assert status == 'empty_response'

    def test_returns_no_list_when_no_python_list_found(self):
        indices, status = extract_index_numbers(SimpleNamespace(text='Headlines 1, 2 and 3'), 5)
        assert indices == []
        assert status == 'no_list'

    def test_returns_ok_with_empty_indices_when_list_is_empty(self):
        indices, status = extract_index_numbers(SimpleNamespace(text='[]'), 5)
        assert indices == []
        assert status == 'ok'