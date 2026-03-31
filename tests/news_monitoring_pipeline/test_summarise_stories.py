import pytest
from types import SimpleNamespace
from newsmonitor.summarise_stories import batch_story_texts


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def dummy_config():
    return SimpleNamespace(LLM_STORY_WORDS_BATCH_SIZE=2)



# ----------------------------------------------------------------------
# TESTS 
# ----------------------------------------------------------------------

class TestBatchStoryTexts:
    def test_multiple_stories(self, dummy_config):
        story_texts = ['One', 'Two', 'Three', 'Four']
        assert batch_story_texts(story_texts, dummy_config) == ['One Two', 'Three Four']

    def test_batch_size_overflow(self, dummy_config):
        story_texts = ['One', 'Two', 'Three', 'Four', 'Five']
        assert batch_story_texts(story_texts, dummy_config) == ['One Two', 'Three Four', 'Five']

    def test_less_than_batch_size(self, dummy_config):
        story_texts = ['One']
        assert batch_story_texts(story_texts, dummy_config) == ['One']

    def test_oversized_story(self, dummy_config): 
        story_texts = ['One Two, Three']
        assert batch_story_texts(story_texts, dummy_config) == ['One Two, Three']

    def test_mixed_and_oversized_story(self, dummy_config):
        story_texts = ['One Two, Three', 'Four']
        assert batch_story_texts(story_texts, dummy_config) == ['One Two, Three', 'Four']

    def test_skips_empty_or_invalid_stories(self, dummy_config):
        story_texts = ['One', 'Two', '', None, 'Five']
        assert batch_story_texts(story_texts, dummy_config) == ['One Two', 'Five']

    def test_empty_input_returns_empty_list(self, dummy_config):
        assert batch_story_texts([], dummy_config) == []

    def test_raises_error_for_invalid_max_words(self, dummy_config):
        dummy_config.LLM_STORY_WORDS_BATCH_SIZE = 0
        with pytest.raises(ValueError):
            batch_story_texts(['One'], dummy_config)
