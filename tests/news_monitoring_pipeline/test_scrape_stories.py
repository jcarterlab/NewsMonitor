import pytest
from bs4 import BeautifulSoup
from newsmonitor.scrape_stories import extract_story_text


@pytest.fixture
def make_paragraph_elements():
    def _make(headline_html):
        soup = BeautifulSoup(headline_html, 'html.parser')
        return soup.find_all('p')
    return _make

@pytest.fixture
def story_url():
    return 'https://example_url.com'


class TestExtractStoryText:
    def test_extracts_single_paragraph(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>Paragraph one.</p>')
        assert extract_story_text(elements, story_url) == 'Paragraph one.'

    def test_extracts_multiple_paragraphs(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>Paragraph one.</p><p>Paragraph two.</p>')
        assert extract_story_text(elements, story_url) == 'Paragraph one. Paragraph two.'

    def test_preserves_order_of_unique_paragraphs(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>First.</p><p>Second.</p><p>First.</p><p>Third.</p>')
        assert extract_story_text(elements, story_url) == 'First. Second. Third.'

    def test_normalises_extra_whitespace(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>  Paragraph   one.  </p><p>\nParagraph\t two.\n</p>')
        assert extract_story_text(elements, story_url) == 'Paragraph one. Paragraph two.'

    def test_skips_duplicate_paragraphs(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>One.</p><p>Two.</p><p>Two.</p>')
        assert extract_story_text(elements, story_url) == 'One. Two.'

    def test_removes_zero_width_characters(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>Para\u200bgraph one.</p>')
        assert extract_story_text(elements, story_url) == 'Paragraph one.'

    def test_ignores_empty_paragraphs_among_valid_ones(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p>Paragraph one.</p><p></p><p>   </p><p>Paragraph two.</p>')
        assert extract_story_text(elements, story_url) == 'Paragraph one. Paragraph two.'

    def test_returns_none_for_none_input(self, story_url):
        assert extract_story_text(None, story_url) == None

    def test_returns_none_for_empty_list(self, story_url):
        assert extract_story_text([], story_url) is None

    def test_returns_none_when_all_paragraphs_are_empty(self, make_paragraph_elements, story_url):
        elements = make_paragraph_elements('<p></p><p>   </p>')
        assert extract_story_text(elements, story_url) is None

    
