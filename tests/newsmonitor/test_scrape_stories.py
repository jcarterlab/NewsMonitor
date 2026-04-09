import pytest
from bs4 import BeautifulSoup
from newsmonitor.scrape_stories import extract_story_text


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def make_paragraph_elements():
    def _make(headline_html):
        soup = BeautifulSoup(headline_html, 'html.parser')
        return soup.find_all('p')
    return _make

@pytest.fixture
def website():
    return 'website_1'

@pytest.fixture
def story_url():
    return 'https://example_url.com'



# ----------------------------------------------------------------------
# TESTS 
# ----------------------------------------------------------------------

class TestExtractStoryText:
    def test_extracts_single_paragraph(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('<p>Paragraph one (over 30 characters).</p>')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters).'

    def test_extracts_multiple_paragraphs(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('<p>Paragraph one (over 30 characters).</p><p>Paragraph two (over 30 characters).</p>')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters). Paragraph two (over 30 characters).'

    def test_preserves_order_of_unique_paragraphs(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements(''
        '<p>Paragraph one (over 30 characters).</p>'
        '<p>Paragraph two (over 30 characters).</p>'
        '<p>Paragraph one (over 30 characters).</p>'
        '<p>Paragraph three (over 30 characters).</p>')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters). Paragraph two (over 30 characters). Paragraph three (over 30 characters).'

    def test_normalises_extra_whitespace(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('<p>  Paragraph   one (over 30 characters).  </p><p>\nParagraph\t two\n (over 30 characters).</p>')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters). Paragraph two (over 30 characters).'

    def test_skips_duplicate_paragraphs(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('''
            <p>Paragraph one (over 30 characters).</p>
            <p>Paragraph two (over 30 characters).</p>
            <p>Paragraph two (over 30 characters).</p>
        ''')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters). Paragraph two (over 30 characters).'

    def test_removes_zero_width_characters(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('<p>Para\u200bgraph one (over 30 characters).</p>')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters).'

    def test_ignores_empty_paragraphs_among_valid_ones(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('<p>Paragraph one (over 30 characters).</p><p></p><p>   </p><p>Paragraph two (over 30 characters).</p>')
        assert extract_story_text(elements, website, story_url) == 'Paragraph one (over 30 characters). Paragraph two (over 30 characters).'

    def test_returns_none_for_none_input(self, website, story_url):
        assert extract_story_text(None, website, story_url) == None

    def test_returns_none_for_empty_list(self, website, story_url):
        assert extract_story_text([], website, story_url) is None

    def test_returns_none_when_all_paragraphs_are_empty(self, make_paragraph_elements, website, story_url):
        elements = make_paragraph_elements('<p></p><p>   </p>')
        assert extract_story_text(elements, website, story_url) is None

    
