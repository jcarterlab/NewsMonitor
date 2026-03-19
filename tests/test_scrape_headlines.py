import pytest
from bs4 import BeautifulSoup
from news_monitoring_pipeline.scrape_headlines import extract_text, extract_link


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def make_headline_element():
    def _make(headline_html):
        soup = BeautifulSoup(headline_html, 'html.parser')
        return soup.find('a')
    return _make


# ----------------------------------------------------------------------
# EXTRACT_TEXT 
# ----------------------------------------------------------------------

class TestExtractText:
    def test_returns_none_for_none(self):
        assert extract_text(None) is None

    def test_returns_none_for_empty_text(self, make_headline_element):
        element = make_headline_element('<a></a>')
        assert extract_text(element) is None

    def test_strips_whitespace(self, make_headline_element):
        element = make_headline_element('<a>  Hello world  </a>')
        assert extract_text(element) == 'Hello world'

    def test_returns_text_from_nested_tags(self, make_headline_element):
        element = make_headline_element('<a><span>Hello</span> world</a>')
        assert extract_text(element) == 'Hello world'

    def test_normalizes_internal_whitespace(self, make_headline_element):
        element = make_headline_element('<a>Hello     world</a>')
        assert extract_text(element) == 'Hello world'

    def test_normalizes_newlines_and_tabs(self, make_headline_element):
        element = make_headline_element('<a>Hello\n\tworld</a>')
        assert extract_text(element) == 'Hello world'

    def test_returns_none_when_get_text_raises(self):
        class DummyElement:
            def get_text(self, *args, **kwargs):
                raise Exception('Boom')
            
        assert extract_text(DummyElement()) is None


# ----------------------------------------------------------------------
# EXTRACT_LINK
# ----------------------------------------------------------------------

class TestExtractLink:
    def test_returns_none_when_element_is_none(self):
        base_url = 'https://example.com'
        assert extract_link(None, base_url) is None

    def test_returns_none_when_base_url_is_none(self, make_headline_element):
        element = make_headline_element('<a href="/test">Hello world</a>')
        assert extract_link(element, None) is None

    def test_returns_none_when_href_is_missing(self, make_headline_element):
        element = make_headline_element('<a>Hello world</a>')
        base_url = 'https://example.com'
        assert extract_link(element, base_url) is None

    def test_returns_none_when_href_is_empty(self, make_headline_element):
        element = make_headline_element('<a href="">Hello world</a>')
        base_url = 'https://example.com'
        assert extract_link(element, base_url) is None

    def test_returns_absolute_url_when_href_is_relative(self, make_headline_element):
        element = make_headline_element('<a href="/news/test">Hello world</a>')
        base_url = 'https://example.com'
        assert extract_link(element, base_url) == 'https://example.com/news/test'

    def test_returns_href_when_href_is_already_absolute(self, make_headline_element):
        element = make_headline_element('<a href="https://example.com/news/test">Hello world</a>')
        base_url = 'https://another-site.com'
        assert extract_link(element, base_url) == 'https://example.com/news/test'

    def test_joins_relative_href_without_leading_slash(self, make_headline_element):
        element = make_headline_element('<a href="news/test">Hello world</a>')
        base_url = 'https://example.com/'
        assert extract_link(element, base_url) == 'https://example.com/news/test'

    def test_returns_none_when_element_get_raises_exception(self):
        class BadElement:
            def get(self, _):
                raise Exception('boom')

        assert extract_link(BadElement(), 'https://example.com') is None