import pytest
from bs4 import BeautifulSoup
from newsmonitor.scrape_headlines import extract_text, extract_link


# ----------------------------------------------------------------------
# FIXTURES 
# ----------------------------------------------------------------------

@pytest.fixture
def make_headline_element():
    def _make(headline_html):
        soup = BeautifulSoup(headline_html, 'html.parser')
        return soup.find('a')
    return _make

@pytest.fixture
def base_url():
    return 'https://example.com'


# ----------------------------------------------------------------------
# TESTS 
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


class TestExtractLink:
    def test_returns_none_when_element_is_none(self, base_url):
        assert extract_link(None, base_url) is None

    def test_returns_none_when_base_url_is_none(self, make_headline_element):
        element = make_headline_element('<a href="/test">Hello world</a>')
        assert extract_link(element, None) is None

    def test_returns_none_when_href_is_missing(self, make_headline_element, base_url):
        element = make_headline_element('<a>Hello world</a>')
        assert extract_link(element, base_url) is None

    def test_returns_none_when_href_is_empty(self, make_headline_element, base_url):
        element = make_headline_element('<a href="">Hello world</a>')
        assert extract_link(element, base_url) is None

    def test_returns_absolute_url_when_href_is_relative(self, make_headline_element, base_url):
        element = make_headline_element('<a href="/news/test">Hello world</a>')
        assert extract_link(element, base_url) == 'https://example.com/news/test'

    def test_returns_href_when_href_is_already_absolute(self, make_headline_element, base_url):
        element = make_headline_element('<a href="https://example.com/news/test">Hello world</a>')
        assert extract_link(element, base_url) == 'https://example.com/news/test'

    def test_joins_relative_href_without_leading_slash(self, make_headline_element, base_url):
        element = make_headline_element('<a href="news/test">Hello world</a>')
        assert extract_link(element, base_url) == 'https://example.com/news/test'

    def test_returns_none_when_element_get_raises_exception(self, base_url):
        class BadElement:
            def get(self, _):
                raise Exception('Boom')

        assert extract_link(BadElement(), base_url) is None


class TestScrapeHeadlineElements: 
    def test_returns_none_when_element_get_raises_exception(self, base_url):
        class BadElement:
            def get(self, _):
                raise Exception('Boom')

        assert extract_link(BadElement(), base_url) is None
        
        