"""Unit tests for discovery_engine.search."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from discovery_engine.search import _build_search_payload, _extract_lots, search_lots, search_lots_bulk


class TestBuildSearchPayload:
    def test_default_payload(self):
        payload = _build_search_payload()
        assert payload["query"] == "*"
        assert payload["filter"] == {}
        assert payload["page"] == 0
        assert payload["size"] == 100

    def test_custom_filters_and_sort(self):
        payload = _build_search_payload(
            filters={"make": ["TOYOTA"], "year": ["2020-2024"]},
            sort={"column": "auctionDate", "order": "asc"},
            page=2,
            size=50,
        )
        assert payload["filter"]["make"] == ["TOYOTA"]
        assert payload["sort"]["order"] == "asc"
        assert payload["page"] == 2
        assert payload["size"] == 50


class TestExtractLots:
    def test_nested_content_path(self):
        response = {"data": {"results": {"content": [{"lotNumber": "1"}, {"lotNumber": "2"}]}}}
        lots = _extract_lots(response)
        assert len(lots) == 2

    def test_flat_lots_key(self):
        response = {"lots": [{"lotNumber": "3"}]}
        lots = _extract_lots(response)
        assert len(lots) == 1

    def test_flat_content_key(self):
        response = {"content": [{"lotNumber": "4"}]}
        lots = _extract_lots(response)
        assert len(lots) == 1

    def test_empty_response(self):
        lots = _extract_lots({})
        assert lots == []

    def test_malformed_response(self):
        lots = _extract_lots({"data": "oops"})
        assert lots == []


class TestSearchLots:
    @pytest.mark.asyncio
    async def test_returns_lots_on_success(self):
        http = MagicMock()
        http.post_json = AsyncMock(return_value={
            "data": {"results": {"content": [{"lotNumber": "11111111"}]}}
        })
        lots = await search_lots(http)
        assert len(lots) == 1
        assert lots[0]["lotNumber"] == "11111111"

    @pytest.mark.asyncio
    async def test_returns_empty_list_on_error(self):
        http = MagicMock()
        http.post_json = AsyncMock(side_effect=Exception("network error"))
        lots = await search_lots(http)
        assert lots == []

    @pytest.mark.asyncio
    async def test_passes_filters_to_payload(self):
        http = MagicMock()
        http.post_json = AsyncMock(return_value={"data": {"results": {"content": []}}})
        await search_lots(http, filters={"make": ["FORD"]})
        call_payload = http.post_json.call_args[0][1]
        assert call_payload["filter"]["make"] == ["FORD"]


class TestSearchLotsBulk:
    def _make_http(self, pages: list[list[dict]]) -> MagicMock:
        """Return an http mock that serves one page of results per call."""
        http = MagicMock()
        responses = [
            {"data": {"results": {"content": page}}}
            for page in pages
        ]
        http.post_json = AsyncMock(side_effect=responses)
        return http

    @pytest.mark.asyncio
    async def test_single_page(self):
        http = self._make_http([[{"lotNumber": "1"}, {"lotNumber": "2"}]])
        lots = [lot async for lot in search_lots_bulk(http, max_results=10, page_size=10)]
        assert [l["lotNumber"] for l in lots] == ["1", "2"]

    @pytest.mark.asyncio
    async def test_multi_page_auto_pagination(self):
        http = self._make_http([
            [{"lotNumber": str(i)} for i in range(1, 4)],   # page 0: 3 lots
            [{"lotNumber": str(i)} for i in range(4, 7)],   # page 1: 3 lots
            [],                                               # page 2: empty → stop
        ])
        lots = [lot async for lot in search_lots_bulk(http, max_results=100, page_size=3)]
        assert len(lots) == 6

    @pytest.mark.asyncio
    async def test_stops_at_max_results(self):
        http = self._make_http([
            [{"lotNumber": str(i)} for i in range(1, 6)],  # page 0: 5 lots
            [{"lotNumber": str(i)} for i in range(6, 11)], # page 1: 5 lots (never reached)
        ])
        lots = [lot async for lot in search_lots_bulk(http, max_results=3, page_size=5)]
        assert len(lots) == 3

    @pytest.mark.asyncio
    async def test_stops_on_last_page(self):
        """When a page returns fewer lots than page_size, no further requests are made."""
        http = self._make_http([
            [{"lotNumber": "1"}, {"lotNumber": "2"}],  # page 0: 2 < page_size=5 → stop
        ])
        lots = [lot async for lot in search_lots_bulk(http, max_results=100, page_size=5)]
        assert len(lots) == 2
        http.post_json.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_first_page_returns_nothing(self):
        http = self._make_http([[]])
        lots = [lot async for lot in search_lots_bulk(http, max_results=100, page_size=10)]
        assert lots == []

    @pytest.mark.asyncio
    async def test_search_error_stops_gracefully(self):
        http = MagicMock()
        http.post_json = AsyncMock(side_effect=Exception("API down"))
        lots = [lot async for lot in search_lots_bulk(http, max_results=100, page_size=10)]
        assert lots == []
