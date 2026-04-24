"""Unit tests for discovery_engine.search."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from discovery_engine.search import _build_search_payload, _extract_lots, search_lots


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
