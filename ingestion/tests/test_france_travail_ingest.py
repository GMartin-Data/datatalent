"""Tests pour les fonctions d'ingestion France Travail."""

import json
import os

from ingestion.france_travail.ingest import deduplicate_offres, write_jsonl


class TestDeduplicateOffres:
    def test_no_duplicates(self):
        offres = [{"id": "A", "titre": "X"}, {"id": "B", "titre": "Y"}]
        result = deduplicate_offres(offres)
        assert len(result) == 2

    def test_removes_duplicates(self):
        offres = [
            {"id": "A", "titre": "v1"},
            {"id": "B", "titre": "Y"},
            {"id": "A", "titre": "v2"},
        ]
        result = deduplicate_offres(offres)
        assert len(result) == 2

    def test_last_wins(self):
        offres = [
            {"id": "A", "titre": "v1"},
            {"id": "A", "titre": "v2"},
        ]
        result = deduplicate_offres(offres)
        assert result[0]["titre"] == "v2"

    def test_empty_list(self):
        assert deduplicate_offres([]) == []


class TestWriteJsonl:
    def test_writes_correct_line_count(self, tmp_path):
        offres = [{"id": "A"}, {"id": "B"}, {"id": "C"}]
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl(offres, file_path)

        with open(file_path) as f:
            lines = f.readlines()
        assert len(lines) == 3

    def test_each_line_is_valid_json(self, tmp_path):
        offres = [{"id": "A", "nom": "test"}, {"id": "B", "nom": "autre"}]
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl(offres, file_path)

        with open(file_path) as f:
            for line in f:
                parsed = json.loads(line)
                assert "id" in parsed

    def test_handles_unicode(self, tmp_path):
        offres = [{"id": "A", "titre": "Ingénieur données"}]
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl(offres, file_path)

        with open(file_path, encoding="utf-8") as f:
            parsed = json.loads(f.readline())
        assert parsed["titre"] == "Ingénieur données"

    def test_empty_list_creates_empty_file(self, tmp_path):
        file_path = str(tmp_path / "test.jsonl")
        write_jsonl([], file_path)
        assert os.path.getsize(file_path) == 0
