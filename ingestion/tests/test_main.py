from unittest.mock import MagicMock, patch

from main import main


class TestMain:
    @patch(
        "main.SOURCES",
        [
            ("france_travail", MagicMock()),
            ("sirene", MagicMock()),
            ("adzuna", MagicMock()),
            ("urssaf_effectifs", MagicMock()),
            ("urssaf_masse_salariale", MagicMock()),
            ("bmo", MagicMock()),
            ("geo", MagicMock()),
        ],
    )
    def test_calls_all_sources_in_order(self) -> None:
        """main() calls all seven source run() functions."""
        from main import SOURCES

        main()

        for name, mock_fn in SOURCES:
            mock_fn.assert_called_once()

    @patch("main.sys.exit")
    @patch(
        "main.SOURCES",
        [
            ("france_travail", MagicMock(side_effect=RuntimeError("OAuth2 expired"))),
            ("sirene", MagicMock()),
            ("adzuna", MagicMock()),
            ("urssaf_effectifs", MagicMock()),
            ("urssaf_masse_salariale", MagicMock()),
            ("bmo", MagicMock()),
            ("geo", MagicMock()),
        ],
    )
    def test_continues_on_failure(self, mock_exit: MagicMock) -> None:
        """main() continues after a source failure and exits with code 1."""
        from main import SOURCES

        main()

        for _, mock_fn in SOURCES:
            mock_fn.assert_called_once()
        mock_exit.assert_called_once_with(1)
