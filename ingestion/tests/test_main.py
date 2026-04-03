"""Tests for the ingestion entrypoint."""

from unittest.mock import MagicMock, patch

from main import main


class TestMain:
    """Tests for the ingestion entrypoint."""

    @patch("main.run_geo")
    @patch("main.run_bmo")
    @patch("main.run_urssaf_masse_salariale")
    @patch("main.run_urssaf_effectifs")
    @patch("main.run_adzuna")
    @patch("main.run_sirene")
    @patch("main.run_france_travail")
    def test_calls_all_sources_in_order(
        self,
        mock_ft: MagicMock,
        mock_sirene: MagicMock,
        mock_adzuna: MagicMock,
        mock_urssaf_effectifs: MagicMock,
        mock_urssaf_masse_salariale: MagicMock,
        mock_bmo: MagicMock,
        mock_geo: MagicMock,
    ) -> None:
        """main() calls all seven source run() functions."""
        main()

        mock_ft.assert_called_once()
        mock_sirene.assert_called_once()
        mock_adzuna.assert_called_once()
        mock_urssaf_effectifs.assert_called_once()
        mock_urssaf_masse_salariale.assert_called_once()
        mock_bmo.assert_called_once()
        mock_geo.assert_called_once()

    @patch("main.sys.exit")
    @patch("main.run_france_travail", side_effect=RuntimeError("OAuth2 expired"))
    def test_exits_on_failure(
        self,
        mock_ft: MagicMock,
        mock_exit: MagicMock,
    ) -> None:
        """main() logs the error and exits with code 1 on failure."""
        main()

        mock_exit.assert_called_once_with(1)
