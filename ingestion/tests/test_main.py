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


class TestDbtInvocation:
    """Cover the dbt Cloud Run Job orchestration logic (D108)."""

    @patch("main.GCP_REGION", "europe-west1")
    @patch("main.GCP_PROJECT_ID", "datatalent-glaq-2")
    @patch("main.run_v2.JobsClient")
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
    def test_invokes_dbt_on_full_success(self, mock_client_cls: MagicMock) -> None:
        """All sources OK → dbt Cloud Run Job is invoked with the right name."""
        main()

        mock_client_cls.assert_called_once_with()
        mock_client_cls.return_value.run_job.assert_called_once_with(
            name="projects/datatalent-glaq-2/locations/europe-west1/jobs/datatalent-dbt",
        )

    @patch("main.sys.exit")
    @patch("main.GCP_REGION", "europe-west1")
    @patch("main.GCP_PROJECT_ID", "datatalent-glaq-2")
    @patch("main.run_v2.JobsClient")
    @patch(
        "main.SOURCES",
        [
            ("france_travail", MagicMock()),
            ("sirene", MagicMock()),
            ("adzuna", MagicMock()),
            ("urssaf_effectifs", MagicMock()),
            ("urssaf_masse_salariale", MagicMock()),
            ("bmo", MagicMock()),
            ("geo", MagicMock(side_effect=RuntimeError("geo API down"))),
        ],
    )
    def test_invokes_dbt_on_non_critical_failure(
        self,
        mock_client_cls: MagicMock,
        mock_exit: MagicMock,
    ) -> None:
        """Non-critical source failure (geo) → dbt still invoked, exit 1 preserved."""
        main()

        mock_client_cls.return_value.run_job.assert_called_once()
        mock_exit.assert_called_once_with(1)

    @patch("main.sys.exit")
    @patch("main.GCP_REGION", "europe-west1")
    @patch("main.GCP_PROJECT_ID", "datatalent-glaq-2")
    @patch("main.run_v2.JobsClient")
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
    def test_skips_dbt_on_critical_failure(
        self,
        mock_client_cls: MagicMock,
        mock_exit: MagicMock,
    ) -> None:
        """Critical source failure (france_travail) → dbt NOT invoked, exit 1."""
        main()

        mock_client_cls.assert_not_called()
        mock_exit.assert_called_once_with(1)

    @patch("main.GCP_REGION", "europe-west1")
    @patch("main.GCP_PROJECT_ID", "datatalent-glaq-2")
    @patch("main.run_v2.JobsClient")
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
    def test_does_not_raise_when_run_job_fails(
        self,
        mock_client_cls: MagicMock,
    ) -> None:
        """run_job raising → main() logs but does not re-raise (fire-and-forget)."""
        mock_client_cls.return_value.run_job.side_effect = RuntimeError(
            "Cloud Run API unavailable"
        )

        main()  # must not raise

        mock_client_cls.return_value.run_job.assert_called_once()
