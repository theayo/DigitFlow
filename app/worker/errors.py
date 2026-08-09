"""Worker exception hierarchy.

The split matters for control flow: `RetryAfterError` is a pause the caller must
sit through, `NotFoundError` and `ArchiveError` trigger per-name isolation, and
`RunFailed` is terminal — the run cannot continue without leaving a file that can
never be confirmed and would therefore be handed out by /names forever.
"""


class WorkerError(Exception):
    """Base class for everything raised by the download worker."""


class ExternalApiError(WorkerError):
    """The external API answered, but not with success."""


class RetryAfterError(ExternalApiError):
    """429 or 403: the caller must wait before retrying the same operation."""

    def __init__(self, status_code: int, retry_after: float | None, endpoint: str) -> None:
        super().__init__(f"{status_code} from {endpoint}, retry_after={retry_after}")
        self.status_code = status_code
        self.retry_after = retry_after
        self.endpoint = endpoint


class NotFoundError(ExternalApiError):
    """404: at least one of the requested names is missing from the catalog."""


class UnprocessableError(ExternalApiError):
    """422: our request is malformed. Retrying cannot help."""


class ExternalServerError(ExternalApiError):
    """5xx or an unexpected status code. Retried, then surfaced as exhaustion."""


class NetworkExhausted(ExternalApiError):
    """Network errors or 5xx persisted across every allowed attempt."""


class InvalidResponseError(ExternalApiError):
    """A 2xx answer whose body does not match the documented contract.

    Terminal rather than retried: the request went through and the service
    answered successfully, so repeating it would only produce the same body. The
    danger is specific — a missing `file_names` read as an empty list would be
    indistinguishable from "the catalog is finished" and would end the run as
    `done` with the catalog half downloaded.
    """


class ArchiveError(WorkerError):
    """The ZIP payload failed validation and must not be saved."""


class LockLost(WorkerError):
    """The run no longer owns the slot, or ownership can no longer be verified.

    Terminal by design: another worker may already be downloading the same names,
    so confirming anything to the external API from here could mark a file that
    was never stored.
    """


class RunFailed(WorkerError):
    """Terminal condition: the run stops and is recorded as failed."""


class FileUnavailable(RunFailed):
    """One specific file cannot be obtained, however many times we ask.

    Terminal for the run — an unconfirmable file comes back from /names forever
    (§ 2) — but not immediately. The other names of the same chunk have nothing
    to do with this one and are still downloaded, saved and confirmed before the
    run is closed (§ 8.4). That is what keeps the outcome independent of the
    order the names happened to arrive in.
    """

    def __init__(self, name: str, message: str) -> None:
        super().__init__(message)
        self.name = name
