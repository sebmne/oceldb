from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class OCELMetadata:
    """
    Provenance and versioning metadata for the OCEL log.

    Attributes:
        oceldb_version: The version of oceldb used to convert the file.
        source: The original filename of the source SQLite database.
        converted_at: The exact UTC timestamp of the conversion.
    """

    oceldb_version: str
    source: str
    converted_at: datetime
