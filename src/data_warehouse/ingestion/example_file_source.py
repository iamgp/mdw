import os

from .source_base import SourceBase


class ExampleFileSource(SourceBase[list[str]]):
    """
    Example file-based source connector for DLT ingestion.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract(self) -> list[str]:
        if not self.validate():
            raise FileNotFoundError(f"File not found: {self.file_path}")
        with open(self.file_path) as f:
            return f.readlines()

    def validate(self) -> bool:
        return os.path.isfile(self.file_path)
