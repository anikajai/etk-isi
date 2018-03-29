from etk.etk_extraction import Extractable
from etk.origin_record import OriginRecord
from typing import List, Dict

from etk.origin_record import OriginRecord
# from etk.document import Document # will throw exception


class ExtractionProvenanceRecord(Extractable):
    """
    An individual segment in a document.
    For now, it supports recording of JSONPath results, but we should consider extending
    to record segments within a text doc, e.g., by start and end char, or segments within
    a token list with start and end tokens.
    """
    def __init__(self, id: int, json_path: str, method: str, start_char: str, end_char:str, confidence, _document=None, parent_extraction_provenance: List[int] = None) -> None:
        Extractable.__init__(self)
        self.id = id
        self.origin_record = OriginRecord(json_path, start_char, end_char)
        self.method = method
        self.extraction_confidence = confidence
        self._document = _document
        self._parent_extraction_provenance = parent_extraction_provenance

    @property
    def full_path(self) -> str:
        """
        Returns: The full path of a JSONPath match
        """
        return self.json_path

    @property
    def document(self):
        """
        Returns: the parent Document
        """
        return self._document

    @property
    def parent_extraction_provenance(self):
        """
        Returns: the parent Document
        """
        return self._parent_extraction_provenance