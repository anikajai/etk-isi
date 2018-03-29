from typing import List, Dict
from etk.extraction_provenance_record import ExtractionProvenanceRecord
from etk.etk_extraction import Extractable, Extraction
from etk.extractor import Extractor, InputType
from etk.segment import Segment
from etk.tokenizer import Tokenizer


class Document(Extractable):
    """
        This class wraps raw CDR documents and provides a convenient API for ETK
        to query elements of the document and to update the document with the results
        of extractors.
        """
    def __init__(self, etk, cdr_document: Dict) -> None:

        """
        Wrapper object for CDR documents.

        Args:
            etk (ETK): embed the etk object so that docs have access to global info.
            cdr_document (JSON): the raw CDR document received in ETK.

        Returns: the wrapped CDR document

        """
        Extractable.__init__(self)
        self.etk = etk
        self.cdr_document = cdr_document
        self._value = cdr_document
        self.default_tokenizer = etk.default_tokenizer
        self.extraction_provenance_records = []
        self.extraction_provenance_id_index = 0
        #self.extraction_provenance_batch = {}
        #self.batch_id_index = 0

    def select_segments(self, jsonpath: str) -> List[Segment]:
        """
        Dereferences the json_path inside the document and returns the selected elements.
        This method should compile and cache the compiled json_path in case the same path
        is reused by multiple extractors.

        Args:
            jsonpath (str): a valid JSON path.

        Returns: A list of Segments object that contains the elements selected by the json path.
        """
        path = self.etk.invoke_parser(jsonpath)
        matches = path.find(self.cdr_document)
        segments = []
        for a_match in matches:
            this_segment = Segment(str(a_match.full_path), a_match.value, self)
            segments.append(this_segment)

        return segments

    def invoke_extractor(self, extractor: Extractor, extractable: Extractable = None, tokenizer: Tokenizer = None,
                         joiner: str = "  ", **options) -> List[Extraction]:

        """
        Invoke the extractor on the given extractable, accumulating all the extractions in a list.

        Args:
            extractor (Extractor):
            extractable (extractable): object for extraction
            tokenizer: user can pass custom tokenizer if extractor wants token
            joiner: user can pass joiner if extractor wants text
            options: user can pass arguments as a dict to the extract() function of different extractors

        Returns: List of Extraction, containing all the extractions.

        """
        if not extractable:
            extractable = self

        if not tokenizer:
            tokenizer = self.default_tokenizer

        extracted_results = list()

        if extractor.input_type == InputType.TOKENS:
            tokens = extractable.get_tokens(tokenizer)
            if tokens:
                extracted_results = extractor.extract(tokens, **options)

        elif extractor.input_type == InputType.TEXT:
            text = extractable.get_string(joiner)
            if text:
                extracted_results = extractor.extract(text, **options)

        elif extractor.input_type == InputType.OBJECT:
            extracted_results = extractor.extract(extractable.value, **options)

        elif extractor.input_type == InputType.HTML:
            extracted_results = extractor.extract(extractable.value, **options)

        #self.extraction_provenance_records = []
        for e in extracted_results:
            extraction_provenance_record: ExtractionProvenanceRecord = ExtractionProvenanceRecord(self.extraction_provenance_id_index, extractable.full_path, e.provenance["extractor_name"], e.provenance["start_char"], e.provenance["end_char"],e.provenance["confidence"], extractable.document, extractable.prov_id)
            #self.extraction_provenance_records.append(self.extraction_provenance_id_index)
            e.prov_id = self.extraction_provenance_id_index # for the purpose of provenance hierarrchy tracking
            self.extraction_provenance_id_index = self.extraction_provenance_id_index + 1
            self.create_provenance(extraction_provenance_record)
        # TODO: the reason that extractors must return Extraction objects is so that
        # they can communicate back the provenance.

        return extracted_results
        # record provenance:
        #  add a ProvenanceRecord for the extraction
        #  the prov record for each extraction should point to all extractables:
        #  If the extractables are segments, put them in the "input_segments"
        #  If the extractables are extractions, put the prov ids of the extractions in "input_extractions"

    @property
    def doc_id(self):
       """
       Returns: the doc_id of the CDR document

       """
       return self._value.get("doc_id")

    @doc_id.setter
    def doc_id(self, new_doc_id):
       """

       Args:
           new_doc_id ():

       Returns:

       """
       self._value["doc_id"] = new_doc_id


    def create_provenance(self, extractionProvenanceRecord: ExtractionProvenanceRecord) -> None:
        if "provenances" not in self.cdr_document:
            self.cdr_document["provenances"] = []
        self.cdr_document["provenances"].append(self.get_dict_extraction_provenance(extractionProvenanceRecord))
        #self.document.cdr_document["provenances"] = [] # to be done only at the 1st time
        #get_dictionary from extractionProveneaceRecord. Append that dictionary here


    def get_dict_extraction_provenance(self, extractionProvenanceRecord: ExtractionProvenanceRecord) -> None:
        dict = {}
        dict["@type"] = "extraction_provenance_record"
        dict["@id"] = extractionProvenanceRecord.id
        dict["method"] = extractionProvenanceRecord.method
        dict["confidence"] = extractionProvenanceRecord.extraction_confidence
        dict["origin_record"] = []
        origin_dict = {}
        origin_dict["path"] = extractionProvenanceRecord.origin_record.full_path
        origin_dict["start_char"] = extractionProvenanceRecord.origin_record.start_char
        origin_dict["end_char"] = extractionProvenanceRecord.origin_record.end_char
        dict["origin_record"].append(origin_dict)
        if extractionProvenanceRecord.parent_extraction_provenance is not None:
            dict["provenance_id"] = extractionProvenanceRecord.parent_extraction_provenance
        return dict

