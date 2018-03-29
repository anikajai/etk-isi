from etk.etk_extraction import Extractable, Extraction
import copy
from typing import List, Dict
# from etk.document import Document # will throw exception
from etk.storage_provenance_record import StorageProvenanceRecord

class Segment(Extractable):
    """
    An individual segment in a document.
    For now, it supports recording of JSONPath results, but we should consider extending
    to record segments within a text doc, e.g., by start and end char, or segments within
    a token list with start and end tokens.
    """
    def __init__(self, json_path: str, _value: Dict, _document=None) -> None:
        Extractable.__init__(self)
        self.json_path = json_path
        self._value = _value
        self._extractions = dict()
        self._document = _document

    @property
    def full_path(self) -> str:
        """
        Returns: The full path of a JSONPath match
        """
        return self.json_path

    @property
    def document(self):
        """
        Still thinking about this, having the parent doc inside each extractable is convenient to avoid
        passing around the parent doc to all methods.

        Returns: the parent Document
        """
        return self._document

    def store_extractions(self, extractions: List[Extraction], attribute: str, group_by_tags: bool=True) -> None:
        """
        Records extractions in the container, and for each individual extraction inserts a
        ProvenanceRecord to record where the extraction is stored.
        Records the "output_segment" in the provenance.

        Extractions are always recorded in a list.

        Errors out if the segment is primitive, such as a string.

        Args:
            extractions (List[Extraction]):
            attribute (str): where to store the extractions.
            group_by_tags (bool): Set to True to use tags as sub-keys, and values of Extractions
                with the same tag will be stored in a list as the value of the corresponding key.
                (if none of the Extractions has 'tag', do not group by tags)

        Returns:

        """
        print ("I am storing")
        if not len(extractions):
            return

        if group_by_tags:
            print ("I am storing2")
            try:
                print ("I am storing3")
                #next(x for x in extractions if x.tag)
                print ("I am storing4")
                
                child_segment = Segment(self.full_path+'.'+attribute, {}, self.document)
                provenance_ids = []
                print ("hello\n")
                for e in extractions:
                    child_segment.store_extractions([e], e.tag if e.tag else 'NO_TAGS', False)
                    provenance_ids.append(e.prov_id)

                    #storage_provenance_record = StorageProvenanceRecord(extractable.full_path, extracted_results.extractor_name, extracted_results.start_char, extracted_results.end_char, extracted_results.confidence, extractable.document)
                    #extraction_provenance_records.append(extraction_provenance_record)
                    #self.document.store_provenance(storage_provenance_record)
                self.document.cdr_document[attribute] = child_segment._value
                storage_provenance_record: StorageProvenanceRecord = StorageProvenanceRecord(self.json_path, attribute, provenance_ids, self.document)
                #self.extraction_provenance_records.append(self.extraction_provenance_id_index)
                #self.extraction_provenance_id_index = self.extraction_provenance_id_index + 1
                self.create_provenance(storage_provenance_record)
                return
            except StopIteration:
                pass
        if attribute not in self._extractions:
            self._extractions[attribute] = set([])
        self._extractions[attribute] = self._extractions[attribute].union(extractions)
        try:
            # TODO: why is it necessary to deepcopy the extraction?
            self._value[attribute] = [copy.deepcopy(a_extraction.value) for a_extraction in self._extractions[attribute]]
        except Exception as e:
            print("segment is " + str(type(self._value)))
            print(e)

    @property
    def extractions(self) -> Dict:
        """
        Get the extractions stored in this container.
        Returns: Dict

        """
        return self._extractions


    def create_provenance(self, storage_provenance_record: StorageProvenanceRecord) -> None:
        if "provenances" not in self.document.cdr_document:
            self.document.cdr_document["provenances"] = []
        self.document.cdr_document["provenances"].append(self.get_dict_storage_provenance(storage_provenance_record))
        #self.document.cdr_document["provenances"] = [] # to be done only at the 1st time
        #get_dictionary from extractionProveneaceRecord. Append that dictionary here


    def get_dict_storage_provenance(self, storage_provenance_record: StorageProvenanceRecord) -> None:
        dict = {}
        dict["@type"] = "storage_provenance_record"
        dict["doc_id"] = storage_provenance_record.doc_id
        dict["field"] = storage_provenance_record.field
        dict["destination"] = storage_provenance_record.destination
        dict["provenance_record_id"] = storage_provenance_record.provenance_record_id
        return dict
