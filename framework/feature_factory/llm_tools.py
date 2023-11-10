from abc import ABC, abstractmethod
from typing import List, Union
from llama_index import SimpleDirectoryReader
from llama_index.node_parser import SimpleNodeParser
from llama_index.node_parser.extractors import (
    MetadataExtractor,
    TitleExtractor
)
from llama_index.text_splitter import TokenTextSplitter
from llama_index.schema import MetadataMode, Document as Document
from langchain.docstore.document import Document as LCDocument
from langchain.text_splitter import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

class LLMTool(ABC):

    def __init__(self) -> None:
        self._initialized = False

    def _require_init(self) -> bool:
        if self._initialized:
            return False
        else:
            self._initialized = True
            return True
        
    @abstractmethod
    def apply(self):
        ...

    @abstractmethod
    def create(self):
        ...


class DocReader(LLMTool):

    def apply(self, filename: str) -> Union[str, List[Document]]:
        ...


class DocSplitter(LLMTool):
    
    def __init__(self) -> None:
        super().__init__()

    def _to_text(self, docs: Union[str, List[Document], List[LCDocument]]):
        if not docs: 
            return ""
        if isinstance(docs, str):
            return docs
        if isinstance(docs[0], Document):
            texts = [doc.get_content(metadata_mode=MetadataMode.LLM) for doc in docs]
            return "\n\n".join(texts)
        if isinstance(docs[0], LCDocument):
            texts = [doc.page_content for doc in docs]
            return "\n\n".join(texts)
        raise ValueError("doc type is not defined.")


    def apply(self, docs: Union[str, List[Document]]) -> List[str]:
        ...


class LlamaIndexDocReader(DocReader):

    def __init__(self) -> None:
        super().__init__()

    def create(self):
        ...

    def apply(self, filename: str):
        documents = SimpleDirectoryReader(input_files=[filename]).load_data()
        return documents


class LLMDef(LLMTool):

    def __init__(self) -> None:
        self._instance = None

    def get_instance(self):
        return self._instance
    


class LlamaIndexDocSplitter(DocSplitter):

    def __init__(self, chunk_size:int=1024, chunk_overlap:int=64, llm:LLMDef=None) -> None:
        super().__init__()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.llm = llm

    def create(self):
        if super()._require_init():
            text_splitter = TokenTextSplitter(
                separator=" ", chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap
            )
            if self.llm:
                self.llm.create()
                metadata_extractor = MetadataExtractor(
                    extractors=[
                        TitleExtractor(nodes=5, llm=self.llm.get_instance())
                    ],
                    in_place=False,
                )
            else:
                metadata_extractor = None

            self.node_parser = SimpleNodeParser.from_defaults(
                text_splitter=text_splitter,
                metadata_extractor=metadata_extractor,
            )
        return None
    
    def apply(self, docs: List[Document]):
        self.create()
        doc_nodes = self.node_parser.get_nodes_from_documents(docs)
        chunks = [node.get_content(metadata_mode=MetadataMode.LLM) for node in doc_nodes]
        return chunks


class LangChainRecursiveCharacterTextSplitter(DocSplitter):

    def __init__(self, chunk_size=1024, chunk_overlap=64, pretrained_model_path: str=None) -> None:
        super().__init__()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.pretrained_model_path = pretrained_model_path
    
    def create(self):
        if super()._require_init():
            if self.pretrained_model_path:
                tokenizer = AutoTokenizer.from_pretrained(self.pretrained_model_path)
                self.text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(tokenizer, 
                    chunk_size=self.chunk_size, 
                    chunk_overlap=self.chunk_overlap)
            else:
                self.text_splitter = RecursiveCharacterTextSplitter(
                    chunk_size = self.chunk_size, 
                    chunk_overlap = self.chunk_overlap
                )
    
    def apply(self, docs):
        self.create()
        txt = super()._to_text(docs)
        chunks = self.text_splitter.split_text(txt)
        return chunks


class LLMFeature(LLMTool):
    
    def __init__(self, name: str, reader: DocReader, splitter: DocSplitter) -> None:
        super().__init__()
        self.name = name
        self.reader = reader
        self.splitter = splitter
    
    def create(self):
        if super()._require_init():
            self.reader.create()
            self.splitter.create()

    def apply(self, filename: str):
        self.create()
        docs = self.reader.apply(filename)
        return self.splitter.apply(docs)
        
