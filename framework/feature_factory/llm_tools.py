from abc import ABC, abstractmethod
from typing import List, Union, Tuple
from llama_index import SimpleDirectoryReader
from llama_index.node_parser import SimpleNodeParser
from llama_index.node_parser.extractors import (
    MetadataExtractor,
    TitleExtractor,
    MetadataFeatureExtractor
)
from llama_index.node_parser.extractors.metadata_extractors import (
    DEFAULT_TITLE_COMBINE_TEMPLATE, 
    DEFAULT_TITLE_NODE_TEMPLATE
)
from llama_index.text_splitter import TokenTextSplitter
from llama_index.schema import MetadataMode, Document as Document
from langchain.docstore.document import Document as LCDocument
from langchain.text_splitter import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer
from langchain.document_loaders import UnstructuredPDFLoader
import math, os, re


class LLMTool(ABC):
    """Generic interface for LLMs tools.
    apply and create methods need to be implemented in the children classes.
    create method creates resources for the tool and apply method makes inference using the resources.
    If the resources are not created before calling apply(), create() will be invoked in the beginning of the apply().
    Having a separate create() will make it more efficient to initalize/create all required resouces only once per partition.
    """
    def __init__(self) -> None:
        self._initialized = False
        self.instance = None

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

    def get_instance(self):
        return self._instance


class DocReader(LLMTool):
    """ Generic class for doc reader.
    """
    def create(self):
        ...

    def apply(self, filename: str) -> Union[str, List[Document]]:
        ...


class DocSplitter(LLMTool):
    """ Generic class for doc splitter.
    """
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def _wrap_docs(cls, docs: Union[Document, LCDocument]):
        if isinstance(docs, Document):
            docs = [docs]
        elif isinstance(docs, LCDocument):
            docs = [docs]
        return docs

    @classmethod
    def _to_text(cls, docs: Union[str, List[Document], List[LCDocument]]):
        if not docs: 
            return ""
        if isinstance(docs, str):
            return docs
        docs = cls._wrap_docs(docs)
        if isinstance(docs[0], Document):
            texts = [doc.get_content(metadata_mode=MetadataMode.LLM) for doc in docs]
            return "\n\n".join(texts)
        if isinstance(docs[0], LCDocument):
            texts = [doc.page_content for doc in docs]
            return "\n\n".join(texts)
        raise ValueError("doc type is not defined.")

    @classmethod
    def _to_documents(cls, txt: Union[str, List[Document]]):
        if not txt:
            return None
        txt = cls._wrap_docs(txt)
        if isinstance(txt, str):
            doc = Document(text=txt)
            return [doc]
        if isinstance(txt[0], Document):
            return txt
        if isinstance(txt[0], LCDocument):
            new_docs = []
            for doc in txt:
                new_doc = Document(text=doc.page_content, metadata=doc.metadata)
                new_docs.append(new_doc)
            return new_docs
    
    @classmethod
    def _to_lcdocuments(cls, docs: Union[str, List[Document], List[LCDocument]]):
        if not docs:
            return None
        docs = cls._wrap_docs(docs)
        if isinstance(docs, str):
            new_docs = LCDocument(page_content=docs)
            return [new_docs]
        if isinstance(docs[0], LCDocument):
            return docs
        if isinstance(docs[0], Document):
            new_docs = []
            for doc in docs:
                metadata = doc.metadata or {}
                new_doc = LCDocument(page_content=doc.text, metadata=metadata)
                new_docs.append(new_doc)
            return new_docs

    @classmethod
    def extract_directory_metadata(cls, fileName: str):
        path_parts = os.path.normpath(fileName).split(os.path.sep)
        attrs = {}
        for part in path_parts:
            if "=" in part:
                attr, val = part.split('=')
                if attr and val:
                    attr = re.sub(r'[-_]', ' ', attr, flags=re.IGNORECASE)
                    attrs[attr] = val
        return attrs

    def apply(self, docs: Union[str, List[Document]]) -> List[str]:
        ...


class LlamaIndexDocReader(DocReader):
    """A wrapper class for SimpleDirectoryReader of LlamaIndex.
    For more details, refer to https://gpt-index.readthedocs.io/en/latest/examples/data_connectors/simple_directory_reader.html
    """
    def __init__(self) -> None:
        super().__init__()

    def apply(self, filename: str) -> List[Document]:
        documents = SimpleDirectoryReader(input_files=[filename]).load_data()
        return documents


class UnstructuredDocReader(DocReader):
    """
    A doc reader class using Unstructured API. Only allowed categories will be included in the final parsed text.
    """

    def __init__(self, allowedCategories: Tuple[str]=('NarrativeText', 'ListItem')) -> None:
        super().__init__()
        self.categories = allowedCategories

    def apply(self, filename: str) -> str:
        loader = UnstructuredPDFLoader(filename, mode="elements")
        docs = loader.load()
        filtered_docs = []
        for d in docs:
            if d.metadata.get('category') in self.categories:
                filtered_docs.append(d.page_content)
        combined_text = "\n\n".join(filtered_docs)
        
        return combined_text
    


# class LLMDef(LLMTool):
#     """ A generic class to define LLM instance e.g. using HuggingFace APIs.
#     An example can be found at notebooks/feature_factory_llms.py
#     """
#     def __init__(self) -> None:
#         self._instance = None

#     def get_instance(self):
#         return self._instance
    
class LlamaIndexTitleExtractor(LLMTool):

    def __init__(self, llm_def, nodes, prompt_template=DEFAULT_TITLE_NODE_TEMPLATE, combine_template=DEFAULT_TITLE_COMBINE_TEMPLATE) -> None:
        super().__init__()
        self.llm_def = llm_def
        self.nodes = nodes
        self.prompt_template = prompt_template
        self.combine_template = combine_template

    def create(self):
        if super()._require_init():
            self.llm_def.create()
            self._instance = TitleExtractor(
                nodes=self.nodes, 
                llm=self.llm_def.get_instance(),
                node_template=self.prompt_template,
                combine_template= self.combine_template
            )
    
    def apply(self):
        self.create()



class LlamaIndexDocSplitter(DocSplitter):
    """A class to split documents using LlamaIndex SimpleNodeParser. 
    TokenTextSplitter and TitleExtractor are used to generate text chunks and metadata for each chunk.
    `chunk_size`, `chunk_overlap` are the super parameters to tweak for better response from LLMs.
    `llm` is the LLM instance used for metadata extraction. If not provided, the splitter will generate text chunks only.
    """
    def __init__(self, chunk_size:int=1024, chunk_overlap:int=64, extractors:List[LLMTool]=None) -> None:
        super().__init__()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.extractors = extractors

    def create(self):
        if super()._require_init():
            text_splitter = TokenTextSplitter(
                separator=" ", chunk_size=self.chunk_size, chunk_overlap=self.chunk_overlap
            )
            if self.extractors:
                for extractor in self.extractors:
                    extractor.create()
                extractor_instances = [e.get_instance() for e in self.extractors]
                metadata_extractor = MetadataExtractor(
                    extractors=extractor_instances,
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
        docs = DocSplitter._to_documents(docs)
        self.create()
        doc_nodes = self.node_parser.get_nodes_from_documents(docs)
        for node in doc_nodes:
            if 'file_path' in node.metadata:
                filepath = node.metadata['file_path']
                doc_attrs = DocSplitter.extract_directory_metadata(filepath)
                node.metadata.update(doc_attrs)
        chunks = [node.get_content(metadata_mode=MetadataMode.LLM) for node in doc_nodes]
        return chunks


class LangChainRecursiveCharacterTextSplitter(DocSplitter):
    """ A splitter class to utilize Langchain RecursiveCharacterTextSplitter to generate text chunks.
    If `pretrained_model_path` is provided, the `chunk_size` and `chunk_overlap` will be measured in tokens. 
    If `pretrained_model_path` is not provided, the `chunk_size` and `chunk_overlap` will be measured in characters.
    """
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
        txt = DocSplitter._to_text(docs)
        chunks = self.text_splitter.split_text(txt)
        return chunks


class TokenizerTextSpliter(DocSplitter):
    """ A text splitter which uses LLM defined by `pretrained_tokenizer_path` to encode the input text. 
    The splitting will be applied to the tokens instead of characters.
    """
    def __init__(self, chunk_size=1024, chunk_overlap=64, pretrained_tokenizer_path: str=None) -> None:
        super().__init__()
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.pretrained_tokenizer_path = pretrained_tokenizer_path
        self.tokenizer = None

    def create(self):
        if not self.pretrained_tokenizer_path:
            raise ValueError("Pretrained tokenizer is not defined in TokenizerTextSplitter.")
        if super()._require_init():
            self.tokenizer = AutoTokenizer.from_pretrained(self.pretrained_tokenizer_path)

    def apply(self, text: Union[str, List[Document]]) -> List[str]:
        self.create()
        text = DocSplitter._to_text(text)
        text_length = len(self.tokenizer.encode(text))
        num_chunks = math.ceil(text_length / self.chunk_size)
        chunk_length = math.ceil(len(text) / num_chunks)
        chunks = []
        for i in range(0, len(text), chunk_length):
            start = max(0, i-self.chunk_overlap)
            end = min(len(text), i + chunk_length + self.chunk_overlap)  
            chunks.append(text[start:end])
        return chunks


class LLMFeature(LLMTool):
    """ A container class to hold all required reader and splitter instances.
    The name is the column name for text chunks in the generated spark dataframe.
    If the name is not provided, it will take the variable name in the LLM catalog as the name.
    e.g. 
    class TestCatalog(LLMCatalogBase):

            # define a reader for the documents
            doc_reader = LlamaIndexDocReader()

            # define a text splitter
            doc_splitter = LangChainRecursiveCharacterTextSplitter()

            # define a LLM feature, the name is the column name in the result dataframe
            chunk_col_name = LLMFeature(reader=doc_reader, splitter=doc_splitter)
    
    The name of output dataframe will be `chunk_col_name`.
    """
    def __init__(self, reader: DocReader, splitter: DocSplitter, name: str = "chunks") -> None:
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
        

class LLMUtils:
    """ Util class to define generic split and process methods invoked from spark.
    """
    @classmethod
    def split_docs(cls, fileName: str, llmFeat: LLMFeature):
        print(fileName)
        chunks = llmFeat.apply(fileName)
        return (chunks,)
        
    @classmethod
    def process_docs(cls, partitionData, llmFeat):
        llmFeat.create()
        for row in partitionData:
            yield cls.split_docs(row, llmFeat)

