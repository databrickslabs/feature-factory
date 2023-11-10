# Databricks notebook source
# MAGIC %pip install llama-index pypdf

# COMMAND ----------

# MAGIC %pip install typing_extensions==4.7.1

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from framework.feature_factory.llm_tools import LLMFeature, LlamaIndexDocReader, LlamaIndexDocSplitter, LLMDef
from framework.feature_factory import Feature_Factory
import torch
from llama_index.llms import HuggingFaceLLM

# COMMAND ----------

ff = Feature_Factory()

# COMMAND ----------

class MPT7b(LLMDef):
  def create(self):
    torch.cuda.empty_cache()
    generate_params = {
      "temperature": 1.0, 
      "top_p": 1.0, 
      "top_k": 50, 
      "use_cache": True, 
      "do_sample": True, 
      "eos_token_id": 0, 
      "pad_token_id": 0
    }

    self._instance = HuggingFaceLLM(
      max_new_tokens=256,
      generate_kwargs=generate_params,
      # system_prompt=system_prompt,
      # query_wrapper_prompt=query_wrapper_prompt,
      tokenizer_name="mosaicml/mpt-7b-instruct",
      model_name="mosaicml/mpt-7b-instruct",
      device_map="auto",
      tokenizer_kwargs={"max_length": 1024},
      model_kwargs={"torch_dtype": torch.float16, "trust_remote_code": True}
    )
    return None
  
  def apply(self):
    ...

# COMMAND ----------

doc_splitter = LlamaIndexDocSplitter(
  chunk_size = 1024,
  chunk_overlap = 32,
  llm = MPT7b()
)

# COMMAND ----------

llm_feature = LLMFeature (
  name = "chunks",
  reader = LlamaIndexDocReader(),
  splitter = doc_splitter
)

# COMMAND ----------

partition_num = 2

# COMMAND ----------

df = ff.assemble_llm_feature(spark, srcDirectory= "/dbfs/tmp/li_yu/va_llms/pdf", llmFeature=llm_feature, partitionNum=partition_num)

# COMMAND ----------

display(df)

# COMMAND ----------


