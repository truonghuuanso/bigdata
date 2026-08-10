"""
Tang 8 (AI/ML) - Tro ly tra cuu phac do bang RAG, dung dung LlamaIndex
nhu de bai yeu cau (thay the ban tu goi tay Ollama/ChromaDB truoc do).

Cai thu vien can thiet (chay 1 lan):
    pip install llama-index llama-index-llms-ollama llama-index-embeddings-ollama llama-index-vector-stores-chroma chromadb

Truoc khi chay lan dau, can tai 2 model ve Ollama (chi 1 lan):
    docker exec -it ollama ollama pull llama3.2:1b
    docker exec -it ollama ollama pull nomic-embed-text
"""

import chromadb
from llama_index.core import (
    SimpleDirectoryReader,
    VectorStoreIndex,
    StorageContext,
    Settings,
)
from llama_index.llms.ollama import Ollama
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore

DOCS_FOLDER = "./data/phac_do"
CHROMA_PATH = "./chroma_db_llamaindex"
EMBED_MODEL = "nomic-embed-text"
CHAT_MODEL = "llama3.2:1b"

# ----------------------------------------------------------------------
# 1. Cau hinh LlamaIndex dung Ollama local (thay the OpenAI mac dinh)
# ----------------------------------------------------------------------
Settings.llm = Ollama(model=CHAT_MODEL, base_url="http://localhost:11434", request_timeout=120.0)
Settings.embed_model = OllamaEmbedding(model_name=EMBED_MODEL, base_url="http://localhost:11434")

# ----------------------------------------------------------------------
# 2. Ket noi ChromaDB (vector store) qua LlamaIndex
# ----------------------------------------------------------------------
chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
chroma_collection = chroma_client.get_or_create_collection("phac_do")
vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
storage_context = StorageContext.from_defaults(vector_store=vector_store)

# ----------------------------------------------------------------------
# 3. Nap tai lieu (chi nap moi neu ChromaDB dang rong)
# ----------------------------------------------------------------------
if chroma_collection.count() > 0:
    print(f"Da co {chroma_collection.count()} doan trong ChromaDB, tai lai index co san.")
    index = VectorStoreIndex.from_vector_store(vector_store)
else:
    print("Dang doc va nap tai lieu tu thu muc phac do...")
    documents = SimpleDirectoryReader(DOCS_FOLDER).load_data()
    index = VectorStoreIndex.from_documents(documents, storage_context=storage_context)
    print(f"Da nap {len(documents)} tai lieu vao ChromaDB qua LlamaIndex.")

# ----------------------------------------------------------------------
# 4. Tao query engine - LlamaIndex tu lo het buoc retrieve + prompt + goi LLM
# ----------------------------------------------------------------------
SYSTEM_PROMPT = (
    "Ban la tro ly tra cuu thong tin y te noi bo, chi tra loi dua tren tai "
    "lieu duoc cung cap. Neu khong co du du lieu, hay noi ro. Luon nhac "
    "nguoi dung day chi la thong tin tham khao, khong thay the y kien bac si."
)

query_engine = index.as_query_engine(
    similarity_top_k=3,
    system_prompt=SYSTEM_PROMPT,
)


# ----------------------------------------------------------------------
# 5. Giao dien dong lenh
# ----------------------------------------------------------------------
def main():
    print("=" * 60)
    print("TRO LY TRA CUU PHAC DO (RAG voi LlamaIndex) - Do an 5 Hospital DW")
    print("=" * 60)
    print("[Luu y: tai lieu la noi dung minh hoa cho do an hoc tap,")
    print(" khong phai phac do y khoa chinh thuc.]\n")
    print("Go cau hoi cua ban (go 'exit' de thoat):\n")

    while True:
        question = input("Cau hoi: ").strip()
        if question.lower() in ("exit", "quit", "thoat"):
            print("Tam biet!")
            break
        if not question:
            continue

        print("Dang tim kiem tai lieu lien quan va tra loi...")
        try:
            response = query_engine.query(question)
            print(f"\nTra loi: {response}\n")

            sources = {node.metadata.get("file_name", "?") for node in response.source_nodes}
            print(f"(Nguon tham khao: {', '.join(sources)})\n")
        except Exception as e:
            print(f"Loi: {e}")
            print("Kiem tra Ollama da chay chua: docker ps | tim 'ollama'\n")


if __name__ == "__main__":
    main()