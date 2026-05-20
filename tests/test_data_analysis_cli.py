from __future__ import annotations

import importlib.util
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from pathlib import Path
from sqlalchemy.pool import NullPool


ROOT = Path(__file__).resolve().parents[1]
CLI_PATH = ROOT / "skills" / "data-analysis" / "scripts" / "data_analysis_cli.py"
SKILL_PYTHON = ROOT / "skills" / "data-analysis" / "venv" / "bin" / "python"


def load_cli_module():
    spec = importlib.util.spec_from_file_location("data_analysis_cli", CLI_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_data_exploration_reads_vector_store_metadata(monkeypatch) -> None:
    class FakeCollection:
        def query(self, **kwargs):
            assert kwargs["query_texts"] == ["统计订单 GMV"]
            assert kwargs["n_results"] == 3
            return {
                "metadatas": [
                    [
                        {
                            "table": "orders",
                            "description": "订单明细，用于 GMV 和订单数分析",
                            "ddl": "CREATE TABLE orders (id INTEGER, amount REAL, created_at TEXT)",
                        }
                    ]
                ],
                "documents": [[""]],
            }

    class FakeClient:
        def __init__(self, **kwargs):
            assert kwargs["host"] == "vector.example.com"
            assert kwargs["port"] == 8000
            assert kwargs["ssl"] is True

        def get_collection(self, name):
            assert name == "table_catalog"
            return FakeCollection()

    monkeypatch.setitem(sys.modules, "chromadb", SimpleNamespace(HttpClient=FakeClient))
    cli = load_cli_module()
    result = cli.run_data_exploration(
        {"question": "统计订单 GMV", "topK": 3},
        vector_provider="chroma",
        vector_host="vector.example.com",
        vector_port=8000,
        vector_ssl=True,
        vector_collection="table_catalog",
    )

    assert result["ok"] is True
    assert result["source"] == "vector_store"
    assert result["tables"][0]["table"] == "orders"
    assert "CREATE TABLE orders" in result["tables"][0]["ddl"]


def test_data_exploration_reads_starrocks_vector_store_metadata(tmp_path) -> None:
    cli = load_cli_module()
    captured = {}

    class FakeRow:
        _mapping = {
            "doc_id": "orders",
            "content": "订单明细，用于 GMV 和订单数分析",
            "metadata_json": json.dumps(
                {
                    "name": "orders",
                    "create_statement": "CREATE TABLE orders (id INTEGER, amount REAL)",
                }
            ),
        }

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return None

        def execute(self, sql, params):
            captured["sql"] = str(sql)
            captured["params"] = params
            return [FakeRow()]

    class FakeEngine:
        def connect(self):
            return FakeConnection()

        def dispose(self):
            captured["disposed"] = True

    def fake_create_engine(dsn, **kwargs):
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return FakeEngine()

    cli._embed_query_text = lambda **kwargs: [0.1, 0.2, 0.3]
    cli.create_engine = fake_create_engine
    result = cli.run_data_exploration(
        {"question": "GMV", "topK": 3},
        vector_provider="starrocks",
        vector_dsn="mysql+pymysql://readonly:secret@starrocks-fe:9030/data_ai_rag",
        embedding_model="text-embedding-v3",
        embedding_base_url="https://embedding.example.com/v1",
        embedding_api_key="secret",
    )

    assert result["ok"] is True
    assert result["source"] == "vector_store"
    assert result["tables"][0]["table"] == "orders"
    assert result["tables"][0]["ddl"] == "CREATE TABLE orders (id INTEGER, amount REAL)"
    assert "approx_cosine_similarity" in captured["sql"]
    assert "FROM data_ai_rag.rag_documents" in captured["sql"]
    assert captured["params"]["collection_id"] == 1


def test_data_exploration_reads_starrocks_vector_config_from_env(monkeypatch) -> None:
    cli = load_cli_module()
    captured = {}

    monkeypatch.setenv("VECTOR_STORE_PROVIDER", "starrocks")
    monkeypatch.setenv("VECTOR_STORE_STARROCKS_DSN", "mysql+pymysql://root@localhost:9030")
    monkeypatch.setenv("VECTOR_STORE_EMBEDDING_MODEL", "text-embedding-v3")
    monkeypatch.setenv("VECTOR_STORE_EMBEDDING_BASE_URL", "https://embedding.example.com/v1")
    monkeypatch.setenv("VECTOR_STORE_EMBEDDING_API_KEY", "secret")

    def fake_embed(**kwargs):
        captured["embedding_args"] = kwargs
        return [0.1, 0.2, 0.3]

    class FakeRow:
        _mapping = {
            "doc_id": "orders",
            "content": "订单表",
            "metadata_json": json.dumps(
                {
                    "name": "orders",
                    "create_statement": "CREATE TABLE orders (id INTEGER)",
                }
            ),
            "similarity": 0.92,
        }

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return None

        def execute(self, sql, params):
            captured["sql"] = str(sql)
            captured["params"] = params
            return [FakeRow()]

    class FakeEngine:
        def connect(self):
            return FakeConnection()

        def dispose(self):
            pass

    cli._embed_query_text = fake_embed
    cli.create_engine = lambda dsn, **kwargs: FakeEngine()

    result = cli.run_data_exploration({"question": "订单明细", "topK": 5})

    assert result["ok"] is True
    assert "approx_cosine_similarity" in captured["sql"]
    assert "FROM data_ai_rag.rag_documents" in captured["sql"]
    assert captured["params"]["collection_id"] == 1
    assert captured["params"]["query_embedding"] == "[0.1, 0.2, 0.3]"
    assert captured["embedding_args"] == {
        "text": "订单明细",
        "model": "text-embedding-v3",
        "base_url": "https://embedding.example.com/v1",
        "api_key": "secret",
    }
    assert result["tables"][0]["ddl"] == "CREATE TABLE orders (id INTEGER)"


def test_sql_analysis_executes_sql_and_returns_result_file_path(tmp_path) -> None:
    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER, amount REAL, created_at TEXT)")
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [(1, 10.5, "2026-05-18"), (2, 20.0, "2026-05-18")],
    )
    conn.commit()
    conn.close()
    cli = load_cli_module()
    result = cli.run_sql_analysis(
        {
            "sql": "SELECT COUNT(*) AS order_count, SUM(amount) AS gmv FROM orders LIMIT 100",
        },
        engine_kind="mysql",
        dsn=f"sqlite:///{db_path}",
    )

    assert result["ok"] is True
    assert result["rowCount"] == 1
    assert result["resultFile"].endswith(".json")
    rows = json.loads(Path(result["resultFile"]).read_text(encoding="utf-8"))["rows"]
    assert rows == [{"order_count": 2, "gmv": 30.5}]
    assert "rows" not in result


def test_sql_engine_uses_null_pool_for_cli_lifecycle(tmp_path) -> None:
    db_path = tmp_path / "orders.db"
    cli = load_cli_module()
    engine = cli._make_engine("mysql", f"sqlite:///{db_path}")
    try:
        assert isinstance(engine._engine.pool, NullPool)
    finally:
        engine.close()


def test_cli_sql_analysis_accepts_input_file_and_prints_result_file(tmp_path) -> None:
    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER, amount REAL)")
    conn.execute("INSERT INTO orders VALUES (1, 9.0)")
    conn.commit()
    conn.close()

    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "sql": "SELECT id, amount FROM orders LIMIT 100",
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "sql_analysis",
            "--input",
            str(payload_path),
            "--engine-kind",
            "mysql",
            "--dsn",
            f"sqlite:///{db_path}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    response = json.loads(completed.stdout)
    assert response["ok"] is True
    assert Path(response["resultFile"]).is_file()
    assert "rows" not in response


def test_cli_sql_analysis_without_sql_returns_missing_argument(tmp_path) -> None:
    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "sql_analysis",
            "--engine-kind",
            "mysql",
            "--dsn",
            "sqlite:////tmp/unused.db",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    response = json.loads(completed.stdout)
    assert response["ok"] is False
    assert response["errorCode"] == "MissingRequiredArgument"
    assert "--sql" in response["error"]


def test_cli_validate_sql_uses_engine_kind_arg() -> None:
    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "validate_sql",
            "--sql",
            "SELECT 1",
            "--engine-kind",
            "starrocks",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    response = json.loads(completed.stdout)
    assert response["passed"] is True
    assert response["diagnostics"]["engineKind"] == "starrocks"
    assert response["diagnostics"]["dialect"] == "mysql"


def test_cli_validate_sql_requires_engine_kind_arg() -> None:
    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "validate_sql",
            "--sql",
            "SELECT 1",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 2
    assert "--engine-kind" in completed.stderr


def test_cli_sql_analysis_rejects_engine_selection_in_payload(tmp_path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "sql": "SELECT 1",
                "engineKind": "mysql",
                "dialect": "mysql",
            }
        ),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "sql_analysis",
            "--input",
            str(payload_path),
            "--engine-kind",
            "mysql",
            "--dsn",
            "sqlite:////tmp/unused.db",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 1
    response = json.loads(completed.stdout)
    assert response["ok"] is False
    assert response["errorCode"] == "EngineSelectionNotAllowed"
    assert response["fields"] == ["dialect", "engineKind"]


def test_cli_data_exploration_without_sql_does_not_require_sql(tmp_path) -> None:
    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "data_exploration",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    assert completed.returncode == 0
    response = json.loads(completed.stdout)
    assert response["ok"] is False
    assert response["errorCode"] == "MissingMetadataSource"


def test_cli_default_output_dir_is_scripts_output_not_cwd(tmp_path) -> None:
    db_path = tmp_path / "orders.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE orders (id INTEGER)")
    conn.execute("INSERT INTO orders VALUES (1)")
    conn.commit()
    conn.close()

    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "sql": "SELECT id FROM orders LIMIT 100",
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            str(SKILL_PYTHON if SKILL_PYTHON.exists() else sys.executable),
            str(CLI_PATH),
            "sql_analysis",
            "--input",
            str(payload_path),
            "--engine-kind",
            "mysql",
            "--dsn",
            f"sqlite:///{db_path}",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=True,
    )

    response = json.loads(completed.stdout)
    result_file = Path(response["resultFile"])
    assert response["ok"] is True
    assert result_file.is_file()
    assert result_file.is_relative_to(ROOT / "skills" / "data-analysis" / "scripts" / "output")
    assert not (tmp_path / "output").exists()


def test_answer_result_writes_reply_file_from_result_file(tmp_path) -> None:
    result_path = tmp_path / "sql_result.json"
    result_path.write_text(
        json.dumps(
            {
                "sql": "SELECT COUNT(*) AS order_count FROM orders LIMIT 100",
                "rows": [{"order_count": 2}],
                "rowCount": 1,
                "validation": {"warnings": ["SQL 使用了 LIMIT 100。"]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    cli = load_cli_module()
    result = cli.run_answer_result(
        {
            "question": "订单数是多少？",
            "resultFile": str(result_path),
            "resultScopeNotes": ["统计 orders 表。"],
        }
    )

    assert result["ok"] is True
    reply_file = Path(result["replyFile"])
    assert reply_file.is_file()
    reply_payload = json.loads(reply_file.read_text(encoding="utf-8"))
    assert "订单数是多少" in reply_payload["reply"]
    assert str(result_path) == reply_payload["resultFile"]
