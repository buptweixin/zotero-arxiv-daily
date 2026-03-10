import importlib
import sys
import types


class _FakeOrt:
    def __init__(self):
        self.levels = []

    def set_default_logger_severity(self, level):
        self.levels.append(level)


def test_utils_suppresses_onnxruntime_warnings(monkeypatch):
    fake_ort = _FakeOrt()
    fake_pymupdf = types.ModuleType("pymupdf")
    fake_pymupdf_layout = types.ModuleType("pymupdf.layout")
    fake_pymupdf_layout.activate = lambda: None
    fake_pymupdf.layout = fake_pymupdf_layout

    fake_pymupdf4llm = types.ModuleType("pymupdf4llm")
    fake_pymupdf4llm.to_markdown = lambda *args, **kwargs: ""

    monkeypatch.setitem(sys.modules, "onnxruntime", fake_ort)
    monkeypatch.setitem(sys.modules, "pymupdf", fake_pymupdf)
    monkeypatch.setitem(sys.modules, "pymupdf.layout", fake_pymupdf_layout)
    monkeypatch.setitem(sys.modules, "pymupdf4llm", fake_pymupdf4llm)

    import zotero_arxiv_daily.utils as utils

    importlib.reload(utils)

    assert fake_ort.levels[-1] == 3
