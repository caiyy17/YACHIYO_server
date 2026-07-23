"""Keep unit tests independent from the repository's runnable configs."""

import ast
import unittest
from pathlib import Path


TEST_DIR = Path(__file__).resolve().parent


class UnitConfigIsolationTest(unittest.TestCase):
    def test_unit_sources_do_not_reference_configs_directory(self):
        offenders = []
        for path in sorted(TEST_DIR.glob("test_*_unit.py")):
            if path == Path(__file__).resolve():
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            docstrings = {
                id(owner.body[0].value)
                for owner in ast.walk(tree)
                if isinstance(
                    owner,
                    (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
                )
                and owner.body
                and isinstance(owner.body[0], ast.Expr)
                and isinstance(owner.body[0].value, ast.Constant)
                and isinstance(owner.body[0].value.value, str)
            }
            for node in ast.walk(tree):
                if not isinstance(node, ast.Constant) or not isinstance(
                    node.value, str
                ) or id(node) in docstrings:
                    continue
                value = node.value.replace("\\", "/")
                if (
                    value == "configs"
                    or value.startswith("configs/")
                    or "/configs/" in value
                ):
                    offenders.append(
                        f"{path.name}:{node.lineno}: {node.value!r}"
                    )

        self.assertEqual(
            offenders,
            [],
            "unit tests must build inline fixtures instead of reading formal "
            "configs:\n" + "\n".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
