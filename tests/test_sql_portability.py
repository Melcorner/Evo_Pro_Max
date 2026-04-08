import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_SQL_PATHS = [
    PROJECT_ROOT / "app" / "api",
    PROJECT_ROOT / "app" / "workers",
    PROJECT_ROOT / "app" / "stores",
    PROJECT_ROOT / "app" / "services",
    PROJECT_ROOT / "app" / "clients",
    PROJECT_ROOT / "app" / "handlers",
    PROJECT_ROOT / "app" / "main.py",
]


class ParameterizedSqlVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.scope_stack: list[set[str]] = [set()]
        self.issues: list[tuple[int, str]] = []

    def _push_scope(self) -> None:
        self.scope_stack.append(set())

    def _pop_scope(self) -> None:
        self.scope_stack.pop()

    def _remember_aq_name(self, node: ast.AST, value: ast.AST) -> None:
        if not (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "aq"
        ):
            return

        if isinstance(node, ast.Name):
            self.scope_stack[-1].add(node.id)

    def _is_aq_wrapped(self, node: ast.AST) -> bool:
        return (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "aq"
        )

    def _is_known_adapted_name(self, node: ast.AST) -> bool:
        if not isinstance(node, ast.Name):
            return False
        return any(node.id in scope for scope in reversed(self.scope_stack))

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._remember_aq_name(target, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._remember_aq_name(node.target, node.value)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._push_scope()
        self.generic_visit(node)
        self._pop_scope()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._push_scope()
        self.generic_visit(node)
        self._pop_scope()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._push_scope()
        self.generic_visit(node)
        self._pop_scope()

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in {"execute", "executemany"}
            and len(node.args) >= 2
        ):
            sql_arg = node.args[0]
            if not (self._is_aq_wrapped(sql_arg) or self._is_known_adapted_name(sql_arg)):
                self.issues.append(
                    (
                        node.lineno,
                        "Parameterized SQL should use aq(...) directly or an aq-adapted SQL variable",
                    )
                )

        self.generic_visit(node)


def _iter_runtime_sql_files():
    for path in RUNTIME_SQL_PATHS:
        if path.is_dir():
            yield from sorted(path.rglob("*.py"))
        elif path.is_file():
            yield path


def test_runtime_parameterized_sql_uses_adapt_query():
    issues: list[str] = []

    for path in _iter_runtime_sql_files():
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        visitor = ParameterizedSqlVisitor()
        visitor.visit(tree)

        for lineno, message in visitor.issues:
            rel_path = path.relative_to(PROJECT_ROOT)
            issues.append(f"{rel_path}:{lineno}: {message}")

    assert not issues, "Found runtime SQL calls without aq(...):\n" + "\n".join(issues)


def test_requirements_include_psycopg2_binary():
    requirements = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8")
    lines = {line.strip() for line in requirements.splitlines() if line.strip() and not line.strip().startswith("#")}

    assert any(line.startswith("psycopg2-binary") for line in lines)
