from pathlib import Path
import json
from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parent            # src/
TEMPLATES_DIR = ROOT / "templates"
DATA_PATH = ROOT / "data" / "works.json"
DIST = ROOT.parent / "docs"                       # GitHub Pagesの公開フォルダ

def write_text(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def main():
    data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    site_name = data.get("site_name", "テスト用 静的サイト")
    works = data.get("works", [])

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        autoescape=select_autoescape(["html", "xml"]),
    )

    # 1) トップページ（一覧）
    tpl_index = env.get_template("index.html")
    html_index = tpl_index.render(site_name=site_name, works=works)
    write_text(DIST / "index.html", html_index)

    # 2) 作品ページ（/works/<id>/index.html）
    tpl_work = env.get_template("work.html")
    for w in works:
        # 作品ページに site_name を渡す
        ctx = {**w, "site_name": site_name}
        out = DIST / "works" / w["id"] / "index.html"
        html_work = tpl_work.render(**ctx)
        write_text(out, html_work)

    print(f"OK: generated {len(works)} works")
    print(f"TOP: {DIST / 'index.html'}")

if __name__ == "__main__":
    main()
