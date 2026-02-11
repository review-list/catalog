from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
TEMPLATES = SRC / "templates"
DATA_DIR = SRC / "data"
OUT = ROOT / "docs"

WORKS_JSON = DATA_DIR / "works.json"

TPL_INDEX = "index.html"
TPL_PAGE = "page.html"
TPL_LIST = "list_works.html"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSONが見つかりません: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def safe_template(env: Environment, name: str, fallback: str | None = None):
    try:
        return env.get_template(name)
    except Exception:
        if fallback:
            return env.get_template(fallback)
        raise


def slugify_simple(s: str) -> str:
    s = (s or "").strip()
    for ch in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
        s = s.replace(ch, "_")
    s = s.replace(" ", "_")
    return s or "unknown"


def build_indexes_from_works(works: List[Dict[str, Any]]):
    actresses: Dict[str, List[Dict[str, Any]]] = {}
    genres: Dict[str, List[Dict[str, Any]]] = {}

    for w in works:
        for a in (w.get("actresses") or []):
            actresses.setdefault(a, []).append(w)

        for g in (w.get("tags") or []):
            genres.setdefault(g, []).append(w)

    return actresses, sorted(actresses.keys()), genres, sorted(genres.keys())


def main():
    data = load_json(WORKS_JSON)
    site_name = data.get("site_name", "Catalog")
    works: List[Dict[str, Any]] = data.get("works", [])

    ensure_dir(OUT / "works")
    ensure_dir(OUT / "actresses")
    ensure_dir(OUT / "genres")

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        autoescape=select_autoescape(["html", "xml"]),
    )

    tpl_index = safe_template(env, TPL_INDEX)
    tpl_page = safe_template(env, TPL_PAGE)
    tpl_list = safe_template(env, TPL_LIST, fallback=TPL_INDEX)

    # =============================
    # ★女優/ジャンルの索引をここで作る（これが無いと actresess_keys 未定義）
    # =============================
    actresses_map, actresses_keys, genres_map, genres_keys = build_indexes_from_works(works)

    # =============================
    # CSS相対パス
    # =============================
    CSS_ROOT = "assets/style.css"
    CSS_1DOWN = "../assets/style.css"
    CSS_2DOWN = "../../assets/style.css"

    # =============================
    # 1) トップページ
    # =============================
    write_text(
        OUT / "index.html",
        tpl_index.render(
            site_name=site_name,
            works=works,
            actresses_keys=actresses_keys,   # ← 追加
            genres_keys=genres_keys,         # ← 追加
            css_path=CSS_ROOT,
            home_href="./",
            actresses_href="actresses/",
            genres_href="genres/",
            works_prefix="works/",
        ),
    )

    # =============================
    # 2) 作品個別ページ（関連作品つき）
    # =============================

    # 作品ID→作品 dict の辞書（参照用）
    works_by_id: dict[str, dict] = {}
    for ww in works:
        if isinstance(ww, dict) and ww.get("id"):
            works_by_id[str(ww["id"])] = ww

    # 女優名→作品IDのリスト（関連作品用）
    actress_to_ids: dict[str, list[str]] = {}
    for ww in works:
        if not isinstance(ww, dict):
            continue
        wid = ww.get("id")
        if not wid:
            continue
        wid = str(wid)
        for a in (ww.get("actresses") or []):
            if not a:
                continue
            actress_to_ids.setdefault(a, []).append(wid)

    def get_related_works(current_work: dict, limit: int = 12) -> list[dict]:
        """同じ女優の作品を集めて返す（自分自身は除外）"""
        cur_id = str(current_work.get("id") or "")
        cur_actresses = current_work.get("actresses") or []

        if not cur_actresses:
            return []

        related_ids: list[str] = []
        for a in cur_actresses:
            for wid in actress_to_ids.get(a, []):
                if wid == cur_id:
                    continue
                if wid not in related_ids:
                    related_ids.append(wid)

        related = [works_by_id[wid] for wid in related_ids if wid in works_by_id]
        related.sort(key=lambda x: (x.get("release_date") or ""), reverse=True)
        return related[:limit]

    for w in works:
        wid = w.get("id")
        if not wid:
            continue
        wid = str(wid)

        related_works = get_related_works(w, limit=12)

        write_text(
            OUT / "works" / wid / "index.html",
            tpl_page.render(
                site_name=site_name,
                w=w,
                related_works=related_works,
                css_path=CSS_2DOWN,
                home_href="../../",
                actresses_href="../../actresses/",
                genres_href="../../genres/",
                works_prefix="../../works/",
            ),
        )

    # =============================
    # 3) 女優一覧ページ
    # =============================
    write_text(
        OUT / "actresses" / "index.html",
        tpl_list.render(
            site_name=site_name,
            page_title="女優一覧",
            page_description="女優別の一覧ページです。",
            items=[{"name": a, "href": f"./{slugify_simple(a)}/"} for a in actresses_keys],
            css_path=CSS_1DOWN,
            home_href="../",
            actresses_href="./",
            genres_href="../genres/",
            works_prefix="../works/",
        ),
    )

    # =============================
    # 4) 女優個別ページ
    # =============================
    for a in actresses_keys:
        write_text(
            OUT / "actresses" / slugify_simple(a) / "index.html",
            tpl_index.render(
                site_name=site_name,
                works=actresses_map.get(a, []),
                css_path=CSS_2DOWN,
                home_href="../../",
                actresses_href="../",
                genres_href="../../genres/",
                works_prefix="../../works/",
            ),
        )

    # =============================
    # 5) ジャンル一覧ページ
    # =============================
    write_text(
        OUT / "genres" / "index.html",
        tpl_list.render(
            site_name=site_name,
            page_title="ジャンル一覧",
            page_description="タグ（ジャンル）別の一覧ページです。",
            items=[{"name": g, "href": f"./{slugify_simple(g)}/"} for g in genres_keys],
            css_path=CSS_1DOWN,
            home_href="../",
            actresses_href="../actresses/",
            genres_href="./",
            works_prefix="../works/",
        ),
    )

    # =============================
    # 6) ジャンル個別ページ
    # =============================
    for g in genres_keys:
        write_text(
            OUT / "genres" / slugify_simple(g) / "index.html",
            tpl_index.render(
                site_name=site_name,
                works=genres_map.get(g, []),
                css_path=CSS_2DOWN,
                home_href="../../",
                actresses_href="../../actresses/",
                genres_href="../",
                works_prefix="../../works/",
            ),
        )

    print("生成完了：docs/ に出力しました")


if __name__ == "__main__":
    main()
