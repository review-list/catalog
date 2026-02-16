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
TPL_SEARCH = "search.html"


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"JSONが見つかりません: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


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


def chunk(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def sort_works_newest_first(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # release_date は文字列でOK（YYYY-MM-DD HH:MM:SS想定）
    return sorted(works, key=lambda x: (x.get("release_date") or ""), reverse=True)


def make_search_index(works: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 検索に必要な最小限だけ（軽量）
    out: List[Dict[str, Any]] = []
    for w in works:
        wid = w.get("id")
        if not wid:
            continue
        out.append({
            "id": str(wid),
            "title": w.get("title") or "",
            "release_date": w.get("release_date") or "",
            "hero_image": w.get("hero_image") or "",
            "official_url": w.get("official_url") or "",
            "actresses": w.get("actresses") or [],
            "tags": w.get("tags") or [],
        })
    return out
def count_top_tags_and_actresses(works: List[Dict[str, Any]], top_n: int = 30):
    tag_count: Dict[str, int] = {}
    actress_count: Dict[str, int] = {}

    for w in works:
        for t in (w.get("tags") or []):
            if t:
                tag_count[t] = tag_count.get(t, 0) + 1
        for a in (w.get("actresses") or []):
            if a:
                actress_count[a] = actress_count.get(a, 0) + 1

    # 件数が多い順 → 同数なら日本語順
    top_tags = sorted(tag_count.items(), key=lambda x: (-x[1], x[0].lower()))[:top_n]
    top_actresses = sorted(actress_count.items(), key=lambda x: (-x[1], x[0].lower()))[:top_n]

    return tag_count, actress_count, top_tags, top_actresses


def write_search_index_chunks(
    works_sorted: List[Dict[str, Any]],
    out_assets_dir: Path,
    chunk_size: int = 2000,
) -> None:
    """
    超大量対応：
    - works_index_manifest.json … 目次（タグ/女優一覧・人気TOP・チャンク一覧）
    - works_index_000.json, works_index_001.json ... … 分割データ
    - works_index.json …（互換用）全件1ファイルも残す（任意）
    """
    search_index = make_search_index(works_sorted)

    # 1) 互換用（小～中規模ならこれだけでもOK）
    write_json(out_assets_dir / "works_index.json", search_index)

    # 2) 分割
    chunks = [search_index[i:i + chunk_size] for i in range(0, len(search_index), chunk_size)]
    chunk_files = []
    for i, ch in enumerate(chunks):
        name = f"works_index_{i:03d}.json"
        write_json(out_assets_dir / name, ch)
        chunk_files.append(name)

    # 3) manifest（検索ページが最初に読む）
    tag_count, actress_count, top_tags, top_actresses = count_top_tags_and_actresses(works_sorted, top_n=40)

    manifest = {
        "version": 1,
        "total_items": len(search_index),
        "chunk_size": chunk_size,
        "chunk_files": chunk_files,
        "all_tags": sorted(tag_count.keys(), key=lambda s: s.lower()),
        "all_actresses": sorted(actress_count.keys(), key=lambda s: s.lower()),
        "top_tags": [{"name": k, "count": v} for k, v in top_tags],
        "top_actresses": [{"name": k, "count": v} for k, v in top_actresses],
    }
    write_json(out_assets_dir / "works_index_manifest.json", manifest)


def main():
    data = load_json(WORKS_JSON)
    site_name = data.get("site_name", "Catalog")
    works: List[Dict[str, Any]] = data.get("works", [])

    works_sorted = sort_works_newest_first(works)

    ensure_dir(OUT / "works")
    ensure_dir(OUT / "actresses")
    ensure_dir(OUT / "genres")
    ensure_dir(OUT / "pages")
    ensure_dir(OUT / "search")
    ensure_dir(OUT / "assets")

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATES)),
        autoescape=select_autoescape(["html", "xml"]),
    )

    tpl_index = safe_template(env, TPL_INDEX)
    tpl_page = safe_template(env, TPL_PAGE)
    tpl_list = safe_template(env, TPL_LIST, fallback=TPL_INDEX)
    tpl_search = safe_template(env, TPL_SEARCH)

    actresses_map, actresses_keys, genres_map, genres_keys = build_indexes_from_works(works_sorted)

    # =============================
    # CSS相対パス
    # =============================
    CSS_ROOT = "assets/style.css"
    CSS_1DOWN = "../assets/style.css"
    CSS_2DOWN = "../../assets/style.css"
    CSS_3DOWN = "../../../assets/style.css"

    # =============================
    # A) 検索用の軽量JSONを出力
    # =============================
    search_index = make_search_index(works_sorted)
    write_json(OUT / "assets" / "works_index.json", search_index)

    # =============================
    # 1) トップページ：最新100件だけ（軽量）
    # =============================
    TOP_LIMIT = 100
    top_works = works_sorted[:TOP_LIMIT]

    write_text(
        OUT / "index.html",
        tpl_index.render(
            site_name=site_name,
            works=top_works,
            actresses_keys=actresses_keys,   # 使うテンプレなら活用可（無くてもOK）
            genres_keys=genres_keys,
            css_path=CSS_ROOT,
            home_href="./",
            actresses_href="actresses/",
            genres_href="genres/",
            works_prefix="works/",
            # 追加導線（テンプレ側で使いたければ）
            pages_href="pages/1/",
            search_href="search/",
            note=f"トップは最新{TOP_LIMIT}件のみ表示（重くならない設計）",
        ),
    )

    # =============================
    # 1.5) 検索ページ（JSON検索）
    # =============================
    write_text(
        OUT / "search" / "index.html",
        tpl_search.render(
            site_name=site_name,
            css_path=CSS_1DOWN,
            home_href="../",
            pages_href="../pages/1/",
            actresses_href="../actresses/",
            genres_href="../genres/",
            data_url="../assets/works_index.json",
        ),
    )

    # =============================
    # 1.6) 全作品ページ（ページネーション 50件/ページ）
    # =============================
    PER_PAGE = 50
    pages = chunk(works_sorted, PER_PAGE)
    total_pages = max(1, len(pages))

    for i, page_works in enumerate(pages, start=1):
        # pages/<n>/index.html
        prev_href = f"../{i-1}/" if i > 1 else None
        next_href = f"../{i+1}/" if i < total_pages else None

        # list_works.html がある前提（無ければ index.html にfallback）
        write_text(
            OUT / "pages" / str(i) / "index.html",
            tpl_list.render(
                site_name=site_name,
                page_title=f"全作品（{i}/{total_pages}）",
                page_description=f"全作品一覧です（{PER_PAGE}件/ページ）。",
                # listテンプレは items を想定している場合があるので、
                # ここでは index.html fallback を見越して works も渡す
                works=page_works,
                items=[],  # list_works.htmlがitemsベースでも壊れにくくする
                css_path=CSS_2DOWN,
                home_href="../../",
                actresses_href="../../actresses/",
                genres_href="../../genres/",
                works_prefix="../../works/",
                pages_href="../1/",
                search_href="../../search/",
                page=i,
                total_pages=total_pages,
                prev_href=prev_href,
                next_href=next_href,
            ),
        )

    # =============================
    # 2) 作品個別ページ（関連作品）
    # =============================
    works_by_id: dict[str, dict] = {}
    for ww in works_sorted:
        if isinstance(ww, dict) and ww.get("id"):
            works_by_id[str(ww["id"])] = ww

    actress_to_ids: dict[str, list[str]] = {}
    for ww in works_sorted:
        wid = ww.get("id")
        if not wid:
            continue
        wid = str(wid)
        for a in (ww.get("actresses") or []):
            if not a:
                continue
            actress_to_ids.setdefault(a, []).append(wid)

    def get_related_works(current_work: dict, limit: int = 12) -> list[dict]:
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

    for w in works_sorted:
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
                pages_href="../../pages/1/",
                search_href="../../search/",
            ),
        )

    # =============================
    # 3) 女優一覧ページ（リンク一覧）
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
            pages_href="../pages/1/",
            search_href="../search/",
        ),
    )

    # 4) 女優個別ページ
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
                pages_href="../../pages/1/",
                search_href="../../search/",
            ),
        )

    # =============================
    # 5) ジャンル一覧ページ（リンク一覧）
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
            pages_href="../pages/1/",
            search_href="../search/",
        ),
    )

    # 6) ジャンル個別ページ
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
                pages_href="../../pages/1/",
                search_href="../../search/",
            ),
        )

    print("生成完了：docs/ に出力しました")
    print(" - docs/index.html (最新100件)")
    print(" - docs/pages/<n>/index.html (50件/ページ)")
    print(" - docs/search/index.html (JSON検索)")
    print(" - docs/assets/works_index.json (検索用JSON)")


if __name__ == "__main__":
    main()

