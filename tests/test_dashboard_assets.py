"""档案终端构建产物（pages/dashboard）的检查：两套前端分开加载，且能通过 AstrBot 插件页面的地址改写。

AstrBot 只给 HTML 的 src/href、CSS 的 url() 和 JS 里写死的 import 地址补 asset_token，
运行时拼出来的请求（预加载 <link>、动态 chunk 自带的 CSS）没有令牌，会被拒绝；
所以产物必须满足这些条件，改前端构建配置后要重新构建并过这几项。
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest

DASHBOARD = Path(__file__).resolve().parents[1] / "pages" / "dashboard"
ASSETS = DASHBOARD / "assets"
# 所有写死的相对模块地址（含动态 import("./x.js")）；STATIC_ONLY 只含静态引用
ANY_IMPORT = re.compile(r"""(?:\bfrom|\bimport)\s*\(?\s*["'](\.{1,2}/[^"']+)["']""")
STATIC_ONLY = re.compile(r"""(?:\bfrom|\bimport)\s+["'](\.{1,2}/[^"']+)["']""")


def _chunks() -> dict[str, str]:
    return {p.name: p.read_text(encoding="utf-8") for p in ASSETS.glob("*.js")}


def test_entry_has_single_stylesheet_and_no_runtime_preload():
    html = (DASHBOARD / "index.html").read_text(encoding="utf-8")
    assert len(re.findall(r'<link rel="stylesheet"', html)) == 1
    assert len(re.findall(r'<script type="module"', html)) == 1
    # 动态 chunk 不能带独立 CSS，否则运行时拼 <link> 加载会丢令牌
    assert len(list(ASSETS.glob("*.css"))) == 1
    for name, code in _chunks().items():
        assert "modulepreload" not in code, name
        assert 'createElement("link")' not in code, name
        assert 'createElement("script")' not in code, name


def test_normal_frontend_does_not_pull_soul_assets():
    chunks = _chunks()
    normal = next(n for n, c in chunks.items() if "normal-styles" in c)
    soul = next(n for n, c in chunks.items() if "WebGLRenderer" in c)
    seen: set[str] = set()
    todo = [normal]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        todo += [s.rsplit("/", 1)[-1] for s in STATIC_ONLY.findall(chunks[name])]
    assert soul not in seen
    for name in seen:
        code = chunks[name]
        assert "WebGLRenderer" not in code and ".glb" not in code, name
        assert "fonts/MiSans" not in code, name


ASTRBOT_SOURCE = os.environ.get("ASTRBOT_SOURCE_PATH")


@pytest.mark.skipif(
    not ASTRBOT_SOURCE or not Path(ASTRBOT_SOURCE).is_dir(),
    reason="需要 ASTRBOT_SOURCE_PATH 指向 AstrBot 源码",
)
def test_every_relative_import_gets_asset_token_from_real_rewriter():
    if str(ASTRBOT_SOURCE) not in sys.path:
        sys.path.insert(0, str(ASTRBOT_SOURCE))
    from astrbot.dashboard.services.plugin_page_service import PluginPageService

    if not hasattr(PluginPageService, "rewrite_plugin_page_js"):
        # 4.29.0-beta.2 起令牌放进页面路径，服务端不再改写 JS，相对引用自然带上令牌
        pytest.skip("这个 AstrBot 版本不再改写插件页面的 JS")
    service = PluginPageService.__new__(PluginPageService)
    total = 0
    for path in ASSETS.glob("*.js"):
        code = path.read_text(encoding="utf-8")
        rewritten = service.rewrite_plugin_page_js(
            code,
            "astrbot_plugin_astrhe",
            "dashboard",
            f"assets/{path.name}",
            {"asset_token": "TESTTOKEN"},
        )
        for spec in ANY_IMPORT.findall(code):
            total += 1
            name = re.escape(spec.rsplit("/", 1)[-1])
            assert re.search(name + r"\?[^\"']*asset_token=TESTTOKEN", rewritten), (
                path.name,
                spec,
            )
    # 入口、两套前端和它们的共享 chunk 之间至少有这些静态 / 动态引用
    assert total >= 4


def _glb_json(path: Path) -> dict:
    import json
    import struct

    data = _unpack(path)
    length = struct.unpack("<I", data[12:16])[0]
    return json.loads(data[20 : 20 + length])


def _unpack(path: Path) -> bytes:
    import shutil
    import subprocess

    if shutil.which("node") is None:
        pytest.skip("需要 Node.js 验证前端资源的无损解压")
    return subprocess.check_output(
        [
            "node",
            "--input-type=module",
            "-e",
            "import{readFileSync}from'node:fs';"
            "import{brotliDecompressSync}from'node:zlib';"
            "process.stdout.write(brotliDecompressSync(readFileSync(process.argv[1])))",
            str(path),
        ]
    )


def test_models_are_meshopt_compressed_and_decoder_is_bundled():
    import hashlib

    models = sorted(ASSETS.glob("archive-*.glb.br"))
    assert [p.name.split(".")[0] for p in models] == [
        "archive-assembly",
        "archive-cassette",
    ]
    # 还原后与此前已验证过的 meshopt GLB 完全相同，烘焙顶点和查看器输入均不变。
    expected = {
        "archive-assembly": "b49bb886d76cd207e62fd8d85847e4c63a040af8415b0a6771c65de70898fd8a",
        "archive-cassette": "d5e6ee6bcab61999c62b79064811214b9c0a84f653f6cb00c20e41bcda8a11ef",
    }
    for path in models:
        assert (
            hashlib.sha256(_unpack(path)).hexdigest()
            == expected[path.name.split(".")[0]]
        )
        gltf = _glb_json(path)
        assert "EXT_meshopt_compression" in gltf.get("extensionsRequired", []), path
        # 无损压缩：不量化，所以不需要 KHR_mesh_quantization
        assert "KHR_mesh_quantization" not in gltf.get("extensionsUsed", []), path
    licenses = (DASHBOARD / "licenses" / "third-party.txt").read_text(encoding="utf-8")
    for name in (
        "three@",
        "meshoptimizer@",
        "mdui@",
        "lit-html@",
        "material-color-utilities@",
        "brotli-dec-wasm@2.3.2",
    ):
        assert name in licenses, name
    loaders = [name for name, code in _chunks().items() if ".glb" in code]
    assert loaders and all("setMeshoptDecoder" in _chunks()[n] for n in loaders)


def test_complete_variable_font_roundtrips_without_removing_characters():
    import hashlib

    fonts = list((DASHBOARD / "fonts").glob("MiSansVF.*.ttf.br"))
    assert len(fonts) == 1
    assert not list((DASHBOARD / "fonts").glob("*.woff2"))
    restored = _unpack(fonts[0])
    source = DASHBOARD.parents[1] / "frontend" / "fonts" / "MiSansVF.ttf"
    assert restored == source.read_bytes()
    assert hashlib.sha256(restored).hexdigest() == (
        "0ddef90648998900175cfdca9a6f087a2544c182f130b0ad4f7e94a03a115e79"
    )
    assert len(list(ASSETS.glob("*.wasm"))) == 1
    assert (DASHBOARD / "fonts" / "MiSans-license.pdf").is_file()


def test_worktree_install_zip_is_below_sixteen_decimal_megabytes(tmp_path):
    import shutil
    import subprocess

    root = DASHBOARD.parents[1]
    if shutil.which("git") is None or not (root / ".git").exists():
        pytest.skip("需要 git 按实际发布规则生成 ZIP")
    # 临时索引只收录本次安装包的运行文件，不改变用户暂存区，也不提交。
    env = {**os.environ, "GIT_INDEX_FILE": str(tmp_path / "index")}
    subprocess.run(["git", "read-tree", "HEAD"], cwd=root, env=env, check=True)
    subprocess.run(
        [
            "git",
            "add",
            "--all",
            "--",
            ".gitattributes",
            "pages/dashboard",
            "README.md",
            "CHANGELOG.md",
            "frontend/LICENSE-RhineLabUI",
            "frontend/THIRD_PARTY_NOTICES.md",
        ],
        cwd=root,
        env=env,
        check=True,
    )
    tree = subprocess.check_output(
        ["git", "write-tree"],
        cwd=root,
        env=env,
        text=True,
    ).strip()
    archive = subprocess.check_output(
        ["git", "archive", "--format=zip", "--prefix=AstrHe/", tree],
        cwd=root,
    )
    # 按更严格的十进制 16 MB 留足余量，同样适用于 16 MiB 限制。
    assert len(archive) < 16_000_000, f"安装包 {len(archive):,} 字节，超过 16 MB"


def test_install_archive_skips_dev_sources_but_keeps_notices():
    import shutil
    import subprocess
    import tarfile

    root = DASHBOARD.parents[1]
    if shutil.which("git") is None or not (root / ".git").exists():
        pytest.skip("需要在 git 仓库里检查 .gitattributes")
    # 按工作区的 .gitattributes 打包 HEAD，和 GitHub 生成源码压缩包的规则一致
    proc = subprocess.Popen(
        ["git", "archive", "--worktree-attributes", "--format=tar", "HEAD"],
        cwd=root,
        stdout=subprocess.PIPE,
    )
    with tarfile.open(fileobj=proc.stdout, mode="r|") as tar:
        names = {member.name for member in tar}
    proc.wait()
    assert proc.returncode == 0
    assert not any(n.startswith("tests") for n in names)
    assert {n for n in names if n.startswith("frontend/") and not n.endswith("/")} == {
        "frontend/LICENSE-RhineLabUI",
        "frontend/THIRD_PARTY_NOTICES.md",
    }
    assert "main.py" in names and "metadata.yaml" in names and "logo.png" in names
    assert "logo-full.png" not in names
    assert any(n.startswith("pages/dashboard/fonts/") for n in names)
