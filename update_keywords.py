#!/usr/bin/env python3
"""一键替换 config.json 里的 keywords 为标准 59 词根（其他字段不动）。

用法:
    python3 update_keywords.py                    # 默认改 ./config.json
    python3 update_keywords.py /path/to/config.json
"""
import json
import sys
from pathlib import Path

KEYWORDS = [
    "Translator", "Generator", "Example", "Convert", "Online",
    "Downloader", "Maker", "Creator", "Editor", "Processor",
    "Designer", "Compiler", "Analyzer", "Evaluator", "Sender",
    "Receiver", "Interpreter", "Uploader", "Calculator", "Sample",
    "Template", "Format", "Builder", "Scheme", "Pattern",
    "Checker", "Detector", "Scraper", "Manager", "Explorer",
    "Dashboard", "Planner", "Tracker", "Recorder", "Optimizer",
    "Scheduler", "Converter", "Viewer", "Extractor", "Monitor",
    "Notifier", "Verifier", "Simulator", "Assistant", "Constructor",
    "Comparator", "Navigator", "Syncer", "Connector", "Cataloger",
    "Responder",
    "music", "Lyrics", "video", "audio", "Image",
    "music Generator", "video Generator", "Image Generator",
]


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "config.json")
    if not path.exists():
        print(f"❌ 文件不存在: {path}")
        sys.exit(1)

    config = json.loads(path.read_text(encoding="utf-8"))
    old_count = len([k for k in config.get("keywords", []) if not k.startswith("__")])
    config["keywords"] = KEYWORDS
    path.write_text(json.dumps(config, indent=4, ensure_ascii=False) + "\n",
                    encoding="utf-8")
    print(f"✅ {path}")
    print(f"   旧词根数: {old_count} → 新词根数: {len(KEYWORDS)}")
    print(f"   前3个: {KEYWORDS[:3]}")
    print(f"   后3个: {KEYWORDS[-3:]}")


if __name__ == "__main__":
    main()
