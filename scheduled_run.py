"""
定时抓取 Google Trends 爆增词并推送通知

使用方式:
  1. 复制 config_example.json 为 config.json，填入你的配置
  2. 运行: python scheduled_run.py
  3. 定时任务: crontab -e 添加:
     0 9 * * 1,4 /path/to/.venv/bin/python /path/to/scheduled_run.py

支持通知渠道: 飞书 / 企业微信 / Server酱(个人微信) / pushplus(个人微信)
"""

import json
import time
import random
import os
import sys
import requests as http_requests
from datetime import datetime
from pathlib import Path

# 把项目目录加入 path，确保能 import pytrends
sys.path.insert(0, str(Path(__file__).parent))

from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

# ── 配置 ──────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config():
    if not CONFIG_PATH.exists():
        print(f"配置文件不存在: {CONFIG_PATH}")
        print("请复制 config_example.json 为 config.json 并填入配置")
        sys.exit(1)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# ── 数据抓取 ──────────────────────────────────────────────────
def fetch_rising_queries(config):
    """抓取所有关键词的 rising queries"""
    kw_list = [kw.strip() for kw in config["keywords"] if kw.strip()]
    geo = config.get("geo", "")
    cat = config.get("category", 0)
    timeframe = config.get("timeframe", "now 7-d")
    interval = config.get("request_interval", 8)

    all_rising = []
    failed = []

    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

    for i, kw in enumerate(kw_list):
        print(f"[{i+1}/{len(kw_list)}] 查询: {kw}")

        retry_count = 0
        while retry_count < 3:
            try:
                pytrend.build_payload(kw_list=[kw], cat=cat, timeframe=timeframe, geo=geo)
                result = pytrend.related_queries()

                if kw in result and result[kw]['rising'] is not None:
                    rising_df = result[kw]['rising'].copy()
                    rising_df['keyword'] = kw
                    all_rising.append(rising_df)
                    print(f"  -> 找到 {len(rising_df)} 个上升词")
                else:
                    print(f"  -> 无上升词")
                break

            except TooManyRequestsError:
                retry_count += 1
                wait = 60 + retry_count * 30 + random.randint(0, 10)
                print(f"  ⚠️ 429 限流，等待 {wait}s 后重试 ({retry_count}/3)")
                time.sleep(wait)
                pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

            except ResponseError as e:
                retry_count += 1
                wait = 30 + retry_count * 15
                print(f"  ⚠️ 错误: {e}，等待 {wait}s ({retry_count}/3)")
                time.sleep(wait)

            except Exception as e:
                print(f"  ❌ 失败: {e}")
                failed.append(kw)
                break
        else:
            failed.append(kw)
            print(f"  ❌ 重试耗尽，跳过")

        if i < len(kw_list) - 1:
            time.sleep(interval + random.uniform(0, 2))

    return all_rising, failed


def analyze_spikes(all_rising, config):
    """分析趋势，判断新词/近日飙升"""
    import numpy as np
    import pandas as pd

    if not all_rising:
        return pd.DataFrame(), {}

    combined = pd.concat(all_rising, ignore_index=True)
    combined['value_num'] = pd.to_numeric(combined['value'], errors='coerce')

    # 取 top N 做趋势验证
    spike_top_n = config.get("spike_top_n", 20)
    temp = combined.copy()
    max_v = temp['value_num'].max()
    temp.loc[temp['value_num'].isna(), 'value_num'] = (max_v * 2) if pd.notna(max_v) else 999999
    top_queries = temp.nlargest(spike_top_n, 'value_num')['query'].unique().tolist()

    geo = config.get("geo", "")
    cat = config.get("category", 0)
    interval = config.get("request_interval", 8)
    spike_results = {}

    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
    batches = [top_queries[i:i+5] for i in range(0, len(top_queries), 5)]

    for bi, batch in enumerate(batches):
        print(f"验证趋势 [{bi+1}/{len(batches)}]: {', '.join(batch[:3])}...")

        try:
            pytrend.build_payload(kw_list=batch, cat=cat, timeframe='now 7-d', geo=geo)
            iot = pytrend.interest_over_time()

            if not iot.empty:
                for q in batch:
                    if q in iot.columns:
                        series = iot[q].values.astype(float)
                        if len(series) < 4:
                            continue
                        split = max(1, len(series) * 2 // 3)
                        early_avg = np.mean(series[:split])
                        late_avg = np.mean(series[split:])
                        peak_in_late = np.argmax(series) >= split

                        if early_avg < 1:
                            pattern = '新词飙升'
                        elif late_avg > early_avg * 2 and peak_in_late:
                            pattern = '近日飙升'
                        elif late_avg > early_avg * 1.3:
                            pattern = '持续上升'
                        else:
                            pattern = '平稳'

                        spike_results[q] = pattern

        except Exception as e:
            print(f"  验证失败: {e}")

        if bi < len(batches) - 1:
            time.sleep(interval + random.uniform(0, 2))

    # 合并趋势标签
    combined['趋势'] = combined['query'].map(lambda q: spike_results.get(q, ''))

    return combined, spike_results


# ── 保存结果 ──────────────────────────────────────────────────
def save_csv(combined):
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)
    filename = f"trending_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = output_dir / filename

    display = combined[['query', 'value', '趋势', 'keyword']].copy()
    display['增长量'] = display['value'].apply(lambda v: f'+{v}%' if str(v).isdigit() else '飙升')
    display = display.rename(columns={'query': '爆词', 'keyword': '来源词根'})
    display = display[['爆词', '增长量', '趋势', '来源词根']]
    display = display.sort_values(
        by='增长量',
        key=lambda s: s.str.replace(r'[+%]', '', regex=True).apply(
            lambda x: float(x) if x.replace('.', '').isdigit() else float('inf')
        ),
        ascending=False
    )
    display.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"结果已保存: {filepath}")
    return filepath, display


# ── 构建通知消息 ──────────────────────────────────────────────
def build_message(combined, spike_results, failed):
    """构建通知消息文本"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = len(combined)
    sources = combined['keyword'].nunique()
    new_count = sum(1 for v in spike_results.values() if v == '新词飙升')
    spike_count = sum(1 for v in spike_results.values() if v == '近日飙升')

    lines = [
        f"🔥 热点关键词趋势报告",
        f"📅 {now}",
        f"",
        f"共找到 {total} 个上升词，来自 {sources} 个词根",
    ]

    if new_count > 0:
        lines.append(f"✨ {new_count} 个新词飙升")
    if spike_count > 0:
        lines.append(f"🔥 {spike_count} 个近日飙升")

    # Top 15
    lines.append("")
    lines.append("📊 Top 15 爆增词:")
    lines.append("")

    import pandas as pd
    combined['_sort'] = pd.to_numeric(combined['value'], errors='coerce')
    max_v = combined['_sort'].max()
    combined.loc[combined['_sort'].isna(), '_sort'] = (max_v * 2) if pd.notna(max_v) else 999999
    top = combined.nlargest(15, '_sort')

    for _, row in top.iterrows():
        val = row['value']
        growth = f'+{val}%' if str(val).isdigit() else '飙升'
        trend_tag = ''
        if row.get('趋势') == '新词飙升':
            trend_tag = ' ✨新词'
        elif row.get('趋势') == '近日飙升':
            trend_tag = ' 🔥飙升'
        lines.append(f"  {row['query']}  ({growth}){trend_tag}  ← {row['keyword']}")

    if failed:
        lines.append("")
        lines.append(f"⚠️ {len(failed)} 个词查询失败: {', '.join(failed[:5])}")

    combined.drop(columns=['_sort'], inplace=True)
    return "\n".join(lines)


# ── 飞书通知 ──────────────────────────────────────────────────
def send_feishu(webhook_url, message):
    """发送飞书群机器人通知"""
    payload = {
        "msg_type": "text",
        "content": {"text": message}
    }
    resp = http_requests.post(webhook_url, json=payload, timeout=10)
    if resp.status_code == 200:
        print("✅ 飞书通知发送成功")
    else:
        print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")


def send_feishu_rich(webhook_url, message, combined, spike_results):
    """发送飞书富文本通知（带格式）"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = len(combined)
    sources = combined['keyword'].nunique()
    new_count = sum(1 for v in spike_results.values() if v == '新词飙升')
    spike_count = sum(1 for v in spike_results.values() if v == '近日飙升')

    import pandas as pd
    combined['_sort'] = pd.to_numeric(combined['value'], errors='coerce')
    max_v = combined['_sort'].max()
    combined.loc[combined['_sort'].isna(), '_sort'] = (max_v * 2) if pd.notna(max_v) else 999999
    top = combined.nlargest(15, '_sort')

    # 构建富文本内容
    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n共找到 {total} 个上升词，来自 {sources} 个词根"}],
    ]

    if new_count > 0 or spike_count > 0:
        summary = ""
        if new_count > 0:
            summary += f"✨ {new_count} 个新词飙升  "
        if spike_count > 0:
            summary += f"🔥 {spike_count} 个近日飙升"
        content_lines.append([{"tag": "text", "text": summary}])

    content_lines.append([{"tag": "text", "text": "\n📊 Top 15 爆增词:"}])

    for _, row in top.iterrows():
        val = row['value']
        growth = f'+{val}%' if str(val).isdigit() else '飙升'
        trend_tag = ''
        if row.get('趋势') == '新词飙升':
            trend_tag = ' ✨新词'
        elif row.get('趋势') == '近日飙升':
            trend_tag = ' 🔥飙升'
        content_lines.append([
            {"tag": "text", "text": f"{row['query']}  "},
            {"tag": "text", "text": f"({growth}){trend_tag}"},
            {"tag": "text", "text": f"  ← {row['keyword']}"},
        ])

    combined.drop(columns=['_sort'], inplace=True)

    payload = {
        "msg_type": "post",
        "content": {
            "post": {
                "zh_cn": {
                    "title": "🔥 热点关键词趋势报告",
                    "content": content_lines,
                }
            }
        }
    }
    resp = http_requests.post(webhook_url, json=payload, timeout=10)
    if resp.status_code == 200:
        print("✅ 飞书通知发送成功")
    else:
        print(f"❌ 飞书通知失败: {resp.status_code} {resp.text}")


# ── 企业微信通知 ──────────────────────────────────────────────
def send_wecom(webhook_url, message):
    """发送企业微信群机器人通知"""
    # 企业微信 markdown 限制 4096 字节，超长截断
    if len(message.encode('utf-8')) > 4000:
        message = message[:1300] + "\n\n... (更多结果请查看 CSV 文件)"

    payload = {
        "msgtype": "text",
        "text": {"content": message}
    }
    resp = http_requests.post(webhook_url, json=payload, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("errcode") == 0:
            print("✅ 企业微信通知发送成功")
        else:
            print(f"❌ 企业微信通知失败: {data}")
    else:
        print(f"❌ 企业微信通知失败: {resp.status_code}")


# ── Server酱 (个人微信) ──────────────────────────────────────
def send_serverchan(sendkey, title, message):
    """通过 Server酱 推送到个人微信
    注册: https://sct.ftqq.com/
    """
    url = f"https://sctapi.ftqq.com/{sendkey}.send"
    payload = {"title": title, "desp": message.replace("\n", "\n\n")}
    resp = http_requests.post(url, data=payload, timeout=10)
    if resp.status_code == 200:
        print("✅ Server酱通知发送成功")
    else:
        print(f"❌ Server酱通知失败: {resp.status_code} {resp.text}")


# ── pushplus (个人微信) ──────────────────────────────────────
def send_pushplus(token, title, message):
    """通过 pushplus 推送到个人微信
    注册: https://www.pushplus.plus/
    """
    url = "https://www.pushplus.plus/send"
    payload = {
        "token": token,
        "title": title,
        "content": message.replace("\n", "<br>"),
        "template": "html",
    }
    resp = http_requests.post(url, json=payload, timeout=10)
    if resp.status_code == 200:
        print("✅ pushplus 通知发送成功")
    else:
        print(f"❌ pushplus 通知失败: {resp.status_code} {resp.text}")


# ── 发送通知 ──────────────────────────────────────────────────
def send_notifications(config, message, combined, spike_results):
    """根据配置发送所有通知"""
    notify = config.get("notify", {})

    # 飞书
    feishu_url = notify.get("feishu_webhook", "")
    if feishu_url:
        send_feishu_rich(feishu_url, message, combined, spike_results)

    # 企业微信
    wecom_url = notify.get("wecom_webhook", "")
    if wecom_url:
        send_wecom(wecom_url, message)

    # Server酱
    serverchan_key = notify.get("serverchan_sendkey", "")
    if serverchan_key:
        send_serverchan(serverchan_key, "🔥 热点关键词趋势报告", message)

    # pushplus
    pushplus_token = notify.get("pushplus_token", "")
    if pushplus_token:
        send_pushplus(pushplus_token, "🔥 热点关键词趋势报告", message)

    if not any([feishu_url, wecom_url, serverchan_key, pushplus_token]):
        print("⚠️ 未配置任何通知渠道，仅保存 CSV")


# ── 主流程 ────────────────────────────────────────────────────
def main():
    print("=" * 50)
    print(f"🔥 热点关键词趋势追踪 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    config = load_config()

    # 1. 抓取
    print("\n📡 开始抓取 rising queries...")
    all_rising, failed = fetch_rising_queries(config)

    if not all_rising:
        print("❌ 未获取到任何数据")
        return

    # 2. 分析趋势
    print("\n📊 分析趋势...")
    combined, spike_results = analyze_spikes(all_rising, config)

    # 3. 保存 CSV
    print("\n💾 保存结果...")
    csv_path, display_df = save_csv(combined)

    # 4. 发送通知
    print("\n📮 发送通知...")
    message = build_message(combined, spike_results, failed)
    send_notifications(config, message, combined, spike_results)

    print(f"\n✅ 完成! 共 {len(combined)} 个上升词，CSV: {csv_path}")


if __name__ == "__main__":
    main()
