import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import time
import random
import json
import requests as http_requests
from datetime import datetime
from pathlib import Path
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError, ResponseError

# ── 飞书通知 ──────────────────────────────────────────────────
def load_notify_config():
    config_path = Path(__file__).parent / "config.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg.get("notify", {})
    return {}

def send_feishu_notify(combined, spike_results=None):
    """查询完成后发送飞书通知"""
    notify = load_notify_config()
    webhook = notify.get("feishu_webhook", "")
    if not webhook:
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    total = len(combined)
    sources = combined['keyword'].nunique()

    # 排序取 Top 15
    df = combined.copy()
    df['_sort'] = pd.to_numeric(df['value'], errors='coerce')
    max_v = df['_sort'].max()
    df.loc[df['_sort'].isna(), '_sort'] = (max_v * 2) if pd.notna(max_v) else 999999
    top = df.nlargest(15, '_sort')

    content_lines = [
        [{"tag": "text", "text": f"📅 {now}\n共找到 {total} 个上升词，来自 {sources} 个词根"}],
    ]

    if spike_results:
        new_count = sum(1 for v in spike_results.values() if v.get('pattern') == '新词飙升')
        spike_count = sum(1 for v in spike_results.values() if v.get('pattern') == '近日飙升')
        parts = []
        if new_count > 0:
            parts.append(f"✨ {new_count} 个新词飙升")
        if spike_count > 0:
            parts.append(f"🔥 {spike_count} 个近日飙升")
        if parts:
            content_lines.append([{"tag": "text", "text": "  ".join(parts)}])

    content_lines.append([{"tag": "text", "text": "\n📊 Top 15 爆增词:"}])

    for _, row in top.iterrows():
        val = row['value']
        growth = f'+{val}%' if str(val).isdigit() else '飙升'
        trend_tag = ''
        if '趋势' in row and row['趋势'] == '新词飙升':
            trend_tag = ' ✨新词'
        elif '趋势' in row and row['趋势'] == '近日飙升':
            trend_tag = ' 🔥飙升'
        content_lines.append([
            {"tag": "text", "text": f"{row['query']}  ({growth}){trend_tag}  ← {row['keyword']}"},
        ])

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
    try:
        resp = http_requests.post(webhook, json=payload, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False

st.set_page_config(page_title="热点关键词趋势追踪", page_icon="🔥", layout="wide")

# 隐藏右上角英文菜单
st.markdown("""
<style>
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ── 左侧边栏设置 ──────────────────────────────────────────────
with st.sidebar:
    st.header("⚙ 查询设置")

    track_freq = st.selectbox("追踪频率", [
        ("每天 1 次", "daily_1"),
        ("每天 2 次", "daily_2"),
        ("每周 1 次", "weekly_1"),
        ("每周 2 次", "weekly_2"),
        ("每周 3 次", "weekly_3"),
        ("仅手动执行", "manual"),
    ], format_func=lambda x: x[0], index=0)

    freq = track_freq[1]
    if freq != 'manual':
        if freq == 'daily_2':
            st.markdown("**执行时间**")
            run_time_1 = st.time_input("第 1 次", value=pd.Timestamp("08:00").time(), key="t1")
            run_time_2 = st.time_input("第 2 次", value=pd.Timestamp("20:00").time(), key="t2")
            st.caption("建议早晚各一次，如 8:00 和 20:00")
        elif freq == 'daily_1':
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())
            st.caption("建议 8:00-10:00，数据更新及时")
        elif freq == 'weekly_1':
            run_day = st.selectbox("执行日", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=0)
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())
        elif freq == 'weekly_2':
            st.markdown("**执行日**")
            run_day_1 = st.selectbox("第 1 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=0, key="d1")
            run_day_2 = st.selectbox("第 2 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=3, key="d2")
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())
        elif freq == 'weekly_3':
            st.markdown("**执行日**")
            run_day_1 = st.selectbox("第 1 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=0, key="d1")
            run_day_2 = st.selectbox("第 2 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=2, key="d2")
            run_day_3 = st.selectbox("第 3 天", ["周一", "周二", "周三", "周四", "周五", "周六", "周日"], index=4, key="d3")
            run_time = st.time_input("执行时间", value=pd.Timestamp("09:00").time())

    st.divider()

    geo = st.selectbox("主要地区", [
        ("全球", ""),
        ("美国", "US"),
        ("中国", "CN"),
        ("日本", "JP"),
        ("英国", "GB"),
        ("德国", "DE"),
        ("法国", "FR"),
        ("韩国", "KR"),
        ("加拿大", "CA"),
        ("澳大利亚", "AU"),
        ("印度", "IN"),
        ("巴西", "BR"),
        ("新加坡", "SG"),
        ("中国香港", "HK"),
        ("中国台湾", "TW"),
    ], format_func=lambda x: x[0], index=0)

    category = st.selectbox("主要分类", [
        ("所有分类", 0),
        ("商业与工业", 12),
        ("计算机与电子产品", 5),
        ("互联网与电信", 13),
        ("购物", 18),
        ("健康", 45),
        ("新闻", 16),
        ("游戏", 8),
        ("金融", 7),
        ("食品与饮料", 71),
        ("旅行与交通", 67),
        ("体育", 20),
        ("娱乐", 3),
        ("科学", 174),
        ("教育", 958),
    ], format_func=lambda x: x[0], index=0)

    timeframe = st.selectbox("时间范围", [
        ("过去 7 天", "now 7-d"),
        ("过去 1 天", "now 1-d"),
        ("过去 30 天", "today 1-m"),
        ("过去 90 天", "today 3-m"),
        ("过去 12 个月", "today 12-m"),
    ], format_func=lambda x: x[0], index=0)

    request_interval = st.slider("请求间隔（秒）", min_value=2, max_value=300, value=5,
                                  help="每次请求之间的基础等待时间，优先级最高")

    max_rpm = st.slider("每分钟最大请求数", min_value=1, max_value=10, value=8,
                         help="频率上限，超过时自动暂停等待")

    st.divider()

    spike_check = st.checkbox("验证近日突然飙升", value=True,
                               help="对 Top 爆增词二次查询趋势曲线，判断是近几天突然飙升还是持续增长")
    spike_top_n = st.slider("验证词数量", min_value=5, max_value=50, value=20,
                             help="对增长最多的前 N 个词做趋势验证") if spike_check else 0

    st.divider()
    st.markdown("**限流说明**")
    st.markdown(
        "- 每次请求自动加入随机延迟\n"
        "- 触发 429 时自动指数退避重试\n"
        "- 超过频率上限时暂停等待\n\n"
        "如遇持续报错，请适当减小每分钟请求数或等待片刻后重试。"
    )

# ── 主区域 ────────────────────────────────────────────────────
st.title("🔥 热点关键词趋势追踪")
st.caption("输入一批关键词，自动查询每个词在 Google Trends 上近期增长最快的相关搜索词")

# 默认示例关键词
default_keywords = (
    "Translate, Generator, Example, Convert, Online, Downloader, "
    "Maker, Creator, Editor, Processor, Designer, Compiler, Analyzer, "
    "Evaluator, Sender, Receiver, Interpreter, Uploader, Calculator, "
    "Sample, Template, Format"
)

keywords_input = st.text_area(
    "输入关键词（用逗号分隔）",
    value=default_keywords,
    height=100,
    label_visibility="collapsed",
    placeholder="输入关键词，用逗号分隔，例如: Translate, Generator, Example ..."
)

# 解析关键词
import re
kw_list = [kw.strip() for kw in re.split(r'[,\n]+', keywords_input) if kw.strip()]
total_kw = len(kw_list)

if total_kw > 0:
    effective_interval = max(request_interval, 60.0 / max_rpm)
    est_time = total_kw * effective_interval / 60
    st.info(f"共 **{total_kw}** 个词根，预计用时约 **{est_time:.1f}** 分钟（间隔 {effective_interval:.0f} 秒）")

# 开始追踪按钮
start = st.button("🔍 开始追踪", type="primary", use_container_width=True)

# ── 执行逻辑 ──────────────────────────────────────────────────
if start and total_kw > 0:
    all_rising = []
    failed_kw = []
    effective_interval = max(request_interval, 60.0 / max_rpm)

    progress_bar = st.progress(0, text="准备开始...")
    status_area = st.empty()

    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

    for i, kw in enumerate(kw_list):
        progress = (i) / total_kw
        progress_bar.progress(progress, text=f"正在查询: **{kw}** ({i+1}/{total_kw})")

        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            try:
                pytrend.build_payload(
                    kw_list=[kw],
                    cat=category[1],
                    timeframe=timeframe[1],
                    geo=geo[1],
                )
                result = pytrend.related_queries()
                if kw in result and result[kw]['rising'] is not None:
                    rising_df = result[kw]['rising'].copy()
                    rising_df['keyword'] = kw
                    all_rising.append(rising_df)

                break  # 成功，跳出重试

            except TooManyRequestsError:
                retry_count += 1
                wait = 60 + retry_count * 30 + random.randint(0, 10)
                status_area.warning(f"⚠️ 触发限流 (429)，等待 {wait} 秒后重试... ({retry_count}/{max_retries})")
                time.sleep(wait)
                pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)

            except ResponseError as e:
                retry_count += 1
                wait = 30 + retry_count * 15
                status_area.warning(f"⚠️ 请求出错: {e}，等待 {wait} 秒后重试... ({retry_count}/{max_retries})")
                time.sleep(wait)

            except Exception as e:
                status_area.error(f"❌ 查询 '{kw}' 失败: {e}")
                failed_kw.append(kw)
                break

        else:
            failed_kw.append(kw)
            status_area.error(f"❌ '{kw}' 重试 {max_retries} 次后仍失败，跳过")

        if i < total_kw - 1:
            jitter = random.uniform(0, 2)
            time.sleep(effective_interval + jitter)

    progress_bar.progress(1.0, text="查询完成！")
    status_area.empty()

    # ── 第二阶段：趋势验证（判断是否近日突然飙升）──────────────
    spike_results = {}  # query -> {'pattern': '近日飙升'/'持续增长'/'稳定', 'trend': [...]}

    if all_rising and spike_check and spike_top_n > 0:
        temp_combined = pd.concat(all_rising, ignore_index=True)
        temp_combined['value_num'] = pd.to_numeric(temp_combined['value'], errors='coerce')
        # Breakout 排最前
        max_v = temp_combined['value_num'].max()
        temp_combined.loc[temp_combined['value_num'].isna(), 'value_num'] = (max_v * 2) if pd.notna(max_v) else 999999
        top_queries = temp_combined.nlargest(spike_top_n, 'value_num')['query'].unique().tolist()

        # 每 5 个一批查 interest_over_time（pytrends 限制最多 5 个关键词）
        batches = [top_queries[i:i+5] for i in range(0, len(top_queries), 5)]
        total_batches = len(batches)

        progress_bar.progress(0, text="正在验证趋势曲线...")

        for bi, batch in enumerate(batches):
            progress_bar.progress((bi) / total_batches, text=f"验证趋势: {', '.join(batch[:3])}... ({bi+1}/{total_batches})")

            retry_count = 0
            while retry_count < 3:
                try:
                    pytrend.build_payload(
                        kw_list=batch,
                        cat=category[1],
                        timeframe='now 7-d',
                        geo=geo[1],
                    )
                    iot = pytrend.interest_over_time()

                    if not iot.empty:
                        for q in batch:
                            if q in iot.columns:
                                series = iot[q].values.astype(float)
                                if len(series) < 4:
                                    continue
                                # 分成前半段和后 1/3 段比较
                                split = max(1, len(series) * 2 // 3)
                                early = series[:split]
                                late = series[split:]
                                early_avg = np.mean(early) if len(early) > 0 else 0
                                late_avg = np.mean(late) if len(late) > 0 else 0
                                peak_pos = np.argmax(series)
                                peak_in_late = peak_pos >= split

                                # 判断模式
                                is_new = early_avg < 1  # 前期几乎没搜索量 = 新词

                                if is_new:
                                    pattern = '新词飙升'
                                elif late_avg > early_avg * 2 and peak_in_late:
                                    pattern = '近日飙升'
                                elif late_avg > early_avg * 1.3:
                                    pattern = '持续上升'
                                else:
                                    pattern = '平稳'

                                spike_results[q] = {
                                    'pattern': pattern,
                                    'is_new': is_new,
                                    'trend': series.tolist(),
                                }
                    break

                except TooManyRequestsError:
                    retry_count += 1
                    wait = 60 + retry_count * 30
                    status_area.warning(f"⚠️ 趋势验证触发限流，等待 {wait} 秒...")
                    time.sleep(wait)
                    pytrend = TrendReq(hl='en-US', tz=360, timeout=(10, 30), retries=2, backoff_factor=1)
                except Exception:
                    break

            if bi < total_batches - 1:
                time.sleep(effective_interval + random.uniform(0, 2))

        progress_bar.progress(1.0, text="验证完成！")

    # ── 结果展示 ──────────────────────────────────────────────
    if all_rising:
            combined = pd.concat(all_rising, ignore_index=True)
            combined['value_num'] = pd.to_numeric(combined['value'], errors='coerce')
            has_numeric = combined['value_num'].notna()
            breakout_df = combined[~has_numeric].copy()

            total_rising = len(combined)
            total_sources = combined['keyword'].nunique()

            st.divider()
            st.subheader("🚀 相关爆增词汇总（近期增长最多）")
            st.markdown(f"**共找到 {total_rising} 个上升词，来自 {total_sources} 个词根**")

            if spike_results:
                combined['趋势'] = combined['query'].map(
                    lambda q: spike_results[q]['pattern'] if q in spike_results else ''
                )
                new_count = sum(1 for v in spike_results.values() if v['pattern'] == '新词飙升')
                spike_count = sum(1 for v in spike_results.values() if v['pattern'] == '近日飙升')
                parts = []
                if new_count > 0:
                    parts.append(f"**{new_count}** 个 **新词飙升** ✨")
                if spike_count > 0:
                    parts.append(f"**{spike_count}** 个 **近日飙升** 🔥")
                if parts:
                    st.markdown("其中 " + "，".join(parts))
            else:
                combined['趋势'] = ''

            # 发送飞书通知
            if send_feishu_notify(combined, spike_results):
                st.success("✅ 飞书通知已发送")
            else:
                st.warning("⚠️ 飞书通知发送失败，请检查 config.json 中的 webhook 配置")

            col_chart, col_table = st.columns([3, 2])

            with col_chart:
                chart_df = combined.copy()
                max_val = chart_df['value_num'].max()
                breakout_placeholder = max_val * 1.2 if pd.notna(max_val) and max_val > 0 else 100000
                chart_df.loc[chart_df['value_num'].isna(), 'value_num'] = breakout_placeholder

                top_n = chart_df.nlargest(20, 'value_num').copy()
                def make_label(row):
                    name = row['query'][:25] + '...' if len(row['query']) > 25 else row['query']
                    tag = row.get('趋势', '')
                    if tag == '新词飙升':
                        return name + ' ✨'
                    elif tag == '近日飙升':
                        return name + ' 🔥'
                    return name
                top_n['label'] = top_n.apply(make_label, axis=1)

                fig = px.bar(
                    top_n.iloc[::-1],
                    x='value_num',
                    y='label',
                    orientation='h',
                    labels={'value_num': '增长幅度 (%)', 'label': '', 'keyword': '词根'},
                    color='keyword',
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                )
                fig.update_layout(
                    height=max(450, len(top_n) * 32),
                    margin=dict(l=0, r=20, t=10, b=0),
                    yaxis=dict(tickfont=dict(size=12)),
                    legend=dict(title='词根', font=dict(size=11)),
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_table:
                display_df = combined[['query', 'value', 'keyword', '趋势']].copy()
                display_df['增长量'] = display_df['value'].apply(
                    lambda v: f'+{v}%' if str(v).isdigit() else '飙升'
                )
                display_df = display_df.rename(columns={'query': '爆词', 'keyword': '来源词根'})
                display_df = display_df.sort_values(
                    by='value',
                    key=lambda s: pd.to_numeric(s, errors='coerce').fillna(float('inf')),
                    ascending=False
                )
                display_df = display_df[['爆词', '增长量', '趋势', '来源词根']]
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=600)

                csv = display_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 下载 CSV",
                    data=csv,
                    file_name="trending_keywords.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            # 近日飙升词趋势曲线
            if spike_results:
                new_queries = [q for q, v in spike_results.items() if v['pattern'] == '新词飙升']
                spike_queries = [q for q, v in spike_results.items() if v['pattern'] == '近日飙升']
                hot_queries = new_queries + spike_queries

                if hot_queries:
                    st.divider()
                    st.subheader("🔥 近日突然飙升的词 — 趋势曲线")
                    st.caption("✨ 新词飙升 = 之前几乎无搜索量，近日突然出现的全新热词 &nbsp;&nbsp; 🔥 近日飙升 = 已有搜索量，近日突然大幅上升")

                    cols_per_row = 3
                    for row_start in range(0, len(hot_queries), cols_per_row):
                        cols = st.columns(cols_per_row)
                        for ci, col in enumerate(cols):
                            idx = row_start + ci
                            if idx >= len(hot_queries):
                                break
                            q = hot_queries[idx]
                            info = spike_results[q]
                            trend = info['trend']
                            tag = '✨ 新词' if info['pattern'] == '新词飙升' else '🔥 飙升'
                            line_color = '#f59e0b' if info['pattern'] == '新词飙升' else '#ff4b4b'
                            with col:
                                fig_mini = go.Figure()
                                fig_mini.add_trace(go.Scatter(
                                    y=trend, mode='lines+markers',
                                    line=dict(color=line_color, width=2),
                                    marker=dict(size=3),
                                    hoverinfo='y',
                                ))
                                fig_mini.update_layout(
                                    title=dict(text=f'{tag} {q[:25]}', font=dict(size=13)),
                                    height=180,
                                    margin=dict(l=10, r=10, t=35, b=10),
                                    xaxis=dict(showticklabels=False, showgrid=False),
                                    yaxis=dict(showticklabels=False, showgrid=True, gridcolor='#f0f0f0'),
                                    plot_bgcolor='white',
                                )
                                st.plotly_chart(fig_mini, use_container_width=True)

    else:
        st.warning("未查询到任何上升趋势词。可能是关键词太冷门，或遭遇限流。请调大请求间隔后重试。")

    if failed_kw:
        st.warning(f"以下 {len(failed_kw)} 个词查询失败: {', '.join(failed_kw)}")

elif start and total_kw == 0:
    st.error("请输入至少一个关键词")
