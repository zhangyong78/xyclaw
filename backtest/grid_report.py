from __future__ import annotations

import html
from pathlib import Path

import pandas as pd


CARD_ORDER = ["1H", "2H", "3H", "4H", "6H", "8H", "12H", "16H", "24H"]

LABEL_SCORE = "\u7efc\u5408\u5f97\u5206"
LABEL_RANK = "\u6392\u540d"
LABEL_TRADE_COUNT = "\u4ea4\u6613\u6570"
LABEL_RETURN = "\u6536\u76ca\u7387"
LABEL_MAX_DRAWDOWN = "\u6700\u5927\u56de\u64a4"
LABEL_PROFIT_FACTOR = "\u76c8\u4e8f\u6bd4"
LABEL_REPORT = "\u56de\u6d4b\u62a5\u544a"
LABEL_OVERVIEW = "\u591a\u5468\u671f\u56de\u6d4b\u603b\u89c8"
LABEL_PERIOD_WINDOW = "\u65f6\u95f4\u7a97\u53e3\uff1a"
LABEL_TO = "\u5230"
LABEL_SCORING = "\u8bc4\u5206\u7ed3\u6784\uff1a30% \u6536\u76ca + 50% \u56de\u64a4 + 20% \u4ea4\u6613\u6837\u672c"
LABEL_FOOTER = "\u5361\u7247\u989c\u8272\u8d8a\u504f\u7eff\uff0c\u7efc\u5408\u5f97\u5206\u8d8a\u9ad8\u3002\u4f18\u5148\u5173\u6ce8\u4ea4\u6613\u6570\u8db3\u591f\u3001\u56de\u64a4\u8f83\u6d45\u3001\u6062\u590d\u8f83\u5feb\u7684\u5468\u671f\u3002"


def _card_color(score: float) -> str:
    clamped = max(0.0, min(100.0, score))
    green = int(110 + clamped * 1.2)
    red = int(230 - clamped * 1.1)
    blue = int(180 - clamped * 0.7)
    return f"rgb({red}, {green}, {blue})"


def write_grid_report(
    summary_df: pd.DataFrame,
    output_path: Path,
    symbol: str,
    start_ts,
    end_ts,
    matrix_section_html: str = "",
    chart_section_html: str = "",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = summary_df.set_index("period").reindex(CARD_ORDER).dropna().reset_index()

    cards: list[str] = []
    for _, row in ordered.iterrows():
        cards.append(
            f"""
            <section class="card" style="--accent:{_card_color(float(row["final_score"]))}">
              <div class="period">{html.escape(str(row["period"]))}</div>
              <div class="score">{LABEL_SCORE} {float(row["final_score"]):.1f}</div>
              <div class="rank">{LABEL_RANK} #{int(row["rank"])}</div>
              <dl>
                <div><dt>{LABEL_TRADE_COUNT}</dt><dd>{int(row["trade_count"])}</dd></div>
                <div><dt>{LABEL_RETURN}</dt><dd>{float(row["net_return_pct"]):.2f}%</dd></div>
                <div><dt>{LABEL_MAX_DRAWDOWN}</dt><dd>{float(row["max_drawdown_pct"]):.2f}%</dd></div>
                <div><dt>CDaR</dt><dd>{float(row["cdar_95_pct"]):.2f}%</dd></div>
                <div><dt>Ulcer</dt><dd>{float(row["ulcer_index"]):.2f}</dd></div>
                <div><dt>{LABEL_PROFIT_FACTOR}</dt><dd>{float(row["profit_factor"]):.2f}</dd></div>
              </dl>
            </section>
            """
        )

    title = html.escape(f"{symbol} {LABEL_REPORT}")
    hero_title = html.escape(f"{symbol} {LABEL_OVERVIEW}")
    period_text = html.escape(f"{LABEL_PERIOD_WINDOW}{start_ts} {LABEL_TO} {end_ts}")

    report_html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light;
      --panel: rgba(255, 255, 255, 0.78);
      --text: #162218;
      --muted: #586759;
      --border: rgba(22, 34, 24, 0.12);
      --shadow: 0 18px 40px rgba(36, 44, 25, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      font-family: "Segoe UI", "Microsoft YaHei UI", "PingFang SC", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(241, 199, 115, 0.6), transparent 28%),
        radial-gradient(circle at top right, rgba(84, 151, 111, 0.24), transparent 30%),
        linear-gradient(160deg, #f8f3ea 0%, #ebe5d7 100%);
      padding: 32px;
    }}
    .shell {{
      max-width: 1380px;
      margin: 0 auto;
    }}
    .hero {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 24px;
      padding: 28px 30px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: clamp(28px, 5vw, 46px);
      line-height: 1.05;
      letter-spacing: -0.03em;
    }}
    .sub {{
      margin: 0;
      color: var(--muted);
      font-size: 15px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
      margin-top: 24px;
    }}
    .card {{
      background: linear-gradient(180deg, color-mix(in srgb, var(--accent) 22%, white), rgba(255,255,255,0.92));
      border: 1px solid color-mix(in srgb, var(--accent) 40%, white);
      border-radius: 22px;
      padding: 20px;
      box-shadow: var(--shadow);
    }}
    .period {{
      font-size: 28px;
      font-weight: 700;
      letter-spacing: -0.04em;
    }}
    .score {{
      margin-top: 8px;
      font-size: 20px;
      font-weight: 600;
    }}
    .rank {{
      margin-top: 4px;
      color: var(--muted);
    }}
    dl {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 0;
    }}
    dl div {{
      background: rgba(255, 255, 255, 0.72);
      border-radius: 14px;
      padding: 10px 12px;
      border: 1px solid rgba(22, 34, 24, 0.08);
    }}
    dt {{
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
    }}
    dd {{
      margin: 6px 0 0;
      font-size: 18px;
      font-weight: 600;
    }}
    .charts {{
      display: grid;
      gap: 18px;
      margin-top: 24px;
    }}
    .sltp-section {{
      margin-top: 24px;
    }}
    .sltp-grid {{
      display: grid;
      gap: 10px;
      margin-top: 16px;
    }}
    .sltp-row {{
      display: grid;
      grid-template-columns: 120px repeat(3, minmax(0, 1fr));
      gap: 10px;
      align-items: stretch;
    }}
    .sltp-header {{
      align-items: end;
    }}
    .sltp-corner,
    .sltp-head,
    .sltp-side,
    .sltp-cell {{
      border: 1px solid rgba(22, 34, 24, 0.12);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.76);
      min-height: 54px;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 12px 14px;
      text-align: center;
    }}
    .sltp-corner,
    .sltp-head,
    .sltp-side {{
      font-weight: 700;
      color: #324236;
      background: rgba(248, 244, 237, 0.96);
    }}
    .sltp-cell {{
      font-family: "Consolas", "SFMono-Regular", monospace;
      color: #182018;
    }}
    .sltp-cell.is-best {{
      border-color: rgba(28, 138, 104, 0.35);
      background: rgba(224, 243, 235, 0.96);
    }}
    .chart-card {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 24px;
      padding: 22px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }}
    .chart-head h2 {{
      margin: 0;
      font-size: 22px;
    }}
    .chart-head p {{
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-top: 14px;
      color: var(--muted);
      font-size: 13px;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .legend-item i {{
      display: inline-block;
      width: 16px;
      height: 4px;
      border-radius: 999px;
      background: #999;
    }}
    .legend-buy {{ background: #1c8a68 !important; height: 10px !important; width: 10px !important; border-radius: 999px !important; }}
    .legend-sell {{ background: #bf5d35 !important; height: 10px !important; width: 10px !important; border-radius: 999px !important; }}
    .legend-fast {{ background: #1f6fb2 !important; }}
    .legend-slow {{ background: #d58a2f !important; }}
    .svg-shell {{
      overflow-x: auto;
      margin-top: 14px;
      padding-bottom: 6px;
    }}
    .svg-shell svg {{
      display: block;
      min-width: 100%;
      height: auto;
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 13px;
    }}
    @media (max-width: 900px) {{
      body {{ padding: 18px; }}
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <h1>{hero_title}</h1>
      <p class="sub">{period_text} | {LABEL_SCORING}</p>
    </section>
    <section class="grid">
      {''.join(cards)}
    </section>
    {matrix_section_html}
    {chart_section_html}
    <p class="footer">{LABEL_FOOTER}</p>
  </main>
</body>
</html>
"""
    output_path.write_text(report_html, encoding="utf-8")
