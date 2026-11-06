"""Export helpers: CSV, Excel, PNG chart and PDF report."""
import io
from datetime import datetime
from xml.sax.saxutils import escape

import pandas as pd
from openpyxl.utils import get_column_letter
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

import config


def _naive(df):
    """Excel cannot store timezone-aware datetimes."""
    out = df.copy()
    for col in out.columns:
        if isinstance(out[col].dtype, pd.DatetimeTZDtype):
            out[col] = out[col].dt.tz_localize(None)
    return out


def to_csv_bytes(df):
    return _naive(df).to_csv(index=False).encode("utf-8")


def _autofit(ws, df):
    for idx, col in enumerate(df.columns, start=1):
        widest = max([len(str(col))] + [len(str(v)) for v in df[col].head(500)])
        ws.column_dimensions[get_column_letter(idx)].width = min(widest + 2, 40)
    ws.freeze_panes = "A2"


def to_excel_bytes(raw_df, summary_df, agg_df):
    sheets = {
        "Raw Data": _naive(raw_df),
        "Summary": summary_df,
        "Spark Aggregates": _naive(agg_df),
    }
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)
            _autofit(writer.sheets[name], frame)
    return buf.getvalue()


def make_chart_png(df):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    metrics = sorted(df["metric_type"].unique())
    fig, axes = plt.subplots(
        len(metrics), 1, figsize=(10, 3.2 * len(metrics)), squeeze=False, sharex=True
    )
    for ax, metric in zip(axes[:, 0], metrics):
        for sensor, grp in df[df["metric_type"] == metric].groupby("sensor_id"):
            ax.plot(
                grp["timestamp"].dt.tz_localize(None), grp["value"],
                label=config.sensor_label(sensor), linewidth=1.6,
            )
        ax.set_title(config.metric_label(metric), fontsize=11, loc="left")
        ax.grid(alpha=0.3)
        ax.legend(loc="upper left", fontsize=8, ncol=4, frameon=False)
    axes[-1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%b %d %H:%M"))
    fig.autofmt_xdate()
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150)
    plt.close(fig)
    return buf.getvalue()


def _table(data, col_widths=None):
    table = Table(data, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f4e79")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f6fa")]),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ]
        )
    )
    return table


def to_pdf_bytes(df, summary_df, filters_text):
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm,
        topMargin=1.5 * cm, bottomMargin=1.5 * cm, title="Weather Data Report",
    )
    styles = getSampleStyleSheet()
    story = [
        Paragraph("Weather Streaming Pipeline - Data Report", styles["Title"]),
        Paragraph(
            f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
            f"Timezone: {escape(config.DISPLAY_TZ)}",
            styles["Normal"],
        ),
        Paragraph(escape(filters_text), styles["Normal"]),
        Paragraph(f"Total records: {len(df):,}", styles["Normal"]),
        Spacer(1, 0.5 * cm),
        Paragraph("Statistical Summary", styles["Heading2"]),
    ]

    rows = [["City", "Metric", "Count", "Mean", "Min", "Max", "Std"]]
    for _, r in summary_df.iterrows():
        rows.append([
            config.sensor_label(r["sensor_id"]), config.metric_label(r["metric_type"]),
            f"{int(r['count']):,}", f"{r['mean']:.2f}", f"{r['min']:.2f}",
            f"{r['max']:.2f}", "-" if pd.isna(r["std"]) else f"{r['std']:.2f}",
        ])
    story += [_table(rows), Spacer(1, 0.6 * cm), Paragraph("Trends", styles["Heading2"])]

    png = make_chart_png(df)
    iw, ih = ImageReader(io.BytesIO(png)).getSize()
    width = doc.width
    height = width * ih / iw
    if height > doc.height * 0.9:
        height = doc.height * 0.9
        width = height * iw / ih
    story += [Image(io.BytesIO(png), width=width, height=height), Spacer(1, 0.5 * cm)]

    sample = df.head(40)
    story.append(
        Paragraph(f"Data Sample (first {len(sample)} of {len(df):,} records)", styles["Heading2"])
    )
    data = [["Timestamp", "City", "Metric", "Value"]]
    for _, r in sample.iterrows():
        data.append([
            r["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
            config.sensor_label(r["sensor_id"]), config.metric_label(r["metric_type"]),
            f"{r['value']:.2f}",
        ])
    story.append(_table(data))

    doc.build(story)
    return buf.getvalue()
